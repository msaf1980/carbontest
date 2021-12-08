package main

import (
	"bufio"
	"carbontest/pkg/base"
	"carbontest/pkg/metricgen"
	"carbontest/pkg/metriclist"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"time"

	"github.com/msaf1980/cyclicbarrier"
)

const aggrHeader = "time\tproto\tconn/s\tconn err/s\tsend/s\tsend err/s\tconn tout/s\tconn reset/s\tconn refused/s\tsend tout/s\tsend reset/s\tsend eof/s\tresolve err/s\tlimit err/s\tbytes/s\n"

const header = "timestamp\tConId\tProto\tType\tStatus\tElapsed\tSize\n"

func worker(name string, localConfig *LocalConfig, sharedConfig *SharedConfig, workerConfig *WorkerConfig) error {
	var (
		err      error
		graphite *GraphiteQueue
	)

	mutex.Lock()
	defer mutex.Unlock()

	if sharedConfig.Max < sharedConfig.Min {
		sharedConfig.Min, sharedConfig.Max = sharedConfig.Max, sharedConfig.Min
	}
	if sharedConfig.Incr < 0 {
		sharedConfig.Incr = -sharedConfig.Incr
	}

	mergeConfig(sharedConfig, workerConfig)

	if err = validateWorkerConfig(name, workerConfig); err != nil {
		return err
	}
	if err = validateSharedConfig(sharedConfig); err != nil {
		return err
	}
	if err = validateLocalConfig(localConfig); err != nil {
		return err
	}

	if len(localConfig.Graphite) > 0 {
		graphite, err = GraphiteInit(localConfig.Graphite, localConfig.GraphitePrefix, 100, 20)
		if err != nil {
			return err
		}
		graphite.Run()
	}

	var file *os.File
	var w *bufio.Writer

	argsHead := "### " + strings.Join(os.Args, " ") + "\n"

	if localConfig.StatFile != "" {
		file, err = os.OpenFile(localConfig.StatFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer file.Close()

		w = bufio.NewWriter(file)
		_, err = w.WriteString(argsHead)
		if err != nil {
			return err
		}
		_, err = w.WriteString(header)
		if err != nil {
			return err
		}
	}

	var dFile *os.File
	var dw *bufio.Writer

	if localConfig.DetailFile != "" {
		dFile, err = os.OpenFile(localConfig.DetailFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer dFile.Close()
		dw = bufio.NewWriter(dFile)
		_, err = dw.WriteString(argsHead)
		if err != nil {
			return err
		}
	}

	var aFile *os.File
	var aw *bufio.Writer

	if localConfig.AggrFile != "" {
		aFile, err = os.OpenFile(localConfig.AggrFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer aFile.Close()

		aw = bufio.NewWriter(aFile)
		_, err = aw.WriteString(argsHead)
		if err != nil {
			return err
		}
		if _, err = aw.WriteString(aggrHeader); err != nil {
			return err
		}
	}

	if localConfig.CPUProf != "" {
		f, err := os.Create(localConfig.CPUProf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		}
		_ = pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	cb = cyclicbarrier.New(workerConfig.T.TCP.Workers + workerConfig.T.UDP.Workers + 1)

	result := make(chan ConStat, (workerConfig.T.TCP.Workers+workerConfig.T.UDP.Workers)*1000)
	mdetail := make(chan string, (workerConfig.T.TCP.Workers+workerConfig.T.UDP.Workers)*10000)

	var titer base.MetricIterator
	var uiter base.MetricIterator

	var udpOffset int

	if len(mainConfig.Local.MetricsList) > 0 {
		if len(localConfig.GraphitePrefix) > 0 {
			metricPing := localConfig.GraphitePrefix + ".ping"
			mainConfig.Local.MetricsList = append(mainConfig.Local.MetricsList, metriclist.Metric{Name: metricPing, Min: 1, Max: 1})
		}
		iter, err := metriclist.New(mainConfig.Local.MetricsList,
			workerConfig.T.TCP.Workers, workerConfig.T.TCP.BatchSend, workerConfig.T.TCP.Metrics,
			workerConfig.T.UDP.Workers, workerConfig.T.UDP.BatchSend, workerConfig.T.UDP.Metrics,
			workerConfig.T.SendDelay)
		if err != nil {
			return err
		}

		if workerConfig.T.TCP.Workers > 0 {
			udpOffset = workerConfig.T.TCP.Workers
			titer = iter
		}
		if workerConfig.T.UDP.Workers > 0 {
			uiter = iter
		}
	} else {
		if workerConfig.T.TCP.Workers > 0 {
			titer, err = metricgen.New(sharedConfig.MetricPrefix+"."+localConfig.Hostname+".tcp", workerConfig.T.TCP.Workers,
				workerConfig.T.TCP.BatchSend, workerConfig.T.TCP.Metrics, workerConfig.T.SendDelay)
			if err != nil {
				return err
			}
		}
		if workerConfig.T.UDP.Workers > 0 {
			uiter, err = metricgen.New(sharedConfig.MetricPrefix+"."+localConfig.Hostname+".udp", workerConfig.T.UDP.Workers,
				workerConfig.T.UDP.BatchSend, workerConfig.T.UDP.Metrics, workerConfig.T.SendDelay)
			if err != nil {
				return err
			}
		}
	}

	for i := 0; i < workerConfig.T.TCP.Workers; i++ {
		go TcpWorker(i, localConfig, sharedConfig, workerConfig, result, mdetail, titer)
	}
	for i := 0; i < workerConfig.T.UDP.Workers; i++ {
		go UDPWorker(udpOffset+i, localConfig, sharedConfig, workerConfig, result, mdetail, uiter)
	}

	// Test duration
	go func() {
		time.Sleep(sharedConfig.Duration)
		log.Printf("Shutting down")
		atomic.StoreInt32(&running, 0)
	}()

	start := time.Now()

	log.Printf("Starting TCP workers: %d, UDP %d\n", workerConfig.T.TCP.Workers, workerConfig.T.UDP.Workers)

	cb.Await()

	begin := time.Now()
	var end time.Time

	aggrTicker := time.Tick(localConfig.AggrDuration)

	var udpStat Stat
	var tcpStat Stat
	workers := workerConfig.T.TCP.Workers
	uworkers := workerConfig.T.UDP.Workers
LOOP:
	for {
		select {
		case r := <-mdetail:
			_, err = dw.WriteString(r)
			if err != nil {
				panic(err)
			}
		case <-aggrTicker:
			end = time.Now()
			if aw != nil {
				printAggrStat(aw, begin, end, tcpStat, udpStat, workerConfig.T.TCP.Workers, workerConfig.T.UDP.Workers)
				aw.Flush()
			}
			if graphite != nil {
				sendAggrStat(graphite, begin, end, tcpStat, udpStat, workerConfig.T.TCP.Workers, workerConfig.T.UDP.Workers)
			}
			tcpStat.Clear()
			udpStat.Clear()
			begin = end
		case r := <-result:
			if r.TimeStamp == 0 {
				if r.Proto == base.TCP {
					workers--
				} else {
					uworkers--
				}
				if workers <= 0 && uworkers <= 0 {
					break LOOP
				}
			} else {
				if graphite != nil || aw != nil {
					if r.Proto == base.TCP {
						tcpStat.Add(&r)
					} else {
						udpStat.Add(&r)
					}
				}

				sProto, ok := totalStat[r.Proto]
				if !ok {
					sProto = map[base.NetOper]map[base.NetErr]int64{}
					totalStat[r.Proto] = sProto
				}
				sOper, ok := sProto[r.Type]
				if !ok {
					sOper = map[base.NetErr]int64{}
					sProto[r.Type] = sOper
				}
				sOper[r.Error]++

				// write to stat file
				if w != nil {
					timeStr := time.Unix(r.TimeStamp/1000000000, r.TimeStamp%1000000000).Format(time.RFC3339Nano)
					fmt.Fprintf(w, "%s\t%d\t%s\t%s\t%s\t%d\t%d\n", timeStr, r.Id,
						r.Proto.String(), r.Type.String(),
						r.Error.String(), r.Elapsed/1000, r.Size)
				}

				// if r.TimeStamp+r.Elapsed > endTimestamp {
				// 	endTimestamp = r.TimeStamp + r.Elapsed
				// }
				// if r.TimeStamp < startTimestamp || startTimestamp == 0 {
				// 	startTimestamp = r.TimeStamp
				// }

				// if aw != nil {
				// 	// round to minute
				// 	endTime := int(endTimestamp / (1000 * 1000 * 1000 * aggrDuration))
				// 	if endTime > aggrTime {
				// 		// flush aggregated stat
				// 		if aggrTime > 0 {
				// 			if aw != nil {
				// 				printAggrStat(aw, aggrTimestamp, float64(endTimestamp-aggrTimestamp)/1000000000.0, config.Workers, config.UWorkers)
				// 			}
				// 		}
				// 		aggrTimestamp = endTimestamp
				// 		aggrTime = endTime

				// 		// merge stat
				// 		mergeStat(totalStat, stat)

				// 		stat = make(map[base.Proto]map[base.NetOper]map[base.NetErr]int64)
				// 	}
				// }
				// // write to stat file
				// if w != nil {
				// 	timeStr := time.Unix(r.TimeStamp/1000000000, r.TimeStamp%1000000000).Format(time.RFC3339Nano)
				// 	fmt.Fprintf(w, "%s\t%d\t%s\t%s\t%s\t%d\t%d\n", timeStr, r.Id,
				// 		r.Proto.String(), r.Type.String(),
				// 		r.Error.String(), r.Elapsed/1000, r.Size)
				// }

				// sProto, ok := stat[r.Proto]
				// if !ok {
				// 	sProto = map[base.NetOper]map[base.NetErr]int64{}
				// 	stat[r.Proto] = sProto
				// }
				// sOper, ok := sProto[r.Type]
				// if !ok {
				// 	sOper = map[base.NetErr]int64{}
				// 	sProto[r.Type] = sOper
				// }
				// _, ok = sOper[r.Error]
				// if !ok {
				// 	sOper[r.Error] = 1
				// } else {
				// 	sOper[r.Error]++
				// }
			}
		}
	}
	end = time.Now()
	duration := end.Sub(start)
	log.Printf("Shutdown. Test Duration %s", duration)
	if localConfig.AggrFile != "" {
		fmt.Printf("aggregated results writed to %s\n", localConfig.AggrFile)
	}
	if localConfig.StatFile != "" {
		fmt.Printf("results writed to %s\n", localConfig.StatFile)
	}
	if localConfig.DetailFile != "" {
		fmt.Printf("sended metrics writed to %s\n", localConfig.DetailFile)
	}
	if aw != nil {
		printAggrStat(aw, begin, end, tcpStat, udpStat, workerConfig.T.TCP.Workers, workerConfig.T.UDP.Workers)
	}

	printStat(start, end)

	if w != nil {
		err = w.Flush()
		if err != nil {
			panic(err)
		}
	}
	if dw != nil {
		for len(mdetail) > 0 {
			r := <-mdetail
			_, err = dw.WriteString(r)
			if err != nil {
				panic(err)
			}
		}
		err = dw.Flush()
		if err != nil {
			panic(err)
		}
	}
	if aw != nil {
		err = aw.Flush()
		if err != nil {
			panic(err)
		}
	}

	time.Sleep(2 * time.Second)
	if graphite != nil {
		graphite.Stop()
	}

	return nil
}
