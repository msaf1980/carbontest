package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"

	//"strconv"
	"strings"
	"time"

	"runtime/pprof"

	"github.com/msaf1980/cyclicbarrier"
	"go.uber.org/ratelimit"

	"carbontest/pkg/base"
)

var cb *cyclicbarrier.CyclicBarrier

var totalStat = map[base.Proto]map[base.NetOper]map[base.NetErr]int64{}
var startTimestamp int64
var stat = map[base.Proto]map[base.NetOper]map[base.NetErr]int64{}
var aggrTime int
var aggrTimestamp int64
var endTimestamp int64

const aggrHeader = "time\tproto\tconn/s\tconn err/s\tsend/s\tsend err/s\tconn tout/s\tconn reset/s\tconn refused/s\tsend tout/s\tsend reset/s\n"

//func pintStat(

var running = true

type config struct {
	Addr string
	//Connections int
	Workers      int // TCP Workers
	Duration     time.Duration
	MetricPerCon int
	BatchSend    int

	Compress base.CompressType

	SendDelayMin time.Duration
	SendDelayMax time.Duration
	RateLimiter  ratelimit.Limiter

	ConTimeout  time.Duration
	SendTimeout time.Duration

	UWorkers   int // UDP Workers
	UBatchSend int

	MetricPrefix string // Prefix for generated metric name
	Verbose      bool

	StatFile   string // write connections stat to file
	DetailFile string // write sended metrics to file
	AggrFile   string // write aggregated connections stat to file
	CPUProf    string // write cpu profile info to file
}

const header = "timestamp\tConId\tProto\tType\tStatus\tElapsed\tSize\n"

func mergeStat(dest map[base.Proto]map[base.NetOper]map[base.NetErr]int64, source map[base.Proto]map[base.NetOper]map[base.NetErr]int64) {
	for k1, v1 := range source {
		_, ok := dest[k1]
		if !ok {
			dest[k1] = map[base.NetOper]map[base.NetErr]int64{}
		}
		for k2, v2 := range v1 {
			_, ok := dest[k1][k2]
			if !ok {
				dest[k1][k2] = map[base.NetErr]int64{}
			}
			for k3, v3 := range v2 {
				_, ok := dest[k1][k2]
				if ok {
					dest[k1][k2][k3] += v3
				} else {
					dest[k1][k2][k3] = v3
				}
			}
		}
	}
}

func printStat() {

	// Print stat
	var statVal []string
	duration := float64(endTimestamp-startTimestamp) / 1000000000.0

	for proto, opers := range totalStat {
		for oper, errors := range opers {
			for error, s := range errors {
				v := fmt.Sprintf("%s.%s.%s %d (%.2f/s)\n", proto.String, oper.String(), error.String(),
					s, float64(s)/duration)
				statVal = append(statVal, v)
			}
		}
	}

	sort.Strings(statVal)
	for _, s := range statVal {
		fmt.Print(s)
	}
}

func printAggrStat(w *bufio.Writer, timeStamp int64, duration float64, workers int, uworkers int) {
	//timeStr := time.Unix(timeStamp/1000000000, 0).Format("2006-01-02T15:04:05-0700")
	timeStr := time.Unix(timeStamp/1000000000, 0).Format(time.RFC3339)
	if workers > 0 {
		sProto, ok := stat[base.TCP]
		if !ok {
			fmt.Fprintf(w, "%s\t%s\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t\n",
				timeStr, "TCP", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

		} else {
			var sConnOk int64
			var sConnErr int64
			var sConnTOut int64
			var sConnReset int64
			var sConnRefused int64
			for k, v := range sProto[base.CONNECT] {
				if k == base.OK {
					sConnOk = v
				} else {
					sConnErr += v
					switch k {
					case base.TIMEOUT:
						sConnTOut = v
					case base.RESET:
						sConnReset = v
					case base.REFUSED:
						sConnRefused = v
					}
				}
			}

			var sSendOk int64
			var sSendErr int64
			var sSendTOut int64
			var sSendReset int64
			for k, v := range sProto[base.SEND] {
				if k == base.OK {
					sSendOk = v
				} else {
					sSendErr += v
					switch k {
					case base.TIMEOUT:
						sSendTOut = v
					case base.RESET:
						sSendReset = v
					}
				}
			}
			fmt.Fprintf(w, "%s\t%s\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n",
				timeStr, "TCP",
				float64(sConnOk)/duration, float64(sConnErr)/duration,
				float64(sSendOk)/duration, float64(sSendErr)/duration,
				float64(sConnTOut)/duration, float64(sConnReset)/duration, float64(sConnRefused)/duration,
				float64(sSendTOut)/duration, float64(sSendReset)/duration)
		}
	}
	if uworkers > 0 {
		sProto, ok := stat[base.UDP]
		if !ok {
			fmt.Fprintf(w, "%s\t%s\t-\t-\t%.2f\t%.2f\t-\t-\t-\t%.2f\t%.2f\n",
				timeStr, "UDP", 0.0, 0.0, 0.0, 0.0)

		} else {
			var sSendOk int64
			var sSendErr int64
			var sSendTOut int64
			var sSendReset int64
			for k, v := range sProto[base.SEND] {
				if k == base.OK {
					sSendOk = v
				} else {
					sSendErr += v
					switch k {
					case base.TIMEOUT:
						sSendTOut = v
					case base.RESET:
						sSendReset = v
					}
				}
			}
			fmt.Fprintf(w, "%s\t%s\t-\t-\t%.2f\t%.2f\t-\t-\t-\t%.2f\t%.2f\n",
				timeStr, "UDP",
				float64(sSendOk)/duration, float64(sSendErr)/duration,
				float64(sSendTOut)/duration, float64(sSendReset)/duration)
		}
	}
	w.Flush()
}

func parseArgs() (config, error) {
	var (
		config       config
		conTimeout   string
		sendTimeout  string
		sendDelay    string
		host         string
		port         int
		duration     string
		err          error
		compressType string
		rateLimit    int
	)

	flag.StringVar(&host, "host", "127.0.0.1", "hostname")
	flag.IntVar(&port, "port", 2003, "port")
	flag.IntVar(&config.Workers, "workers", 10, "TCP workers")
	flag.StringVar(&duration, "duration", "60s", "total test duration")
	flag.IntVar(&config.MetricPerCon, "metric", 1, "send metric count in one TCP connection")
	//flag.IntVar(&config.BatchSend, "batch", 1, "send metric count in one TCP send")
	flag.IntVar(&config.UWorkers, "uworkers", 0, "UDP workers (default 0)")
	//flag.IntVar(&config.UBatchSend, "ubatch", 1, "send metric count in one UDP send")
	flag.StringVar(&config.MetricPrefix, "prefix", "test", "metric prefix")

	flag.StringVar(&conTimeout, "c", "100ms", "TCP connect timeout (ms)")
	flag.StringVar(&sendTimeout, "s", "500ms", "TCP send timeout (ms)")

	flag.StringVar(&sendDelay, "delay", "0s", "send delay random range (min[:max])")
	flag.IntVar(&rateLimit, "rate", 0, "rate limit/s")

	flag.BoolVar(&config.Verbose, "verbose", false, "verbose")

	flag.StringVar(&config.StatFile, "stat", "", "stat file")
	flag.StringVar(&config.AggrFile, "aggr", "", "sended metrics file (appended)")
	flag.StringVar(&config.DetailFile, "detail", "", "sended metrics file (appended)")

	flag.StringVar(&config.CPUProf, "cpuprofile", "", "write cpu profile to file")

	flag.StringVar(&compressType, "compress", "", "compress [ gzip | lz4 ]")

	flag.Parse()
	if host == "" {
		host = "127.0.0.1"
	}
	if port < 1 {
		return config, fmt.Errorf("Invalid port value: %d", port)
	}
	if config.Workers < 1 {
		return config, fmt.Errorf("Invalid TCP workers value: %d", config.Workers)
	}
	if config.MetricPerCon < 1 {
		return config, fmt.Errorf("Invalid TCP metric value: %d", config.MetricPerCon)
	}
	/*
		if config.BatchSend < 1 {
			return config, errors.New(fmt.Sprintf("Invalid TCP metric batchsend value: %d\n", config.BatchSend))
		}
	*/
	if config.UWorkers < 0 {
		return config, fmt.Errorf("Invalid UDP workers value: %d", config.Workers)
	}

	splitSendDelay := strings.Split(sendDelay, ":")
	if len(splitSendDelay) >= 1 {
		config.SendDelayMin, err = time.ParseDuration(splitSendDelay[0])
		if err != nil || config.SendDelayMin < 0 {
			return config, fmt.Errorf("Invalid min delay value: %s", splitSendDelay[0])
		}
	}
	if len(splitSendDelay) == 1 {
		config.SendDelayMax = config.SendDelayMin
	} else if len(splitSendDelay) == 2 {
		config.SendDelayMax, err = time.ParseDuration(splitSendDelay[1])
		if err != nil || config.SendDelayMax < 0 {
			return config, fmt.Errorf("Invalid max delay value: %s", splitSendDelay[1])
		} else if config.SendDelayMin > config.SendDelayMax {
			return config, fmt.Errorf("Invalid max delay value less than minimal: %s", sendDelay)
		}
	} else {
		return config, fmt.Errorf("Invalid delay value: %s", sendDelay)
	}

	if rateLimit < 0 {
		return config, fmt.Errorf("Invalid rate limit value: %d", rateLimit)
	} else if rateLimit > 0 {
		if config.SendDelayMax > 0 {
			return config, fmt.Errorf("delay and rate limit can't be used together")
		}
		config.RateLimiter = ratelimit.New(rateLimit)
	} else {
		config.RateLimiter = ratelimit.NewUnlimited()
	}

	config.SendTimeout, err = time.ParseDuration(sendTimeout)
	if err != nil || config.SendTimeout < 0 {
		if config.SendTimeout < 1 {
			return config, fmt.Errorf("Invalid TCP send timeout value: %s", sendTimeout)
		}
	}
	config.ConTimeout, err = time.ParseDuration(conTimeout)
	if err != nil || config.ConTimeout < 1*time.Microsecond {
		return config, fmt.Errorf("Invalid TCP connection timeout value: %s", conTimeout)
	}
	config.Duration, err = time.ParseDuration(duration)
	if err != nil || config.Duration < time.Second {
		return config, fmt.Errorf("Invalid test duration: %s", duration)
	}

	compressType = strings.ToLower(compressType)
	if compressType == "" {
		config.Compress = base.NONE
	} else if compressType == "gzip" {
		config.Compress = base.GZIP
	} else {
		return config, fmt.Errorf("Invalid compress type: %s", compressType)
	}

	config.Addr = fmt.Sprintf("%s:%d", host, port)

	return config, nil
}

func main() {
	argsHead := "### " + strings.Join(os.Args, " ") + "\n"
	config, err := parseArgs()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	var file *os.File
	var w *bufio.Writer

	if config.StatFile != "" {
		file, err = os.OpenFile(config.StatFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			os.Exit(2)
		}
		defer file.Close()
		w = bufio.NewWriter(file)
		_, err = w.WriteString(argsHead)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			os.Exit(2)
		}
		_, err = w.WriteString(header)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			os.Exit(2)
		}
	}

	var dFile *os.File
	var dw *bufio.Writer

	if config.DetailFile != "" {
		dFile, err = os.OpenFile(config.DetailFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			os.Exit(2)
		}
		defer dFile.Close()
		dw = bufio.NewWriter(dFile)
		_, err = dw.WriteString(argsHead)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			os.Exit(2)
		}
	}

	var aFile *os.File
	var aw *bufio.Writer

	if config.AggrFile != "" {
		aFile, err = os.OpenFile(config.AggrFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			os.Exit(2)
		}
		defer aFile.Close()
		aw = bufio.NewWriter(aFile)
		_, err = aw.WriteString(argsHead)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			os.Exit(2)
		}
		_, err = aw.WriteString(aggrHeader)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			os.Exit(2)
		}
	}

	if config.CPUProf != "" {
		f, err := os.Create(config.CPUProf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		}
		_ = pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	result := make(chan ConStat, (config.Workers+config.UWorkers)*1000)
	mdetail := make(chan string, (config.Workers+config.UWorkers)*10000)
	workers := config.Workers
	uworkers := config.UWorkers

	cb = cyclicbarrier.New(config.Workers + config.UWorkers + 1)

	for i := 0; i < config.Workers; i++ {
		go TcpWorker(i, config, result, mdetail)
	}
	for i := 0; i < config.UWorkers; i++ {
		go UDPWorker(i, config, result, mdetail)
	}

	// Test duration
	go func() {
		time.Sleep(config.Duration)
		log.Printf("Shutting down")
		running = false
	}()

	start := time.Now()

	log.Printf("Starting TCP workers: %d, UDP %d\n", config.Workers, config.UWorkers)
	cb.Await()

	const aggrDuration = 60

LOOP:
	for {
		select {
		case r := <-mdetail:
			_, err = dw.WriteString(r)
			if err != nil {
				panic(err)
			}
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
				if r.TimeStamp+r.Elapsed > endTimestamp {
					endTimestamp = r.TimeStamp + r.Elapsed
				}
				if r.TimeStamp < startTimestamp || startTimestamp == 0 {
					startTimestamp = r.TimeStamp
				}

				if aw != nil {
					// round to minute
					endTime := int(endTimestamp / (1000 * 1000 * 1000 * aggrDuration))
					if endTime > aggrTime {
						// flush aggregated stat
						if aggrTime > 0 {
							if aw != nil {
								printAggrStat(aw, aggrTimestamp, float64(endTimestamp-aggrTimestamp)/1000000000.0, config.Workers, config.UWorkers)
							}
						}
						aggrTimestamp = endTimestamp
						aggrTime = endTime

						// merge stat
						mergeStat(totalStat, stat)

						stat = make(map[base.Proto]map[base.NetOper]map[base.NetErr]int64)
					}
				}
				// write to stat file
				if w != nil {
					timeStr := time.Unix(r.TimeStamp/1000000000, r.TimeStamp%1000000000).Format(time.RFC3339Nano)
					fmt.Fprintf(w, "%s\t%d\t%s\t%s\t%s\t%d\t%d\n", timeStr, r.Id,
						r.Proto.String(), r.Type.String(),
						r.Error.String(), r.Elapsed/1000, r.Size)
				}

				sProto, ok := stat[r.Proto]
				if !ok {
					sProto = map[base.NetOper]map[base.NetErr]int64{}
					stat[r.Proto] = sProto
				}
				sOper, ok := sProto[r.Type]
				if !ok {
					sOper = map[base.NetErr]int64{}
					sProto[r.Type] = sOper
				}
				_, ok = sOper[r.Error]
				if !ok {
					sOper[r.Error] = 1
				} else {
					sOper[r.Error]++
				}
			}
		}
	}
	end := time.Now()
	duration := end.Sub(start)
	log.Printf("Shutdown. Test Duration %s", duration)
	if config.AggrFile != "" {
		fmt.Printf("aggregated results writed to %s\n", config.AggrFile)
	}
	if config.StatFile != "" {
		fmt.Printf("results writed to %s\n", config.StatFile)
	}
	if config.DetailFile != "" {
		fmt.Printf("sended metrics writed to %s\n", config.DetailFile)
	}
	if len(stat) > 0 {
		mergeStat(totalStat, stat)
		if aw != nil {
			printAggrStat(aw, aggrTimestamp, float64(endTimestamp-aggrTimestamp)/1000000000.0, config.Workers, config.UWorkers)
		}
	}

	printStat()

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
}
