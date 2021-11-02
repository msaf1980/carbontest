package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"

	//"strconv"
	"strings"
	"time"

	"runtime/pprof"

	"github.com/msaf1980/cyclicbarrier"
	"go.uber.org/ratelimit"

	"carbontest/pkg/base"
	"carbontest/pkg/metricgen"
	"carbontest/pkg/metriclist"

	flag "github.com/spf13/pflag"
)

var cb *cyclicbarrier.CyclicBarrier

var totalStat = map[base.Proto]map[base.NetOper]map[base.NetErr]int64{}
var stat = map[base.Proto]map[base.NetOper]map[base.NetErr]int64{}
var startTimestamp int64
var endTimestamp int64

// var aggrTime int
// var aggrTimestamp int64

const aggrHeader = "time\tproto\tconn/s\tconn err/s\tsend/s\tsend err/s\tconn tout/s\tconn reset/s\tconn refused/s\tsend tout/s\tsend reset/s\tsend eof/s\tresolve err/s\tlimit err/s\tbytes/s\n"

//func pintStat(

var running = true

type config struct {
	Addr string
	//Connections int
	Workers   int // TCP Workers
	Duration  time.Duration
	Metrics   int
	BatchSend int

	MetricFile string
	Min        int32
	Max        int32
	Incr       int32

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

	AggrDuration   time.Duration
	AggrFile       string // write aggregated connections stat to file
	Graphite       string // address for graphite relay (for send aggregated connections stat)
	GraphitePrefix string // prefix for graphite metric

	StatFile   string // write connections stat to file
	DetailFile string // write sended metrics to file
	CPUProf    string // write cpu profile info to file
}

const header = "timestamp\tConId\tProto\tType\tStatus\tElapsed\tSize\n"

func printStat(start, end time.Time) {
	// Print stat
	var statVal []string
	duration := end.Sub(start).Seconds()

	for proto, opers := range totalStat {
		for oper, errors := range opers {
			for error, s := range errors {
				v := fmt.Sprintf("%s.%s.%s %d (%.4f/s)\n", proto.String(), oper.String(), error.String(),
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

func printAggrStat(w *bufio.Writer, start, end time.Time, tcpStat, udpStat Stat, workers, uworkers int) {
	timeStr := start.Format(time.RFC3339)
	duration := end.Sub(start).Seconds()
	if workers > 0 {
		fmt.Fprintf(w, "%s\t%s\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\n",
			timeStr, "TCP",
			float64(tcpStat.conns)/duration, float64(tcpStat.connsErr)/duration,
			float64(tcpStat.sends)/duration, float64(tcpStat.sendsErr)/duration,
			float64(tcpStat.connsTout)/duration, float64(tcpStat.connsReset)/duration, float64(tcpStat.connsRefused)/duration,
			float64(tcpStat.sendsTout)/duration, float64(tcpStat.sendsReset)/duration, float64(tcpStat.sendsEOF)/duration,
			float64(tcpStat.connsResolve)/duration, float64(tcpStat.connsFileLimit)/duration,
			float64(tcpStat.size)/duration,
		)
	}
	if uworkers > 0 {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%.4f\t%.4f\t%s\t%s\t%s\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\n",
			timeStr, "UDP",
			"-", "-",
			float64(udpStat.sends)/duration, float64(udpStat.sendsErr)/duration,
			"-", "-", "-",
			float64(udpStat.sendsTout)/duration, float64(udpStat.sendsReset)/duration, float64(udpStat.sendsEOF)/duration,
			float64(udpStat.sendsResolve)/duration, float64(udpStat.sendsFileLimit)/duration,
			float64(udpStat.size)/duration,
		)
	}
}

func sendAggrStat(graphite *GraphiteQueue, start, end time.Time, tcpStat, udpStat Stat, workers, uworkers int) {
	timeStamp := start.Unix()
	duration := end.Sub(start).Seconds()
	if workers > 0 {
		graphite.Put("tcp.size_ps", strconv.FormatFloat(float64(tcpStat.size)/duration, 'g', 6, 64), timeStamp)

		graphite.Put("tcp.conns_ps", strconv.FormatFloat(float64(tcpStat.conns)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.conns_err_ps", strconv.FormatFloat(float64(tcpStat.connsErr)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.conns_tout_ps", strconv.FormatFloat(float64(tcpStat.connsTout)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.conns_reset_ps", strconv.FormatFloat(float64(tcpStat.connsReset)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.conns_refused_ps", strconv.FormatFloat(float64(tcpStat.connsRefused)/duration, 'g', 6, 64), timeStamp)

		graphite.Put("tcp.sends_ps", strconv.FormatFloat(float64(tcpStat.sends)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.sends_err_ps", strconv.FormatFloat(float64(tcpStat.sendsErr)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.sends_tout_ps", strconv.FormatFloat(float64(tcpStat.sendsTout)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.sends_reset_ps", strconv.FormatFloat(float64(tcpStat.sendsReset)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.sends_eof_ps", strconv.FormatFloat(float64(tcpStat.sendsEOF)/duration, 'g', 6, 64), timeStamp)

		graphite.Put("tcp.resolve_err_ps", strconv.FormatFloat(float64(tcpStat.connsResolve)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.limits_err_ps", strconv.FormatFloat(float64(tcpStat.connsFileLimit)/duration, 'g', 6, 64), timeStamp)
	}
	if uworkers > 0 {
		graphite.Put("udp.size_ps", strconv.FormatFloat(float64(udpStat.size)/duration, 'g', 6, 64), timeStamp)

		graphite.Put("udp.sends_ps", strconv.FormatFloat(float64(udpStat.sends)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("udp.sends_err_ps", strconv.FormatFloat(float64(udpStat.sendsErr)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("udp.sends_tout_ps", strconv.FormatFloat(float64(udpStat.sendsTout)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("udp.sends_reset_ps", strconv.FormatFloat(float64(udpStat.sendsReset)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("udp.sends_eof_ps", strconv.FormatFloat(float64(udpStat.sendsEOF)/duration, 'g', 6, 64), timeStamp)

		graphite.Put("udp.resolve_err_ps", strconv.FormatFloat(float64(udpStat.connsResolve)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("udp.limits_err_ps", strconv.FormatFloat(float64(udpStat.connsFileLimit)/duration, 'g', 6, 64), timeStamp)
	}
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
		min          string
		max          string
		inc          string
	)

	flag.StringVarP(&host, "host", "r", "127.0.0.1", "hostname")
	flag.IntVarP(&port, "port", "p", 2003, "port")

	flag.IntVarP(&config.Workers, "workers", "w", 10, "TCP workers")
	flag.StringVarP(&duration, "duration", "d", "60s", "total test duration")
	flag.IntVarP(&config.Metrics, "metrics", "m", 1, "metrics sended in one TCP connection")
	flag.IntVarP(&config.BatchSend, "batch", "b", 1, "metrics count in one TCP send")

	flag.IntVarP(&config.UWorkers, "uworkers", "u", 0, "UDP workers (default 0)")
	flag.IntVarP(&config.UBatchSend, "ubatch", "B", 1, "metrics count in one UDP send")

	flag.StringVarP(&config.MetricPrefix, "prefix", "P", "test", "metric prefix")

	flag.StringVarP(&config.MetricFile, "file", "f", "", "metrics file (format: Name [min[:max[:increment]]")
	flag.StringVar(&min, "min", "0", "default min value for metrics file")
	flag.StringVar(&max, "max", "0", "default max value for metrics file")
	flag.StringVar(&inc, "incr", "0", "default incr value for metrics file (if 0 - value is random, also increase until max, than descrease to min)")

	flag.StringVarP(&conTimeout, "con_timeout", "c", "100ms", "TCP connect timeout (ms)")
	flag.StringVarP(&sendTimeout, "send_timeout", "s", "500ms", "TCP send timeout (ms)")

	flag.StringVarP(&sendDelay, "delay", "D", "0s", "send delay random range (min[:max])")
	flag.IntVar(&rateLimit, "rate", 0, "rate limit/s")

	flag.BoolVarP(&config.Verbose, "verbose", "v", false, "verbose")

	flag.DurationVarP(&config.AggrDuration, "aduration", "A", time.Minute, "aggregation duration")
	flag.StringVarP(&config.AggrFile, "aggr", "a", "", "sended metrics file (appended)")
	flag.StringVarP(&config.Graphite, "graphite", "g", "", "graphite relay address:port")
	flag.StringVarP(&config.GraphitePrefix, "gprefix", "G", "test.carbontest", "metric prefix for aggregated stat")

	flag.StringVar(&config.StatFile, "stat", "", "sended metrics stat file (appended)")
	flag.StringVar(&config.DetailFile, "detail", "", "sended metrics file (appended)")

	flag.StringVar(&config.CPUProf, "cpuprofile", "", "write cpu profile to file")

	flag.StringVar(&compressType, "compress", "", "compress [ gzip | lz4 ]")

	flag.Parse()

	config.Min, err = base.ParseInt32(min, 10)
	if err != nil {
		return config, err
	}
	config.Max, err = base.ParseInt32(max, 10)
	if err != nil {
		return config, err
	}
	if config.Max < config.Min {
		config.Min, config.Max = config.Max, config.Min
	}
	config.Incr, err = base.ParseInt32(inc, 10)
	if err != nil {
		return config, err
	}
	if config.Incr < 0 {
		config.Incr = -config.Incr
	}

	if host == "" {
		host = "127.0.0.1"
	}
	if port < 1 {
		return config, fmt.Errorf("Invalid port value: %d", port)
	}
	if config.Workers < 0 {
		return config, fmt.Errorf("Invalid TCP workers value: %d", config.Workers)
	}
	if config.Metrics < 1 {
		return config, fmt.Errorf("Invalid metrics value: %d", config.Metrics)
	}
	if config.BatchSend < 1 {
		return config, fmt.Errorf("Invalid TCP metric batchsend value: %d\n", config.BatchSend)
	}
	if config.UWorkers < 0 {
		return config, fmt.Errorf("Invalid UDP workers value: %d", config.Workers)
	}

	if config.Workers < 0 && config.UWorkers < 0 {
		return config, fmt.Errorf("Set TCP or UDP workers")
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
		config.RateLimiter = nil
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

	if config.AggrDuration < 10*time.Second {
		return config, fmt.Errorf("Invalid aggregation duration: %v", config.AggrDuration)
	}

	return config, nil
}

func main() {
	argsHead := "### " + strings.Join(os.Args, " ") + "\n"
	config, err := parseArgs()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	if len(config.GraphitePrefix) > 0 {
		hostname, err := os.Hostname()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			os.Exit(1)
		}
		config.GraphitePrefix = config.GraphitePrefix + ".carbontest." + strings.Split(hostname, ".")[0]
	}

	var graphite *GraphiteQueue
	if len(config.Graphite) > 0 {
		graphite, err = GraphiteInit(config.Graphite, config.GraphitePrefix, 100, 20)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			os.Exit(1)
		}
		graphite.Run()
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

	cb = cyclicbarrier.New(config.Workers + config.UWorkers + 1)

	result := make(chan ConStat, (config.Workers+config.UWorkers)*1000)
	mdetail := make(chan string, (config.Workers+config.UWorkers)*10000)
	workers := config.Workers
	uworkers := config.UWorkers
	var titer base.MetricIterator
	var uiter base.MetricIterator

	if len(config.MetricFile) > 0 {
		var metricPing string
		if len(config.GraphitePrefix) > 0 {
			metricPing = config.GraphitePrefix + ".ping"
		}
		metrics, err := metriclist.LoadMetricFile(config.MetricFile, config.Min, config.Max, config.Incr, metricPing)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			os.Exit(2)
		}
		iter, err := metriclist.New(metrics, config.Workers, config.BatchSend, config.Metrics,
			config.SendDelayMin.Nanoseconds(), config.SendDelayMax.Nanoseconds())
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			os.Exit(2)
		}
		if config.Workers > 0 {
			titer = iter
		}
		if config.UWorkers > 0 {
			uiter = iter
		}
	} else {
		if config.Workers > 0 {
			titer, err = metricgen.New(config.MetricPrefix+".tcp", config.Workers, config.BatchSend, config.Metrics,
				config.SendDelayMin.Nanoseconds(), config.SendDelayMax.Nanoseconds())
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(2)
			}
		}
		if config.UWorkers > 0 {
			uiter, err = metricgen.New(config.MetricPrefix+".udp", config.UWorkers, config.BatchSend, config.Metrics,
				config.SendDelayMin.Nanoseconds(), config.SendDelayMax.Nanoseconds())
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
				os.Exit(2)
			}
		}
	}

	for i := 0; i < config.Workers; i++ {
		go TcpWorker(i, config, result, mdetail, titer)
	}
	for i := 0; i < config.UWorkers; i++ {
		go UDPWorker(i, config, result, mdetail, uiter)
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

	begin := time.Now()
	var end time.Time

	aggrTicker := time.Tick(config.AggrDuration)

	var udpStat Stat
	var tcpStat Stat
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
				printAggrStat(aw, begin, end, tcpStat, udpStat, config.Workers, config.UWorkers)
				aw.Flush()
			}
			if graphite != nil {
				sendAggrStat(graphite, begin, end, tcpStat, udpStat, config.Workers, config.UWorkers)
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
	if config.AggrFile != "" {
		fmt.Printf("aggregated results writed to %s\n", config.AggrFile)
	}
	if config.StatFile != "" {
		fmt.Printf("results writed to %s\n", config.StatFile)
	}
	if config.DetailFile != "" {
		fmt.Printf("sended metrics writed to %s\n", config.DetailFile)
	}
	if aw != nil {
		printAggrStat(aw, begin, end, tcpStat, udpStat, config.Workers, config.UWorkers)
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
	graphite.Stop()
}
