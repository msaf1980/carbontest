package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"

	//"strconv"
	"strings"
	"time"

	"runtime/pprof"

	"github.com/msaf1980/cyclicbarrier"
	//"github.com/msaf1980/ratelimit"
)

const (
	maxBuf = 1000
)

var cb *cyclicbarrier.CyclicBarrier

var stat = map[Proto]map[NetOper]map[NetErr]int64{}

var running = true

type config struct {
	Addr string
	//Connections int
	Workers      int // TCP Workers
	Duration     time.Duration
	MetricPerCon int
	BatchSend    int
	//RateLimit    []int32

	SendDelayMin time.Duration
	SendDelayMax time.Duration

	ConTimeout  time.Duration
	SendTimeout time.Duration

	MetricPrefix string // Prefix for generated metric name
	Verbose      bool

	InFile string

	StatFile   string // write connections stat to file
	DetailFile string // write sended metrics to file
	CPUProf    string // write cpu profile info to file
}

const header = "timestamp\tConId\tProto\tType\tStatus\tElapsed\tSize\n"

func printStat(stat map[Proto]map[NetOper]map[NetErr]int64, duration time.Duration) {
	// Print stat
	var statVal []string

	for proto, opers := range stat {
		for oper, errors := range opers {
			for error, s := range errors {
				v := fmt.Sprintf("%s.%s.%s %d (%d/s)\n", ProtoToString(proto), NetOperToString(oper), NetErrToString(error),
					s, s/(duration.Nanoseconds()/1000000000))
				statVal = append(statVal, v)
			}
		}
	}

	sort.Strings(statVal)
	for _, s := range statVal {
		fmt.Print(s)
	}
}

func parseArgs() (config, error) {
	var (
		config      config
		conTimeout  string
		sendTimeout string
		sendDelay   string
		host        string
		port        int
		duration    string
		err         error
		//rateLimit   string
	)

	flag.StringVar(&host, "host", "127.0.0.1", "hostname")
	flag.IntVar(&port, "port", 2003, "port")
	flag.IntVar(&config.Workers, "workers", 10, "TCP workers")
	flag.StringVar(&duration, "duration", "60s", "total test duration")
	flag.IntVar(&config.MetricPerCon, "metric", 1, "send metric count in one TCP connection")
	//flag.IntVar(&config.BatchSend, "batch", 1, "send metric count in one TCP send")
	//flag.IntVar(&config.UBatchSend, "ubatch", 1, "send metric count in one UDP send")
	flag.StringVar(&config.MetricPrefix, "prefix", "test", "metric prefix")

	//flag.StringVar(&rateLimit, "rate", "", "rate limit, format: rate or minRate:maxRate:increment ")
	flag.StringVar(&conTimeout, "c", "100ms", "TCP connect timeout (ms)")
	flag.StringVar(&sendTimeout, "s", "500ms", "TCP send timeout (ms)")
	flag.StringVar(&sendDelay, "delay", "0s", "send delay random range (min[:max])")

	flag.StringVar(&config.InFile, "file", "", "input file")

	flag.BoolVar(&config.Verbose, "verbose", false, "verbose")

	flag.StringVar(&config.StatFile, "stat", "test.csv", "stat file (appended)")

	flag.StringVar(&config.CPUProf, "cpuprofile", "", "write cpu profile to file")

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

	config.Addr = fmt.Sprintf("%s:%d", host, port)

	if config.InFile == "" {
		return config, fmt.Errorf("Invalid input file")
	}

	return config, nil
}

func main() {
	config, err := parseArgs()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	if _, err := os.Stat(config.StatFile); err == nil || !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "file %s already exist\n", config.StatFile)
		os.Exit(1)
	}

	file, err := os.OpenFile(config.StatFile, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(2)
	}
	defer file.Close()
	w := bufio.NewWriter(file)
	_, err = w.WriteString(header)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(2)
	}

	b, err := ioutil.ReadFile(config.InFile)
	// can file be opened?
	if err != nil {
		fmt.Print(err)
		os.Exit(1)
	}

	if config.CPUProf != "" {
		f, err := os.Create(config.CPUProf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	result := make(chan ConStat, config.Workers*1000)
	workers := config.Workers

	cb = cyclicbarrier.New(config.Workers + 1)

	for i := 0; i < config.Workers; i++ {
		go TcpWorker(i, config, b, result)
	}

	go func() {
		//rLimitCount := len(config.RateLimit)
		//if rLimitCount == 0 {
		time.Sleep(config.Duration)
		//} else {

		//}
		log.Printf("Shutting down")
		running = false
	}()

	start := time.Now()

	log.Printf("Starting TCP workers: %d\n", config.Workers)
	cb.Await()

LOOP:
	for {
		select {
		case r := <-result:
			if r.TimeStamp == 0 {
				if r.Proto == TCP {
					workers--
					//if workers == 0 {
					//duration = time.Since(start)
					//}
				}
				if workers <= 0 {
					break LOOP
				}
			} else {
				// write to stat file
				fmt.Fprintf(w, "%d\t%d\t%s\t%s\t%s\t%d\t%d\n", r.TimeStamp/1000, r.Id,
					ProtoToString(r.Proto), NetOperToString(r.Type),
					NetErrToString(r.Error), r.Elapsed/1000, r.Size)

				sProto, ok := stat[r.Proto]
				if !ok {
					sProto = map[NetOper]map[NetErr]int64{}
					stat[r.Proto] = sProto
				}
				sOper, ok := sProto[r.Type]
				if !ok {
					sOper = map[NetErr]int64{}
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
	duration := time.Since(start)
	err = w.Flush()
	if err != nil {
		panic(err)
	}
	log.Printf("Shutdown, results writed to %s. Test duration %s", config.StatFile, duration)

	printStat(stat, duration)
}
