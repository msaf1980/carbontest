package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"strings"
	"time"
)

type CompressType int

const (
	NONE CompressType = iota
	GZIP
	LZ4
)

type NetOper int

const (
	CONNECT NetOper = iota
	SEND
	RECV
)

var NetOperStrings = [...]string{
	"CONNECT",
	"SEND",
	"RECV",
}

func NetOperToString(oper NetOper) string {
	return NetOperStrings[oper]
}

type NetErr int

const (
	OK NetErr = iota
	ERROR
	EOF
	TIMEOUT
	LOOKUP
	REFUSED
	RESET
	FILELIMIT
)

var NetErrStrings = [...]string{
	"OK",
	"ERROR",
	"EOF",
	"TIMEOUT",
	"LOOKUP",
	"REFUSED",
	"RESET",
	"FILELIMIT",
}

func NetErrToString(err NetErr) string {
	return NetErrStrings[err]
}

// NetError return short network error description
func NetError(err error) NetErr {
	if err == nil {
		return OK
	}
	if err == io.EOF {
		return EOF
	}
	netErr, ok := err.(net.Error)
	if ok {
		if netErr.Timeout() {
			return TIMEOUT
		} else if strings.Contains(err.Error(), " lookup ") {
			return LOOKUP
		} else if strings.HasSuffix(err.Error(), ": connection refused") {
			return REFUSED
		} else if strings.HasSuffix(err.Error(), ": connection reset by peer") {
			return RESET
		} else if strings.HasSuffix(err.Error(), ": broken pipe") ||
			strings.HasSuffix(err.Error(), "EOF") {
			return EOF
		} else if strings.HasSuffix(err.Error(), ": socket: too many open files") {
			return FILELIMIT
		}
	}
	return ERROR
}

type Proto int

const (
	TCP Proto = iota
	UDP
)

var ProtoStrings = [...]string{
	"TCP",
	"UDP",
}

func ProtoToString(proto Proto) string {
	return ProtoStrings[proto]
}

// ConStat connection or send statistic
type ConStat struct {
	Id        int
	Proto     Proto
	Type      NetOper
	TimeStamp int64
	Elapsed   int64
	Error     NetErr
	Size      int
}

func ConStatNew(id int, proto Proto) *ConStat {
	r := new(ConStat)
	r.Id = id
	r.Proto = proto
	return r
}

func (r *ConStat) ConStatZero() {
	r.TimeStamp = 0
	r.Size = 0
	r.Elapsed = 0
	r.Error = OK
}

//func (r *Result) Duration() time.Duration {
//if r.Connect.Time <= 0 {
//return 0
//}
//d := r.Connect.Time
//for i := range r.Send {
//if r.Send[i].Time > 0 {
//d += r.Send[i].Time
//}
//}
//return d
//}

type Worker struct {
	out chan string // A channel to communicate to the routine
	//Interval time.Duration // The interval with which to run the Action
	//period   time.Duration // The actual period of the wait
}

func RandomDuration(min time.Duration, max time.Duration) time.Duration {
	if max > min {
		return time.Duration(rand.Int63n(max.Nanoseconds()-min.Nanoseconds())+min.Nanoseconds()) * time.Nanosecond
	} else {
		return min
	}
}

func TcpWorker(id int, config config, out chan<- ConStat, mdetail chan<- string) {
	var err error
	r := ConStatNew(id, TCP)

	defer func(ch chan<- ConStat) {
		r.ConStatZero()
		ch <- *r
	}(out)

	metricPrefix := fmt.Sprintf("%s.worker%d", config.MetricPrefix, id)
	cb.Await()
	if config.Verbose {
		log.Printf("Started TCP worker %d\n", id)
	}

	var count int64
	var w io.Writer
	for running {
		//r.ResultZero()
		var start time.Time
		var end time.Time
		start = time.Now()
		con, conError := net.DialTimeout("tcp", config.Addr, config.ConTimeout)
		r.Elapsed = time.Since(start).Nanoseconds()
		r.Type = CONNECT
		r.Error = NetError(conError)
		r.TimeStamp = start.UnixNano()
		r.Size = 0
		out <- *r
		if conError == nil {
			if config.Compress == GZIP {
				w, _ = gzip.NewWriterLevel(con, gzip.DefaultCompression)
			} else {
				w = bufio.NewWriter(con)
			}
		LOOP_INT:
			for j := 0; running && j < config.MetricPerCon; j++ {
				metricString := fmt.Sprintf("%s.%dtest%d %d %d\n", metricPrefix, j, id, j, start.Unix())
				start = time.Now()
				con.SetDeadline(start.Add(config.SendTimeout))
				r.Size, err = w.Write([]byte(metricString))
				if err == nil {
					if config.Compress == GZIP {
						err = w.(*gzip.Writer).Flush()
					} else {
						err = w.(*bufio.Writer).Flush()
					}
				}

				if config.SendDelayMax > 0 {
					end = time.Now()
					time.Sleep(RandomDuration(config.SendDelayMin, config.SendDelayMax))
				} else {
					end = config.RateLimiter.Take()
				}

				r.Elapsed = end.Sub(start).Nanoseconds()
				r.Type = SEND
				r.Error = NetError(err)
				r.TimeStamp = start.UnixNano()
				out <- *r
				if err == nil {
					if config.DetailFile != "" {
						mdetail <- metricString
					}
					count++
				} else {
					if config.Verbose && r.Error == ERROR {
						log.Print(conError)
					}
					con.Close()
					break LOOP_INT
				}
			}
			con.Close()
		} else {
			if config.Verbose && r.Error == ERROR {
				log.Print(conError)
			}
			if config.SendDelayMax > 0 {
				time.Sleep(RandomDuration(config.SendDelayMin, config.SendDelayMax))
			} else {
				config.RateLimiter.Take()
			}
		}
	}
	if config.Verbose {
		log.Printf("Ended TCP worker %d, %d metrics\n", id, count)
	}
}

func UDPWorker(id int, config config, out chan<- ConStat, mdetail chan<- string) {
	r := ConStatNew(id, UDP)
	r.Type = SEND

	defer func(ch chan<- ConStat) {
		r.ConStatZero()
		ch <- *r
	}(out)

	metricPrefix := fmt.Sprintf("%s.udpworker%d", config.MetricPrefix, id)
	cb.Await()
	if config.Verbose {
		log.Printf("Started UDP worker %d\n", id)
	}

	var count int64
	var end time.Time
	for running {
		for i := 0; running && i < 1000; i++ {
			timeStamp := time.Now().Unix()
			metricString := fmt.Sprintf("%s.%d %d %d\n", metricPrefix, i, i, timeStamp)

			start := time.Now()
			con, conError := net.Dial("udp", config.Addr)
			if conError == nil {
				sended, err := fmt.Fprintf(con, metricString)
				con.Close()
				r.Error = NetError(err)
				r.Size = sended
				if err == nil {
					if config.DetailFile != "" {
						mdetail <- metricString
					}
					count++
				}
			} else {
				r.Error = NetError(conError)
				r.Size = 0
			}

			if config.SendDelayMax > 0 {
				end = time.Now()
				time.Sleep(RandomDuration(config.SendDelayMin, config.SendDelayMax))
			} else {
				end = config.RateLimiter.Take()
			}

			r.Elapsed = end.Sub(start).Nanoseconds()
			r.TimeStamp = start.UnixNano()
			out <- *r
		}
	}
	if config.Verbose {
		log.Printf("Ended UDP worker %d, %d metrics\n", id, count)
	}
}
