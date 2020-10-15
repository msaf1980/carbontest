package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"log"
	"math/rand"
	"time"

	"carbontest/pkg/base"
)

func RandomDuration(min time.Duration, max time.Duration) time.Duration {
	if max > min {
		return time.Duration(rand.Int63n(max.Nanoseconds()-min.Nanoseconds())+min.Nanoseconds()) * time.Nanosecond
	} else {
		return min
	}
}

func TcpWorker(id int, config config, out chan<- ConStat, mdetail chan<- string) {
	var err error
	r := ConStatNew(id, base.TCP)

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
	for running {
		//r.ResultZero()
		var start time.Time
		var end time.Time
		start = time.Now()
		con, w, conError := connect("tcp", config.Addr, config.ConTimeout, config.Compress)
		r.Elapsed = time.Since(start).Nanoseconds()
		r.Type = base.CONNECT
		r.Error = base.NetError(conError)
		r.TimeStamp = start.UnixNano()
		r.Size = 0
		out <- *r
		if conError == nil {
		LOOP_INT:
			for j := 0; running && j < config.MetricPerCon; j++ {
				metricString := fmt.Sprintf("%s.%dtest%d %d %d\n", metricPrefix, j, id, j, start.Unix())
				start = time.Now()
				err = con.SetDeadline(start.Add(config.SendTimeout))
				if err == nil {
					r.Size, err = fmt.Fprint(w, metricString)
					if err == nil {
						if config.Compress == base.GZIP {
							err = w.(*gzip.Writer).Flush()
						} else {
							err = w.(*bufio.Writer).Flush()
						}
					}
				}

				if config.SendDelayMax > 0 {
					end = time.Now()
					time.Sleep(RandomDuration(config.SendDelayMin, config.SendDelayMax))
				} else {
					end = config.RateLimiter.Take()
				}

				r.Elapsed = end.Sub(start).Nanoseconds()
				r.Type = base.SEND
				r.Error = base.NetError(err)
				r.TimeStamp = start.UnixNano()
				out <- *r
				if err == nil {
					if config.DetailFile != "" {
						mdetail <- metricString
					}
					count++
				} else {
					if config.Verbose && r.Error == base.ERROR {
						log.Print(conError)
					}
					con.Close()
					break LOOP_INT
				}
			}
			con.Close()
		} else {
			if config.Verbose && r.Error == base.ERROR {
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
	r := ConStatNew(id, base.UDP)
	r.Type = base.SEND

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
			con, w, conError := connect("udp", config.Addr, config.ConTimeout, config.Compress)
			if conError == nil {
				sended, err := fmt.Fprint(w, metricString)
				con.Close()
				r.Error = base.NetError(err)
				r.Size = sended
				if err == nil {
					if config.DetailFile != "" {
						mdetail <- metricString
					}
					count++
				}
			} else {
				r.Error = base.NetError(conError)
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
