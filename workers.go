package main

import (
	"io"
	"log"
	"net"
	"time"

	"carbontest/pkg/base"
	"carbontest/pkg/metricgen"
)

func delay(config *config, delay int64) time.Time {
	if config.RateLimiter != nil {
		return config.RateLimiter.Take()
	} else {
		end := time.Now()
		if delay > 0 {
			time.Sleep(time.Duration(delay))
		}
		return end
	}
}

func TcpWorker(id int, c config, out chan<- ConStat, mdetail chan<- string, iter metricgen.MetricIterator) {
	r := ConStatNew(id, base.TCP)

	defer func(ch chan<- ConStat) {
		r.ConStatZero()
		ch <- *r
	}(out)

	cb.Await()
	if c.Verbose {
		log.Printf("Started TCP worker %d\n", id)
	}

	var count int64
	var con net.Conn
	var w io.Writer
	var err error
	for running {
		//r.ResultZero()
		//var end time.Time
		start := time.Now()
		e := iter.Next(id, start.Unix())
		if e.Action == base.CLOSE {
			if con != nil {
				con.Close()
				con = nil
				w = nil
			}
		} else if con == nil && e.Action == base.SEND {
			con, w, err = connectWriter("tcp", c.Addr, c.ConTimeout, c.Compress)
			r.Elapsed = time.Since(start).Nanoseconds()
			r.Type = base.CONNECT
			r.Error = base.NetError(err)
			r.TimeStamp = start.UnixNano()
			r.Size = 0
			out <- *r
		}
		if err == nil && e.Action == base.SEND {
			start = time.Now()
			err = con.SetDeadline(start.Add(c.SendTimeout))
			if err == nil {
				//r.Size, err = fmt.Fprint(w, e.Send)
				r.Size, err = w.Write([]byte(e.Send))
				if err == nil {
					if err = flushWriter(w, c.Compress); err == nil {
						count++
					}
				}
			}
		}
		end := delay(&c, e.Delay)
		r.Elapsed = end.Sub(start).Nanoseconds()
		r.Type = base.SEND
		r.Error = base.NetError(err)
		r.TimeStamp = start.UnixNano()
		out <- *r
		if err == nil {
			if c.DetailFile != "" {
				mdetail <- e.Send
			}
		} else {
			if c.Verbose && r.Error == base.ERROR {
				log.Print(err)
			}
			if con != nil {
				con.Close()
				w = nil
				con = nil
			}
		}
	}
	if c.Verbose {
		log.Printf("Ended TCP worker %d, %d metrics\n", id, count)
	}
}

func UDPWorker(id int, c config, out chan<- ConStat, mdetail chan<- string, iter metricgen.MetricIterator) {
	r := ConStatNew(id, base.UDP)
	r.Type = base.SEND

	defer func(ch chan<- ConStat) {
		r.ConStatZero()
		ch <- *r
	}(out)

	cb.Await()
	if c.Verbose {
		log.Printf("Started UDP worker %d\n", id)
	}

	var count int64
	var con net.Conn
	var w io.Writer
	var err error
	for running {
		start := time.Now()
		e := iter.Next(id, start.Unix())
		if e.Action == base.CLOSE {
			if con != nil {
				con.Close()
				w = nil
				con = nil
			}
		} else if con == nil && e.Action == base.SEND {
			start = time.Now()
			con, w, err = connectWriter("udp", c.Addr, c.ConTimeout, c.Compress)
			r.Elapsed = time.Since(start).Nanoseconds()
			r.Type = base.CONNECT
			r.Error = base.NetError(err)
			r.TimeStamp = start.UnixNano()
			r.Size = 0
			out <- *r
		}

		if err == nil && e.Action == base.SEND {
			start = time.Now()
			//sended, err := fmt.Fprint(w, e.Send)
			sended, err := w.Write([]byte(e.Send))
			flushWriter(w, c.Compress)
			r.Error = base.NetError(err)
			r.Size = sended
			if err == nil {
				if c.DetailFile != "" {
					mdetail <- e.Send
				}
				count++
			} else if con != nil {
				con.Close()
				w = nil
				con = nil
			}
		} else {
			r.Error = base.NetError(err)
			r.Size = 0
		}
		end := delay(&c, e.Delay)
		r.Elapsed = end.Sub(start).Nanoseconds()
		r.TimeStamp = start.UnixNano()
		out <- *r
	}
	if con != nil {
		con.Close()
	}
	if c.Verbose {
		log.Printf("Ended UDP worker %d, %d metrics\n", id, count)
	}
}
