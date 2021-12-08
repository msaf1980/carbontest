package main

import (
	"log"
	"net"
	"sync/atomic"
	"time"

	"carbontest/pkg/base"
)

func delay(config *WorkerConfig, delay time.Duration, action base.NetOper) time.Time {
	if action == base.SEND {
		return time.Now()
	}
	if config.RateLimiter != nil {
		return config.RateLimiter.Take()
	} else {
		end := time.Now()
		if delay > 0 {
			time.Sleep(delay)
		}
		return end
	}
}

func TcpWorker(id int, localConfig *LocalConfig, sharedConfig *SharedConfig, workerConfig *WorkerConfig, out chan<- ConStat, mdetail chan<- string, iter base.MetricIterator) {
	r := ConStatNew(id, base.TCP)

	defer func(ch chan<- ConStat) {
		r.ConStatZero()
		ch <- *r
	}(out)

	cb.Await()
	if localConfig.Verbose {
		log.Printf("Started TCP worker %d\n", id)
	}

	var count int64
	var con net.Conn
	var w BufferedWriter
	var err error
	for atomic.LoadInt32(&running) == 1 {
		//r.ResultZero()
		//var end time.Time
		start := time.Now()
		e := iter.Next(id, start.Unix())
		if e.Action == base.CLOSE {
			if con != nil {
				err = w.Flush()
				if err == nil {
					err = con.Close()
				} else {
					con.Close()
				}
				con = nil
				w = nil
			}
		} else if con == nil && (e.Action == base.SEND || e.Action == base.FLUSH) {
			con, w, err = connectWriter("tcp", workerConfig.T.Address, workerConfig.T.ConTimeout, workerConfig.CompressType)
			r.Elapsed = time.Since(start).Nanoseconds()
			r.Type = base.CONNECT
			r.Error = base.NetError(err)
			r.TimeStamp = start.UnixNano()
			r.Size = 0
			out <- *r
		}
		if err == nil && (e.Action == base.SEND || e.Action == base.FLUSH) {
			start = time.Now()
			err = con.SetDeadline(start.Add(workerConfig.T.SendTimeout))
			if err == nil {
				//r.Size, err = fmt.Fprint(w, e.Send)
				r.Size, err = w.Write([]byte(e.Send))
				if err == nil && e.Action == base.FLUSH {
					err = w.Flush()
				}
				if err == nil {
					count++
				}
			}
		}
		end := delay(workerConfig, e.Delay, e.Action)
		r.Elapsed = end.Sub(start).Nanoseconds()
		r.Type = base.SEND
		r.Error = base.NetError(err)
		r.TimeStamp = start.UnixNano()
		out <- *r
		if err == nil {
			if len(localConfig.DetailFile) > 0 {
				mdetail <- e.Send
			}
		} else {
			if localConfig.Verbose && r.Error == base.ERROR {
				log.Print(err)
			}
			if con != nil {
				con.Close()
				w = nil
				con = nil
			}
		}
	}
	if localConfig.Verbose {
		log.Printf("Ended TCP worker %d, %d metrics\n", id, count)
	}
}

func UDPWorker(id int, localConfig *LocalConfig, sharedConfig *SharedConfig, workerConfig *WorkerConfig, out chan<- ConStat, mdetail chan<- string, iter base.MetricIterator) {
	r := ConStatNew(id, base.UDP)
	r.Type = base.SEND

	defer func(ch chan<- ConStat) {
		r.ConStatZero()
		ch <- *r
	}(out)

	cb.Await()
	if localConfig.Verbose {
		log.Printf("Started UDP worker %d\n", id)
	}

	var count int64
	var con net.Conn
	var w BufferedWriter
	var err error
	for atomic.LoadInt32(&running) == 1 {
		start := time.Now()
		e := iter.Next(id, start.Unix())
		if e.Action == base.CLOSE {
			if con != nil {
				err = w.Flush()
				con.Close()
				w = nil
				con = nil
			}
		} else if con == nil && (e.Action == base.SEND || e.Action == base.FLUSH) {
			start = time.Now()
			con, w, err = connectWriter("udp", workerConfig.T.Address, workerConfig.T.ConTimeout, workerConfig.CompressType)
			r.Elapsed = time.Since(start).Nanoseconds()
			r.Type = base.CONNECT
			r.Error = base.NetError(err)
			r.TimeStamp = start.UnixNano()
			r.Size = 0
			out <- *r
		}

		if err == nil && (e.Action == base.SEND || e.Action == base.FLUSH) {
			start = time.Now()
			//sended, err := fmt.Fprint(w, e.Send)
			sended, err := w.Write([]byte(e.Send))
			if err == nil && e.Action == base.FLUSH {
				err = w.Flush()
			}
			r.Error = base.NetError(err)
			r.Size = sended
			if err == nil {
				if len(localConfig.DetailFile) > 0 {
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
		end := delay(workerConfig, e.Delay, e.Action)
		r.Elapsed = end.Sub(start).Nanoseconds()
		r.TimeStamp = start.UnixNano()
		out <- *r
	}
	if con != nil {
		con.Close()
	}
	if localConfig.Verbose {
		log.Printf("Ended UDP worker %d, %d metrics\n", id, count)
	}
}
