package main

import (
	"log"
	"strconv"
	"time"

	lockfree_queue "github.com/msaf1980/go-lockfree-queue"
	graphite "github.com/msaf1980/graphite-golang"
)

type GraphiteQueue struct {
	queue     *lockfree_queue.Queue
	graphite  *graphite.Graphite
	batchSend int

	failed  bool
	running bool
}

// GraphiteInit init metric sender
func GraphiteInit(address string, prefix string, queueSize int, batchSend int) (*GraphiteQueue, error) {
	return new(GraphiteQueue).init(address, prefix, queueSize, batchSend)
}

func (g *GraphiteQueue) init(address string, prefix string, queueSize int, batchSend int) (*GraphiteQueue, error) {
	if len(address) > 0 {
		var err error
		g.queue = lockfree_queue.NewQueue(queueSize)
		if batchSend < 1 {
			g.batchSend = 1
		} else {
			g.batchSend = batchSend
		}
		g.graphite, err = graphite.NewGraphiteWithMetricPrefix(address, prefix)
		return g, err
	} else {
		return g, nil
	}
}

// Put metric to queue
func (g *GraphiteQueue) Put(name, value string, timestamp int64) {
	if g.queue == nil {
		return
	}
	m := graphite.NewMetricPtr(name, value, timestamp)
	if !g.queue.Put(m) {
		// drop last two elements and try put again
		g.queue.Get()
		g.queue.Get()
		g.queue.Put(m)
	}
}

// Run goroutune for queue read and send metrics
func (g *GraphiteQueue) Run() {
	if g.queue == nil {
		return
	}
	g.running = true
	go func() {
		metrics := make([]*graphite.Metric, g.batchSend)
		i := 0
		nextSend := false
		for g.running {
			if i == g.batchSend || nextSend {
				if !g.graphite.IsConnected() {
					err := g.graphite.Connect()
					if err != nil {
						_ = g.graphite.Disconnect()
						if !g.failed {
							g.failed = true
							log.Printf("relaymon: graphite %s", err.Error())
						}
						time.Sleep(1 * time.Second)
						continue
					}
				}
				err := g.graphite.SendMetricPtrs(metrics[0:i])
				if err == nil {
					i = 0
					nextSend = false
					if g.failed {
						g.failed = false
						log.Printf("relaymon: graphite metrics sended")
					}
				} else {
					if !g.failed {
						g.failed = true
						log.Printf("relaymon: graphite %s", err.Error())
						g.graphite.Disconnect()
					}
					continue
				}
			}
			if i < g.batchSend {
				m, _ := g.queue.Get()
				if m != nil {
					metrics[i] = m.(*graphite.Metric)
					i++
				} else {
					m, _ := g.queue.Get()
					if m != nil {
						metrics[i] = m.(*graphite.Metric)
						i++
					} else if i == 0 {
						time.Sleep(1 * time.Second)
					} else {
						nextSend = true
					}
				}
			}
		}
	}()
}

// Stop goroutune for queue read and send metrics
func (g *GraphiteQueue) Stop() {
	g.running = false
}

func sendAggrStat(graphite *GraphiteQueue, start, end time.Time, tcpStat, udpStat Stat, workers, uworkers int) {
	timeStamp := start.Unix()
	duration := end.Sub(start).Seconds()
	if workers > 0 {
		graphite.Put("tcp.size_ps", strconv.FormatFloat(float64(tcpStat.size)/duration, 'g', 6, 64), timeStamp)

		graphite.Put("tcp.conns_ps", strconv.FormatFloat(float64(tcpStat.conns)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.conns_err_ps", strconv.FormatFloat(float64(tcpStat.connsErr)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.conns_err.detail.tout_ps", strconv.FormatFloat(float64(tcpStat.connsTout)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.conns_err.detail.reset_ps", strconv.FormatFloat(float64(tcpStat.connsReset)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.conns_err.detail.refused_ps", strconv.FormatFloat(float64(tcpStat.connsRefused)/duration, 'g', 6, 64), timeStamp)

		graphite.Put("tcp.sends_ps", strconv.FormatFloat(float64(tcpStat.sends)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.sends_err_ps", strconv.FormatFloat(float64(tcpStat.sendsErr)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.sends_err.detail.tout_ps", strconv.FormatFloat(float64(tcpStat.sendsTout)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.sends_err.detail.reset_ps", strconv.FormatFloat(float64(tcpStat.sendsReset)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.sends_err.detail.eof_ps", strconv.FormatFloat(float64(tcpStat.sendsEOF)/duration, 'g', 6, 64), timeStamp)

		graphite.Put("tcp.conns_err.detail.resolve_err_ps", strconv.FormatFloat(float64(tcpStat.connsResolve)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.conns_err.detail.limits_err_ps", strconv.FormatFloat(float64(tcpStat.connsFileLimit)/duration, 'g', 6, 64), timeStamp)
	}
	if uworkers > 0 {
		graphite.Put("udp.size_ps", strconv.FormatFloat(float64(udpStat.size)/duration, 'g', 6, 64), timeStamp)

		graphite.Put("udp.sends_ps", strconv.FormatFloat(float64(udpStat.sends)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("udp.sends_err_ps", strconv.FormatFloat(float64(udpStat.sendsErr)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("udp.sends_err.detail.tout_ps", strconv.FormatFloat(float64(udpStat.sendsTout)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("udp.sends_err.detail.reset_ps", strconv.FormatFloat(float64(udpStat.sendsReset)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("udp.sends_err.detail.eof_ps", strconv.FormatFloat(float64(udpStat.sendsEOF)/duration, 'g', 6, 64), timeStamp)

		graphite.Put("udp.sends_err.detail.resolve_err_ps", strconv.FormatFloat(float64(udpStat.connsResolve)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("udp.sends_err.detail.limits_err_ps", strconv.FormatFloat(float64(udpStat.connsFileLimit)/duration, 'g', 6, 64), timeStamp)
	}
}
