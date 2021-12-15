package main

import (
	"bufio"
	"carbontest/pkg/base"
	"compress/gzip"
	"io"
	"net"
	"time"
)

type BufferedWriter interface {
	io.Writer

	Flush() error
}

type BufferedStringWriter interface {
	BufferedWriter
	io.StringWriter
}

// ConStat connection or send statistic
type ConStat struct {
	Id        int
	Proto     base.Proto
	Type      base.NetOper
	TimeStamp int64 // nanosec
	Elapsed   int64
	Error     base.NetErr
	Size      int
	Metrics   int
}

func ConStatNew(id int, proto base.Proto) *ConStat {
	r := new(ConStat)
	r.Id = id
	r.Proto = proto
	return r
}

func (r *ConStat) ConStatZero() {
	r.TimeStamp = 0
	r.Size = 0
	r.Elapsed = 0
	r.Error = base.OK
}

func connectWriter(proto string, addr string, conTimeout time.Duration, c base.CompressType) (net.Conn, BufferedWriter, error) {
	con, err := net.DialTimeout(proto, addr, conTimeout)
	if err != nil {
		return nil, nil, err
	}
	var w BufferedWriter
	if c == base.GZIP {
		w = gzip.NewWriter(con)
	} else {
		w = bufio.NewWriter(con)
	}
	return con, w, err
}
