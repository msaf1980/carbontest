package main

import (
	"carbontest/pkg/base"
	"compress/gzip"
	"io"
	"net"
	"time"
)

// ConStat connection or send statistic
type ConStat struct {
	Id        int
	Proto     base.Proto
	Type      base.NetOper
	TimeStamp int64 // nanosec
	Elapsed   int64
	Error     base.NetErr
	Size      int
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

func connect(proto string, addr string, conTimeout time.Duration, compress base.CompressType) (net.Conn, io.Writer, error) {
	con, err := net.DialTimeout(proto, addr, conTimeout)
	if err != nil {
		return nil, nil, err
	}
	var w io.Writer
	if compress == base.GZIP {
		w, err = gzip.NewWriterLevel(con, gzip.DefaultCompression)
		// } else {
		// 	w = bufio.NewWriter(con)
	}
	return con, w, err
}
