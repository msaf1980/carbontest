package main

import (
	"carbontest/pkg/base"
	"compress/gzip"
	"io"
	"net"
	"time"
)

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
