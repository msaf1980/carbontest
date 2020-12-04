package base

import (
	"io"
	"math/rand"
	"net"
	"strings"
)

type Proto int

const (
	TCP Proto = iota
	UDP
)

var ProtoStrings = [...]string{
	"TCP",
	"UDP",
}

func (proto *Proto) String() string {
	return ProtoStrings[*proto]
}

type CompressType int

const (
	NONE CompressType = iota
	GZIP
	//LZ4
)

type NetOper int

const (
	INIT NetOper = iota
	CONNECT
	SEND
	FLUSH
	CLOSE
)

var NetOperStrings = [...]string{
	"INIT",
	"CONNECT",
	"SEND",
	"FLUSH", // Send and flush
	"CLOSE",
}

func (oper *NetOper) String() string {
	return NetOperStrings[*oper]
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

func (err *NetErr) String() string {
	return NetErrStrings[*err]
}

type Event struct {
	Action NetOper
	Delay  int64
	Send   string
}

func RandomDuration(min, max int64) int64 {
	if max > min {
		return rand.Int63n(max-min) + min
	} else {
		return min
	}
}

type MetricIterator interface {
	Reset(worker int)                       // Reset iteration
	Next(worker int, timestamp int64) Event // Get next event (CONNECT, SEND, FLUSH, CLOSE)
}
