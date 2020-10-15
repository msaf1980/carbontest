package base

import (
	"io"
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
	CONNECT NetOper = iota
	SEND
	//RECV
)

var NetOperStrings = [...]string{
	"CONNECT",
	"SEND",
	"RECV",
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
