package base

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"
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
	Delay  time.Duration
	Send   string
}

func ParseInt32(s string, base int) (int32, error) {
	n, err := strconv.ParseInt(s, base, 32)
	return int32(n), err
}

func RandomIn32(min, max int32) int32 {
	if max > min {
		return rand.Int31n(max-min) + min
	} else {
		return min
	}
}

func RandomInt64(min, max int64) int64 {
	if max > min {
		return rand.Int63n(max-min) + min
	} else {
		return min
	}
}

type RandomDuration struct {
	Min int64 // nanoseconds
	Max int64 // nanoseconds
}

func (r *RandomDuration) IsZero() bool {
	return r.Min == 0
}

func (r *RandomDuration) Set(value string) error {
	values := strings.Split(value, ":")
	if len(values) >= 1 {
		min, err := time.ParseDuration(values[0])
		if err != nil || min < 0 {
			return fmt.Errorf("Invalid min delay value: %s", values[0])
		}
		r.Min = min.Nanoseconds()
	}
	if len(values) == 1 {
		r.Max = r.Min
	} else if len(values) == 2 {
		max, err := time.ParseDuration(values[1])
		if err != nil || max < 0 {
			return fmt.Errorf("Invalid max delay value: %s", values[1])
		}
		r.Max = max.Nanoseconds()
		if r.Min > r.Max {
			r.Min, r.Max = r.Max, r.Min
		}
	} else {
		return fmt.Errorf("Invalid delay value: %s", value)
	}

	return nil
}

func (r *RandomDuration) String() string {
	if r.Min == r.Max {
		return strconv.FormatInt(r.Min, 10)
	}
	return strconv.FormatInt(r.Min, 10) + ":" + strconv.FormatInt(r.Max, 10)
}

func (r *RandomDuration) Type() string {
	return "time.Duration[:time.Duration]"
}

func (r *RandomDuration) Random() time.Duration {
	if r.Max <= r.Min {
		return time.Duration(r.Min)
	}
	return time.Duration(RandomInt64(r.Min, r.Max))
}

type MetricIterator interface {
	Reset(worker int)                       // Reset iteration
	Next(worker int, timestamp int64) Event // Get next event (CONNECT, SEND, FLUSH, CLOSE)
}
