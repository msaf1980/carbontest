package metriclist

import (
	"bufio"
	"carbontest/pkg/base"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync/atomic"

	stringutils "github.com/msaf1980/go-stringutils"
)

type record struct {
	biter     int // batch iteration
	batch     int // metrics sended before buffer flushed
	iteration int // metrics in connection iteration
	samples   int // metrics, sended in one connection (if reached, connection closed)
	event     base.Event
	sb        stringutils.Builder // metric buffer
}

type metric struct {
	name string
	min  int32
	max  int32
	incr int32 // increment for ITERATE
	last int32 // last value for ITERATE
}

type MetricListIterator struct {
	data     []record // workers state
	minDelay int64
	maxDelay int64

	metrics []metric // metrics
	n       uint64   // metric position
}

func (m *MetricListIterator) Reset(worker int) {
	m.data[worker].iteration = 0
	m.data[worker].biter = 1
}

func (m *MetricListIterator) Value(v *metric) int32 {
	if v.min == v.max {
		return v.min
	} else if v.incr > 0 {
		n := atomic.LoadInt32(&v.last)
		if n > v.max || n < v.min {
			atomic.StoreInt32(&v.last, v.min)
			return v.min
		} else {
			atomic.StoreInt32(&v.last, n+v.incr)
			return n
		}
	} else {
		return base.RandomValue(v.min, v.max)
	}
}

func (m *MetricListIterator) Next(worker int, timestamp int64) base.Event {
	if m.data[worker].samples > 0 && m.data[worker].iteration >= m.data[worker].samples {
		m.Reset(worker)
		return base.Event{base.CLOSE, 0, ""}
	} else {
		for {
			n := atomic.AddUint64(&m.n, 1)
			if n > uint64(len(m.metrics)) {
				if atomic.CompareAndSwapUint64(&m.n, n, 1) {
					n = 0
				} else {
					continue
				}
			} else {
				n--
			}

			var delay int64
			action := base.SEND
			m.data[worker].iteration++
			if m.data[worker].biter >= m.data[worker].batch {
				m.data[worker].biter = 1
				action = base.FLUSH
				delay = base.RandomDuration(m.minDelay, m.maxDelay)
			} else {
				m.data[worker].biter++
			}

			v := m.Value(&m.metrics[n])

			m.data[worker].sb.Reset()
			m.data[worker].sb.WriteString(m.metrics[n].name)
			m.data[worker].sb.WriteString(" ")
			m.data[worker].sb.WriteString(strconv.FormatInt(int64(v), 10))
			m.data[worker].sb.WriteString(" ")
			m.data[worker].sb.WriteString(strconv.FormatInt(timestamp, 10))
			m.data[worker].sb.WriteString("\n")

			return base.Event{
				action,
				delay,
				m.data[worker].sb.String(),
			}
		}
	}
}

func New(metrics []metric, workers int, batch int, samples int,
	minDelay, maxDelay int64) (*MetricListIterator, error) {
	if len(metrics) == 0 {
		return nil, fmt.Errorf("metrics is empty")
	}

	data := make([]record, workers)
	if batch < 0 {
		batch = 1
	}
	if samples < batch {
		samples = batch
	}
	for id := range data {
		data[id].samples = samples
		data[id].batch = batch
		data[id].biter = 1
	}
	m := &MetricListIterator{
		data: data, minDelay: minDelay, maxDelay: maxDelay,
		metrics: metrics,
	}

	return m, nil
}

func LoadMetricFile(filename string, valueMin, valueMax, valueInc int32) ([]metric, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	metrics := make([]metric, 0, 1024)

	reader := bufio.NewReader(file)

	n := 0
	buf := make([]string, 4)
	for {
		n++
		line, _, err := reader.ReadLine()
		if err != nil {
			break
		}
		//fmt.Printf("%s \n", line)
		v, n := stringutils.SplitN(stringutils.UnsafeString(line), " ", buf)
		m := metric{name: v[0]}
		if len(v) > 2 {
			return metrics, fmt.Errorf("%d line incorrect", n)
		}
		if len(v) == 1 {
			m.min = valueMin
			m.max = valueMax
			m.incr = valueInc
		} else if len(v) == 2 {
			vv, n := stringutils.SplitN(v[1], ":", buf)
			if len(v) > 3 {
				return metrics, fmt.Errorf("%d line values field incorrect", n)
			}
			m.min, err = base.ParseInt32(vv[0], 10)
			if err != nil {
				return metrics, fmt.Errorf("%d line min value field incorrect", n)
			}
			if len(vv) == 1 {
				m.max = m.min
			}
			if len(vv) >= 2 {
				m.max, err = base.ParseInt32(vv[1], 10)
				if err != nil {
					return metrics, fmt.Errorf("%d line max value field incorrect", n)
				}
				if len(vv) == 3 {
					m.incr, err = base.ParseInt32(vv[2], 10)
					if err != nil {
						return metrics, fmt.Errorf("%d line increment value field incorrect", n)
					}
				}
			}
		}
		m.last = m.min
		metrics = append(metrics, m)
	}
	if err == io.EOF {
		err = nil
	}

	return metrics, err
}
