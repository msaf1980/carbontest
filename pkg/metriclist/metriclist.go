package metriclist

import (
	"bufio"
	"carbontest/pkg/base"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

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

type Metric struct {
	Name string
	Min  int32
	Max  int32
	Incr int32 // increment for ITERATE

	last int32 // last value for ITERATE
}

type MetricListIterator struct {
	data []record // workers state

	delay base.RandomDuration

	metrics []Metric // metrics
	n       uint64   // metric position
}

func (m *MetricListIterator) Reset(worker int) {
	m.data[worker].iteration = 0
	m.data[worker].biter = 1
}

func (m *MetricListIterator) Value(v *Metric) int32 {
	if v.Min == v.Max {
		return v.Min
	} else if v.Incr > 0 {
		n := atomic.LoadInt32(&v.last)
		if n > v.Max || n < v.Min {
			atomic.StoreInt32(&v.last, v.Min)
			return v.Min
		} else {
			atomic.StoreInt32(&v.last, n+v.Incr)
			return n
		}
	} else {
		return base.RandomIn32(v.Min, v.Max)
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

			var delay time.Duration
			action := base.SEND
			m.data[worker].iteration++
			if m.data[worker].biter >= m.data[worker].batch {
				m.data[worker].biter = 1
				action = base.FLUSH
				delay = m.delay.Random()
			} else {
				m.data[worker].biter++
			}

			v := m.Value(&m.metrics[n])

			m.data[worker].sb.Reset()
			m.data[worker].sb.WriteString(m.metrics[n].Name)
			m.data[worker].sb.WriteString(" ")
			m.data[worker].sb.WriteString(strconv.FormatInt(int64(v), 10))
			m.data[worker].sb.WriteString(" ")
			m.data[worker].sb.WriteString(strconv.FormatInt(timestamp, 10))
			m.data[worker].sb.WriteString("\n")

			//fmt.Print(m.data[worker].sb.String())

			return base.Event{
				action,
				delay,
				m.data[worker].sb.String(),
			}
		}
	}
}

func New(metrics []Metric, workers int, batch int, samples int, delay base.RandomDuration) (*MetricListIterator, error) {
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
	m := &MetricListIterator{data: data, delay: delay, metrics: metrics}

	return m, nil
}

func LoadMetricFile(filenames []string, valueMin, valueMax, valueInc int32) ([]Metric, error) {
	var (
		file *os.File
		err  error
	)

	defer func() {
		if file != nil {
			file.Close()
		}
	}()

	metrics := make([]Metric, 0, 1024)
	for _, filename := range filenames {
		file, err = os.Open(filename)
		if err != nil {
			return nil, err
		}

		var reader *bufio.Reader

		if strings.HasSuffix(filename, ".gz") {
			gzipReader, err := gzip.NewReader(file)
			if err != nil {
				return nil, err
			}
			reader = bufio.NewReader(gzipReader)
		} else {
			reader = bufio.NewReader(file)
		}

		n := 0
		buf := make([]string, 4)
		for {
			n++
			line, err := reader.ReadString('\n')
			if err != nil {
				break
			}
			//fmt.Printf("%s \n", line)
			v, n := stringutils.SplitN(strings.TrimRight(line, "\n"), " ", buf)
			m := Metric{Name: v[0]}
			if len(v) > 2 {
				return metrics, fmt.Errorf("filename %s: %d line incorrect", filename, n)
			}
			if len(v) == 1 {
				m.Min = valueMin
				m.Max = valueMax
				m.Incr = valueInc
			} else if len(v) == 2 {
				vv, n := stringutils.SplitN(v[1], ":", buf)
				if len(v) > 3 {
					return metrics, fmt.Errorf("filename %s: %d line values field incorrect", filename, n)
				}
				m.Min, err = base.ParseInt32(vv[0], 10)
				if err != nil {
					return metrics, fmt.Errorf("filename %s: %d line min value field incorrect", filename, n)
				}
				if len(vv) == 1 {
					m.Max = m.Min
				}
				if len(vv) >= 2 {
					m.Max, err = base.ParseInt32(vv[1], 10)
					if err != nil {
						return metrics, fmt.Errorf("filename %s: %d line max value field incorrect", filename, n)
					}
					if len(vv) == 3 {
						m.Incr, err = base.ParseInt32(vv[2], 10)
						if err != nil {
							return metrics, fmt.Errorf("filename %s: %d line increment value field incorrect", filename, n)
						}
					}
				}
			}
			m.last = m.Min
			metrics = append(metrics, m)
		}
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return metrics, fmt.Errorf("filename %s: %v", filename, err)
		}
		file.Close()
		file = nil
	}

	return metrics, nil
}
