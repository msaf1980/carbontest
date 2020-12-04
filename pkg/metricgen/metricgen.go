package metricgen

import (
	"carbontest/pkg/base"
	"fmt"
	"strconv"

	stringutils "github.com/msaf1980/go-stringutils"
)

type record struct {
	metricPrefix string
	biter        int // batch iteration
	batch        int
	iteration    int // metrics in connection iteration
	samples      int
	event        base.Event
	sb           stringutils.Builder // metric buffer
}

type MetricGenIterator struct {
	data     []record // workers state
	minDealy int64
	maxDelay int64
}

func (m *MetricGenIterator) Reset(worker int) {
	m.data[worker].iteration = 0
	m.data[worker].biter = 1
}

func (m *MetricGenIterator) Next(worker int, timestamp int64) base.Event {
	if m.data[worker].samples > 0 && m.data[worker].iteration >= m.data[worker].samples {
		m.Reset(worker)
		return base.Event{base.CLOSE, 0, ""}
	} else {
		var delay int64
		action := base.SEND
		m.data[worker].iteration++
		if m.data[worker].biter >= m.data[worker].batch {
			m.data[worker].biter = 1
			action = base.FLUSH
			delay = base.RandomDuration(m.minDealy, m.maxDelay)
		} else {
			m.data[worker].biter++
		}
		it := strconv.Itoa(m.data[worker].iteration)
		m.data[worker].sb.Reset()
		m.data[worker].sb.WriteString(m.data[worker].metricPrefix)
		m.data[worker].sb.WriteString(".iter")
		m.data[worker].sb.WriteString(it)
		m.data[worker].sb.WriteString(" ")
		m.data[worker].sb.WriteString(it)
		m.data[worker].sb.WriteString(" ")
		m.data[worker].sb.WriteString(strconv.FormatInt(timestamp, 10))
		m.data[worker].sb.WriteString("\n")
		//s := m.data[worker].metricPrefix + ".iter" + it + " " + it + " " + strconv.FormatInt(timestamp, 10) + "\n"
		return base.Event{
			action,
			delay,
			m.data[worker].sb.String(),
		}
	}
}

func New(metricPrefix string, workers int, batch int, samples int, min int64, max int64) *MetricGenIterator {
	data := make([]record, workers)
	if batch < 0 {
		batch = 1
	}
	if samples < batch {
		samples = batch
	}
	for id := range data {
		data[id].metricPrefix = fmt.Sprintf("%s.worker%d", metricPrefix, id)
		data[id].samples = samples
		data[id].batch = batch
		data[id].biter = 1
		data[id].sb.Grow(100)
	}
	m := &MetricGenIterator{data: data, minDealy: min, maxDelay: max}

	return m
}
