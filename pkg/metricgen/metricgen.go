package metricgen

import (
	"carbontest/pkg/base"
	"fmt"
	"strconv"
)

type record struct {
	metricPrefix string
	biter        int // batch iteration
	batch        int
	iteration    int // metrics in connection iteration
	samples      int
	event        base.Event
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
		//s := fmt.Sprintf("%s.iter%d %d %d\n", m.data[worker].metricPrefix, m.data[worker].iteration, m.data[worker].iteration, timestamp)
		it := strconv.Itoa(m.data[worker].iteration)
		s := m.data[worker].metricPrefix + ".iter" + it + " " + it + " " + strconv.FormatInt(timestamp, 10) + "\n"
		return base.Event{
			action,
			delay,
			s,
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
	}
	m := &MetricGenIterator{data: data, minDealy: min, maxDelay: max}

	return m
}
