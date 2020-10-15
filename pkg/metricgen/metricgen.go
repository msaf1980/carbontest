package metricgen

import (
	"carbontest/pkg/base"
	"fmt"
	"math/rand"
)

func RandomDuration(min int64, max int64) int64 {
	if max > min {
		return rand.Int63n(max-min) + min
	} else {
		return min
	}
}

type MetricIterator interface {
	Reset(worker int)                            // Reset iteration
	Next(worker int, timestamp int64) base.Event // Get next event (CONNECT, SEND, CLOSE)
}

type record struct {
	metricPrefix string
	iteration    uint
	samples      uint
	event        base.Event
}

type MetricGenIterator struct {
	data []record
	min  int64
	max  int64
}

func (m *MetricGenIterator) Reset(worker int) {
	m.data[worker].iteration = 0
}

func (m *MetricGenIterator) Next(worker int, timestamp int64) base.Event {
	if m.data[worker].samples > 0 && m.data[worker].iteration >= m.data[worker].samples {
		m.data[worker].iteration = 0
		return base.Event{base.CLOSE, 0, ""}
	} else {
		m.data[worker].iteration++
		return base.Event{
			base.SEND,
			RandomDuration(m.min, m.max),
			fmt.Sprintf("%s.iter%d %d %d\n", m.data[worker].metricPrefix, m.data[worker].iteration, m.data[worker].iteration, timestamp),
		}
	}
}

func New(metricPrefix string, workers int, samples uint, min int64, max int64) *MetricGenIterator {
	data := make([]record, workers)
	for id := range data {
		data[id].metricPrefix = fmt.Sprintf("%s.worker%d", metricPrefix, id)
		data[id].samples = samples
	}
	m := &MetricGenIterator{data: data, min: min, max: max}

	return m
}
