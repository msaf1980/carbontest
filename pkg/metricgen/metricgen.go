package metricgen

import (
	"fmt"
	"time"
)

type worker struct {
	metricPrefix string
}

type MetricGenIterator struct {
	metricPrefixes []string
}

func (m *MetricGenIterator) NextTimestamp(worker int, iteration int) string {
	return m.Next(worker, iteration, time.Now().Unix())
}

func (m *MetricGenIterator) Next(worker int, iteration int, timestamp int64) string {
	return fmt.Sprintf("%s.iter%d %d %d\n", m.metricPrefixes[worker], iteration, iteration, timestamp)
}

func New(metricPrefix string, workers int) *MetricGenIterator {
	metricPrefixes := make([]string, workers)
	for id := range metricPrefixes {
		metricPrefixes[id] = fmt.Sprintf("%s.worker%d", metricPrefix, id)
	}
	m := &MetricGenIterator{metricPrefixes: metricPrefixes}

	return m
}
