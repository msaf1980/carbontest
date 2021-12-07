package metricgen

import (
	"carbontest/pkg/base"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMetricGenIterator(t *testing.T) {
	workers := 16
	m, err := New("prefix", workers, 2, 4, base.RandomDuration{Min: 0, Max: 0})
	if err != nil {
		t.Error(err)
	}

	tests := []struct {
		worker    int
		timeStamp int64
		want      base.Event
	}{
		{0, 123, base.Event{base.SEND, 0, "prefix.worker0.iter1 1 123\\n"}},
		{14, 254, base.Event{base.SEND, 0, "prefix.worker14.iter1 1 254\\n"}},
		{0, 253, base.Event{base.FLUSH, 0, "prefix.worker0.iter2 2 253\\n"}},
		{0, 254, base.Event{base.SEND, 0, "prefix.worker0.iter3 3 254\\n"}},
		{0, 123, base.Event{base.FLUSH, 0, "prefix.worker0.iter4 4 123\\n"}},
		{0, 123, base.Event{base.CLOSE, 0, ""}},
		{0, 124, base.Event{base.SEND, 0, "prefix.worker0.iter1 1 124\\n"}},
	}
	for n, tt := range tests {
		t.Run("#"+strconv.Itoa(n), func(t *testing.T) {
			got := m.Next(tt.worker, tt.timeStamp)
			got.Send = strings.Replace(got.Send, "\n", "\\n", -1)
			if got != tt.want {
				t.Errorf("Next(%d) = '%+v', want '%+v'", tt.worker, got, tt.want)
			}
		})
	}
}

func BenchmarkMetricGenIteratorNext(b *testing.B) {
	workers := 16
	m, err := New("prefix", workers, 300000000, 1, base.RandomDuration{Min: 1, Max: 1})
	if err != nil {
		b.Error(err)
	}
	ts := time.Now().Unix()
	for n := 0; n < b.N; n++ {
		_ = m.Next(1, ts)
	}
}

func BenchmarkMetricGenIteratorNextBatch(b *testing.B) {
	workers := 16
	m, err := New("prefix", workers, 300000000, 10, base.RandomDuration{Min: 10, Max: 1})
	if err != nil {
		b.Error(err)
	}
	ts := time.Now().Unix()
	for n := 0; n < b.N; n++ {
		_ = m.Next(1, ts)
	}
}
