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
	m := New("prefix", workers, 4, 1, 1)
	tests := []struct {
		worker    int
		timeStamp int64
		want      base.Event
	}{
		{0, 123, base.Event{base.SEND, 1, "prefix.worker0.iter1 1 123\\n"}},
		{14, 254, base.Event{base.SEND, 1, "prefix.worker14.iter1 1 254\\n"}},
		{0, 254, base.Event{base.SEND, 1, "prefix.worker0.iter2 2 254\\n"}},
		{0, 123, base.Event{base.SEND, 1, "prefix.worker0.iter3 3 123\\n"}},
		{0, 123, base.Event{base.SEND, 1, "prefix.worker0.iter4 4 123\\n"}},
		{0, 123, base.Event{base.CLOSE, 0, ""}},
		{0, 124, base.Event{base.SEND, 1, "prefix.worker0.iter1 1 124\\n"}},
	}
	for n, tt := range tests {
		t.Run("Next#"+strconv.Itoa(n), func(t *testing.T) {
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
	m := New("prefix", workers, 300000000, 1, 1)
	t := time.Now().Unix()
	for n := 0; n < b.N; n++ {
		_ = m.Next(1, t)
	}
}
