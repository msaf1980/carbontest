package metricgen

import (
	"testing"
)

func TestMetricGenIterator(t *testing.T) {
	workers := 16
	m := New("prefix", workers)
	tests := []struct {
		worker    int
		iteration int
		timeStamp int64
		want      string
	}{
		{0, 0, 123, "prefix.worker0.iter0 0 123\n"},
		{14, 2, 254, "prefix.worker14.iter2 2 254\n"},
		{0, 1, 254, "prefix.worker0.iter1 1 254\n"},
		{0, 0, 123, "prefix.worker0.iter0 0 123\n"},
	}
	for _, tt := range tests {
		t.Run("Next", func(t *testing.T) {
			got := m.Next(tt.worker, tt.iteration, tt.timeStamp)
			if got != tt.want {
				t.Errorf("Next(%d, %d) = '%+v', want '%+v'", tt.worker, tt.iteration, got, tt.want)
			}
		})
	}
}
