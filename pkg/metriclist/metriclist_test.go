package metriclist

import (
	"carbontest/pkg/base"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestLoadMetricFile(t *testing.T) {
	var min int32 = 1
	var max int32 = 9
	var incr int32 = 3

	tests := []struct {
		filenames []string
		want      []Metric
		wantErr   bool
	}{
		{[]string{"test/nonexistent"}, nil, true},
		{[]string{"test/metrics.txt"}, []Metric{
			{"test.metric.N1", min, max, incr, min},
			{"test.metric2.N11", 1, 10, 0, 1},
			{"test.metric14.N15", 2, 37, 4, 2},
		}, false},
	}

	for _, tt := range tests {
		t.Run(strings.Join(tt.filenames, " , "), func(t *testing.T) {
			got, err := LoadMetricFile(tt.filenames, min, max, incr)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadMetricFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LoadMetricFile() = \n%v\nwant\n%v", got, tt.want)
			}
		})
	}
}

func TestMetricListIterator_Next(t *testing.T) {
	var min int32 = 2
	var max int32 = 7
	var incr int32 = 3
	workers := 16
	metrics, err := LoadMetricFile([]string{"test/metrics2.txt.gz", "test/metrics.txt"}, min, max, incr)
	if err != nil {
		t.Error(err)
	}
	it, err := New(metrics, workers, 2, 4, base.RandomDuration{Min: 0, Max: 0})
	if err != nil {
		t.Error(err)
	}

	tests := []struct {
		worker    int
		timeStamp int64
		want      base.Event
	}{
		{0, 123, base.Event{base.SEND, 0, "test.metric.N2 2 123\\n"}},
		{14, 254, base.Event{base.SEND, 0, "test.metric2.N22 1 254\\n"}},
		{0, 253, base.Event{base.FLUSH, 0, "test.metric14.N35 2 253\\n"}},
		{0, 254, base.Event{base.SEND, 0, "test.metric.N1 2 254\\n"}},
		{0, 123, base.Event{base.FLUSH, 0, "test.metric2.N11 6 123\\n"}},
		{0, 123, base.Event{base.CLOSE, 0, ""}},
		{0, 124, base.Event{base.SEND, 0, "test.metric14.N15 2 124\\n"}},
		{0, 125, base.Event{base.FLUSH, 0, "test.metric.N2 5 125\\n"}},
	}
	for n, tt := range tests {
		t.Run("#"+strconv.Itoa(n), func(t *testing.T) {
			got := it.Next(tt.worker, tt.timeStamp)
			got.Send = strings.Replace(got.Send, "\n", "\\n", -1)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MetricListIterator.Next() = \n%v\nwant\n%v", got, tt.want)
			}
		})
	}
}
