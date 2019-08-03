package bench

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
	"time"
)

func BenchmarkSprintf(b *testing.B) {
	metricPrefix := fmt.Sprintf("test.workeri%d", 1000)
	for i := 0; i < b.N; i++ {
		var buffer bytes.Buffer
		start := time.Now()
		metricString := fmt.Sprintf("%s.%d %d %d\n", metricPrefix, 4, 8, start.Unix())
		buffer.WriteString(metricString)
	}
}

func BenchmarkSprint(b *testing.B) {
	metricPrefix := fmt.Sprintf("test.workeri%d", 1000)
	for i := 0; i < b.N; i++ {
		var buffer bytes.Buffer
		start := time.Now()
		//metricString := fmt.Sprint("%s.%d %d %d\n", metricPrefix, 4, 8, start.Unix())
		metricString := fmt.Sprint(metricPrefix, ".", 4, " ", 8, " ", start.Unix(), "\n")
		buffer.WriteString(metricString)
	}
}

func metricBuffer(prefix string, id int, value int, timeStamp int64) []byte {
	var buffer bytes.Buffer
	buffer.WriteString(prefix)
	buffer.WriteString(".")

	buffer.WriteString(strconv.Itoa(id))
	buffer.WriteString(" ")
	buffer.WriteString(strconv.Itoa(value))
	buffer.WriteString(" ")
	buffer.WriteString(strconv.FormatInt(timeStamp, 10))
	buffer.WriteString("\n")
	return buffer.Bytes()
}

func BenchmarkByteBuffer(b *testing.B) {
	metricPrefix := fmt.Sprintf("test.workeri%d", 1000)
	for i := 0; i < b.N; i++ {
		start := time.Now()
		_ = metricBuffer(metricPrefix, 4, 8, start.Unix())
		//metricString := fmt.Sprint("%s.%d %d %d\n", metricPrefix, 4, 8, start.Unix())
	}
}
