package main

import (
	"bufio"
	"fmt"
	"sort"
	"time"
)

func printStat(start, end time.Time) {
	// Print stat
	var statVal []string
	duration := end.Sub(start).Seconds()

	for proto, opers := range totalStat {
		for oper, errors := range opers {
			for error, s := range errors {
				v := fmt.Sprintf("%s.%s.%s %d (%.4f/s)\n", proto.String(), oper.String(), error.String(),
					s, float64(s)/duration)
				statVal = append(statVal, v)
			}
		}
	}

	sort.Strings(statVal)
	for _, s := range statVal {
		fmt.Print(s)
	}
}

func printAggrStat(w *bufio.Writer, start, end time.Time, tcpStat, udpStat Stat, workers, uworkers int) {
	timeStr := start.Format(time.RFC3339)
	duration := end.Sub(start).Seconds()
	if workers > 0 {
		fmt.Fprintf(w, "%s\t%s\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\n",
			timeStr, "TCP",
			float64(tcpStat.conns)/duration, float64(tcpStat.connsErr)/duration,
			float64(tcpStat.sends)/duration, float64(tcpStat.sendsErr)/duration,
			float64(tcpStat.connsTout)/duration, float64(tcpStat.connsReset)/duration, float64(tcpStat.connsRefused)/duration,
			float64(tcpStat.sendsTout)/duration, float64(tcpStat.sendsReset)/duration, float64(tcpStat.sendsEOF)/duration,
			float64(tcpStat.connsResolve)/duration, float64(tcpStat.connsFileLimit)/duration,
			float64(tcpStat.size)/duration,
		)
	}
	if uworkers > 0 {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%.4f\t%.4f\t%s\t%s\t%s\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\n",
			timeStr, "UDP",
			"-", "-",
			float64(udpStat.sends)/duration, float64(udpStat.sendsErr)/duration,
			"-", "-", "-",
			float64(udpStat.sendsTout)/duration, float64(udpStat.sendsReset)/duration, float64(udpStat.sendsEOF)/duration,
			float64(udpStat.sendsResolve)/duration, float64(udpStat.sendsFileLimit)/duration,
			float64(udpStat.size)/duration,
		)
	}
}
