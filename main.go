package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"

	//"strconv"
	"strings"
	"time"

	"github.com/msaf1980/cyclicbarrier"
	"github.com/spf13/cobra"

	"carbontest/pkg/base"
	"carbontest/pkg/metriclist"
)

type StringSlice []string

func (u *StringSlice) Set(value string) error {
	if len(value) == 0 {
		return errors.New("empty file")
	}
	*u = append(*u, value)
	return nil
}

func (u *StringSlice) String() string {
	return "[ " + strings.Join(*u, ", ") + " ]"
}

func (u *StringSlice) Type() string {
	return "[]string"
}

var (
	mainConfig MainConfig

	cb *cyclicbarrier.CyclicBarrier

	totalStat      = map[base.Proto]map[base.NetOper]map[base.NetErr]int64{}
	stat           = map[base.Proto]map[base.NetOper]map[base.NetErr]int64{}
	startTimestamp int64
	endTimestamp   int64

	// aggrTime int
	// aggrTimestamp int64
	running bool = true
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

func sendAggrStat(graphite *GraphiteQueue, start, end time.Time, tcpStat, udpStat Stat, workers, uworkers int) {
	timeStamp := start.Unix()
	duration := end.Sub(start).Seconds()
	if workers > 0 {
		graphite.Put("tcp.size_ps", strconv.FormatFloat(float64(tcpStat.size)/duration, 'g', 6, 64), timeStamp)

		graphite.Put("tcp.conns_ps", strconv.FormatFloat(float64(tcpStat.conns)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.conns_err_ps", strconv.FormatFloat(float64(tcpStat.connsErr)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.conns_err.detail.tout_ps", strconv.FormatFloat(float64(tcpStat.connsTout)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.conns_err.detail.reset_ps", strconv.FormatFloat(float64(tcpStat.connsReset)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.conns_err.detail.refused_ps", strconv.FormatFloat(float64(tcpStat.connsRefused)/duration, 'g', 6, 64), timeStamp)

		graphite.Put("tcp.sends_ps", strconv.FormatFloat(float64(tcpStat.sends)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.sends_err_ps", strconv.FormatFloat(float64(tcpStat.sendsErr)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.sends_err.detail.tout_ps", strconv.FormatFloat(float64(tcpStat.sendsTout)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.sends_err.detail.reset_ps", strconv.FormatFloat(float64(tcpStat.sendsReset)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.sends_err.detail.eof_ps", strconv.FormatFloat(float64(tcpStat.sendsEOF)/duration, 'g', 6, 64), timeStamp)

		graphite.Put("tcp.conns_err.detail.resolve_err_ps", strconv.FormatFloat(float64(tcpStat.connsResolve)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("tcp.conns_err.detail.limits_err_ps", strconv.FormatFloat(float64(tcpStat.connsFileLimit)/duration, 'g', 6, 64), timeStamp)
	}
	if uworkers > 0 {
		graphite.Put("udp.size_ps", strconv.FormatFloat(float64(udpStat.size)/duration, 'g', 6, 64), timeStamp)

		graphite.Put("udp.sends_ps", strconv.FormatFloat(float64(udpStat.sends)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("udp.sends_err_ps", strconv.FormatFloat(float64(udpStat.sendsErr)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("udp.sends_err.detail.tout_ps", strconv.FormatFloat(float64(udpStat.sendsTout)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("udp.sends_err.detail.reset_ps", strconv.FormatFloat(float64(udpStat.sendsReset)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("udp.sends_err.detail.eof_ps", strconv.FormatFloat(float64(udpStat.sendsEOF)/duration, 'g', 6, 64), timeStamp)

		graphite.Put("udp.sends_err.detail.resolve_err_ps", strconv.FormatFloat(float64(udpStat.connsResolve)/duration, 'g', 6, 64), timeStamp)
		graphite.Put("udp.sends_err.detail.limits_err_ps", strconv.FormatFloat(float64(udpStat.connsFileLimit)/duration, 'g', 6, 64), timeStamp)
	}
}

func standaloneRun(cmd *cobra.Command, args []string) {
	var err error

	if len(args) > 0 {
		cmd.Help()
		os.Exit(1)
	}

	if err := localPostConfig(&mainConfig.Local); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	workerConfig := mainConfig.Workers["localhost"]

	if len(mainConfig.Shared.MetricFiles) > 0 {
		workerConfig.MetricsList, err = metriclist.LoadMetricFile(mainConfig.Shared.MetricFiles,
			mainConfig.Shared.Min, mainConfig.Shared.Max, mainConfig.Shared.Incr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			os.Exit(2)
		}
	}
	if err = worker("localhost", &mainConfig.Local, &mainConfig.Shared, workerConfig); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(2)
	}
}

func globalFlags(cmd *cobra.Command, allConfig *LocalConfig) {
	cmd.Flags().BoolVarP(&allConfig.Verbose, "verbose", "v", false, "verbose")

	cmd.Flags().DurationVarP(&allConfig.AggrDuration, "aduration", "A", time.Minute, "aggregation duration")
	cmd.Flags().StringVarP(&allConfig.AggrFile, "aggr", "a", "", "sended metrics file (appended)")
	cmd.Flags().StringVarP(&allConfig.Graphite, "graphite", "g", "", "graphite relay address:port")
	cmd.Flags().StringVarP(&allConfig.GraphitePrefix, "gprefix", "G", "test.carbontest", "metric prefix for aggregated stat")

	cmd.Flags().StringVar(&allConfig.StatFile, "stat", "", "sended metrics stat file (appended)")
	cmd.Flags().StringVar(&allConfig.DetailFile, "detail", "", "sended metrics (with value/timestamp) file (appended)")

	cmd.Flags().StringVar(&allConfig.CPUProf, "cpuprofile", "", "write cpu profile to file")

	cmd.Flags().SortFlags = false
}

func localPostConfig(config *LocalConfig) error {
	if len(config.GraphitePrefix) > 0 {
		hostname, err := os.Hostname()
		if err != nil {
			return err
		}
		config.Hostname = strings.Split(hostname, ".")[0]
		config.GraphitePrefix = config.GraphitePrefix + ".carbontest." + config.Hostname
	}

	return nil
}

func standaloneFlags() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run in standalone mode",
		Run:   standaloneRun,
	}

	mainConfig.Workers = make(map[string]*WorkerConfig)

	globalFlags(cmd, &mainConfig.Local)

	cmd.Flags().SortFlags = false

	cmd.Flags().DurationVarP(&mainConfig.Shared.Duration, "duration", "d", 60*time.Second, "total test duration")

	mainConfig.Workers["localhost"] = &WorkerConfig{}

	// Test endpoint
	cmd.Flags().StringVarP(&mainConfig.Shared.T.Addr, "host", "r", "127.0.0.1", "address[:port] (default port: 2003)")

	// TCP settings
	cmd.Flags().IntVarP(&mainConfig.Shared.T.Workers, "workers", "w", 10, "TCP workers")
	cmd.Flags().IntVarP(&mainConfig.Shared.T.Metrics, "metrics", "m", 1, "metrics sended in one TCP connection")
	cmd.Flags().IntVarP(&mainConfig.Shared.T.BatchSend, "batch", "b", 1, "metrics count in one TCP send")
	cmd.Flags().DurationVarP(&mainConfig.Shared.T.ConTimeout, "con_timeout", "c", 100*time.Millisecond, "TCP connect timeout")
	cmd.Flags().DurationVarP(&mainConfig.Shared.T.SendTimeout, "send_timeout", "s", 500*time.Millisecond, "TCP send timeout")

	// UDP settings
	cmd.Flags().IntVarP(&mainConfig.Shared.T.UWorkers, "uworkers", "u", 0, "UDP workers (default 0)")
	cmd.Flags().IntVarP(&mainConfig.Shared.T.UBatchSend, "ubatch", "B", 1, "metrics count in one UDP send")

	// Metrics source
	// Random metrics
	cmd.Flags().StringVarP(&mainConfig.Shared.MetricPrefix, "prefix", "P", "test", "metric prefix (for autogenerated metrics)")
	// Metrics from file
	cmd.Flags().VarP(&mainConfig.Shared.MetricFiles, "file", "f", "metrics file (valid: plain text, gz) (format: Name [min[:max[:increment]]")
	// Min/Max/Incr
	cmd.Flags().Int32Var(&mainConfig.Shared.Min, "min", 0, "default min value for metrics file")
	cmd.Flags().Int32Var(&mainConfig.Shared.Max, "max", 0, "default max value for metrics file")
	cmd.Flags().Int32Var(&mainConfig.Shared.Incr, "incr", 0, "default incr value for metrics file (if 0 - value is random, also increase until max, than descrease to min)")

	cmd.Flags().VarP(&mainConfig.Shared.T.SendDelay, "delay", "D", "send delay random range (0s) (min[:max])")
	cmd.Flags().IntVar(&mainConfig.Shared.T.RateLimit, "rate", 0, "rate limit/s")

	cmd.Flags().StringVar(&mainConfig.Shared.T.Compress, "compress", "", "compress [ none | gzip | lz4 ]")

	return cmd
}

func main() {
	var rootCmd = &cobra.Command{
		Use:   os.Args[0],
		Short: "carbontest is a carbon load test tool",
	}

	var standaloneCmd = standaloneFlags()

	rootCmd.AddCommand(standaloneCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}
}
