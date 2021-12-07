package main

import (
	"carbontest/pkg/base"
	"carbontest/pkg/metriclist"
	"fmt"
	"strings"
	"time"

	"go.uber.org/ratelimit"
)

type LocalConfig struct {
	Verbose bool

	AggrDuration time.Duration
	AggrFile     string // write aggregated connections stat to file

	Graphite       string // address for graphite relay (for send aggregated connections stat)
	GraphitePrefix string // prefix for graphite metric

	Hostname string

	StatFile   string // write connections stat to file
	DetailFile string // write sended metrics to file

	CPUProf string // write cpu profile info to file
}

type TargetConfig struct {
	Addr string
	//Connections int
	Workers   int // TCP Workers
	Metrics   int
	BatchSend int

	Compress string

	SendDelay base.RandomDuration

	RateLimit int

	UWorkers   int // UDP Workers
	UBatchSend int

	ConTimeout  time.Duration
	SendTimeout time.Duration
}

type WorkerConfig struct {
	T TargetConfig

	CompressType base.CompressType
	MetricsList  []metriclist.Metric // loaded metrics
	RateLimiter  ratelimit.Limiter
}

type SharedConfig struct {
	T TargetConfig

	Duration time.Duration

	MetricPrefix string // Prefix for generated metric name

	MetricFiles StringSlice

	Min  int32
	Max  int32
	Incr int32
}

type MainConfig struct {
	Local   LocalConfig              // Individual settings
	Shared  SharedConfig             // Shared config
	Workers map[string]*WorkerConfig // Settings per worker nodes
}

func mergeConfig(sharedСonfig *SharedConfig, workerConfig *WorkerConfig) {
	if workerConfig.T.Workers == 0 && workerConfig.T.UWorkers == 0 {
		workerConfig.T = sharedСonfig.T
	}
}

func validateWorkerConfig(name string, workerConfig *WorkerConfig) error {
	if len(workerConfig.T.Addr) == 0 {
		workerConfig.T.Addr = "127.0.0.1:2003"
	} else if !strings.Contains(workerConfig.T.Addr, ":") {
		workerConfig.T.Addr += ":2003"
	}
	if workerConfig.T.Workers < 0 {
		return fmt.Errorf("Invalid TCP workers '%s' value: %d", name, workerConfig.T.Workers)
	}
	if workerConfig.T.Metrics < 1 {
		return fmt.Errorf("Invalid metrics '%s' value: %d", name, workerConfig.T.Metrics)
	}
	if workerConfig.T.BatchSend < 1 {
		return fmt.Errorf("Invalid TCP metric batchsend '%s' value: %d\n", name, workerConfig.T.BatchSend)
	}
	if workerConfig.T.UWorkers < 0 {
		return fmt.Errorf("Invalid UDP workers '%s' value: %d", name, workerConfig.T.Workers)
	}

	if workerConfig.T.Workers < 0 && workerConfig.T.UWorkers < 0 {
		return fmt.Errorf("Set TCP or UDP workers for '%s'", name)
	}

	if workerConfig.T.RateLimit < 0 {
		return fmt.Errorf("Invalid rate limit '%s' value: %d", name, workerConfig.T.RateLimit)
	} else if workerConfig.T.RateLimit > 0 {
		if !workerConfig.T.SendDelay.IsZero() {
			return fmt.Errorf("delay and rate limit can't be used together for '%s'", name)
		}
		workerConfig.RateLimiter = ratelimit.New(workerConfig.T.RateLimit)
	} else {
		workerConfig.RateLimiter = nil
	}

	if workerConfig.T.SendTimeout < time.Microsecond {
		return fmt.Errorf("Invalid TCP send timeout '%s' value: %s", name, workerConfig.T.SendTimeout)
	}

	if workerConfig.T.ConTimeout < time.Microsecond {
		return fmt.Errorf("Invalid TCP connection timeout '%s' value: %s", name, workerConfig.T.ConTimeout)
	}

	switch strings.ToLower(workerConfig.T.Compress) {
	case "":
		workerConfig.CompressType = base.NONE
	case "gzip":
		workerConfig.CompressType = base.GZIP
	default:
		return fmt.Errorf("Invalid compress type '%s': %s", name, workerConfig.T.Compress)
	}

	return nil
}

func validateSharedConfig(sharedConfig *SharedConfig) error {
	if sharedConfig.Duration < time.Second {
		return fmt.Errorf("Invalid test duration: %s", sharedConfig.Duration)
	}
	return nil
}

func validateLocalConfig(localConfig *LocalConfig) error {
	if localConfig.AggrDuration < 10*time.Second {
		return fmt.Errorf("Invalid aggregation duration: %v", localConfig.AggrDuration)
	}
	return nil
}
