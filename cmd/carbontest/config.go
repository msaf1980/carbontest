package main

import (
	"carbontest/pkg/base"
	"carbontest/pkg/metriclist"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.uber.org/ratelimit"
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

type LocalConfig struct {
	Verbose bool `json:"verbose"`

	AggrDuration time.Duration `json:"aggr-duration",omitempty`
	AggrFile     string        `json:"aggr-file",omitempty` // write aggregated connections stat to file

	Graphite       string `json:"graphite"`        // address for graphite relay (for send aggregated connections stat)
	GraphitePrefix string `json:"graphite-prefix"` // prefix for graphite metric

	StatFile   string `json:"stat-file",omitempty`   // write connections stat to file
	DetailFile string `json:"detail-file",omitempty` // write sended metrics to file

	CPUProf string `json:"cpu-prof",omitempty` // write cpu profile info to file

	// distributed
	APIKey  string `json:"api-key",omitempty`
	Address string `json:"address",omitempty` // address for distributed node managment listener

	MetricFiles StringSlice         `json:"metric-files",omitempty`
	MetricsList []metriclist.Metric `json:"-"` // loaded metrics

	Hostname string `json:"-"`
}

type TCPConfig struct {
	Workers   int `json:"workers"` // TCP Workers
	Metrics   int `json:"metrics"`
	BatchSend int `json:"batch-send"`
}

type UDPConfig struct {
	Workers   int `json:"workers"` // UDP Workers
	Metrics   int `json:"metrics"`
	BatchSend int `json:"batch-send"`
}

type TargetConfig struct {
	Address string `json:"address",omitempty`

	Compress string `json:"compress",omitempty`

	SendDelay base.RandomDuration `json:"send-delay"`

	RateLimit int `json:"rate"`

	TCP TCPConfig `json:"tcp"`
	UDP UDPConfig `json:"udp"`

	ConTimeout  time.Duration `json:"con-timeout"`
	SendTimeout time.Duration `json:"send-timeout"`
}

type WorkerConfig struct {
	T TargetConfig

	CompressType base.CompressType
	RateLimiter  ratelimit.Limiter
}

type SharedConfig struct {
	T TargetConfig `json:"-"`

	Duration time.Duration `json:"duration"`

	MetricPrefix string `json:"metric-prefix",omitempty` // Prefix for generated metric name

	Min  int32 `json:"min"`
	Max  int32 `json:"max"`
	Incr int32 `json:"incr"`
}

type MainConfig struct {
	Local   LocalConfig              // Individual settings
	Shared  SharedConfig             // Shared config
	Workers map[string]*WorkerConfig // Settings per worker nodes
}

func validateWorkerConfig(name string, workerConfig *WorkerConfig) error {
	if len(workerConfig.T.Address) == 0 {
		workerConfig.T.Address = "127.0.0.1:2003"
	} else if !strings.Contains(workerConfig.T.Address, ":") {
		workerConfig.T.Address += ":2003"
	}

	if workerConfig.T.TCP.Workers < 0 {
		return fmt.Errorf("Invalid TCP workers '%s' value: %d", name, workerConfig.T.TCP.Workers)
	}
	if workerConfig.T.TCP.Metrics < 1 {
		workerConfig.T.TCP.Metrics = 1
	}
	if workerConfig.T.TCP.BatchSend < 1 {
		workerConfig.T.TCP.BatchSend = 1
	}

	if workerConfig.T.UDP.Workers < 0 {
		return fmt.Errorf("Invalid UDP workers '%s' value: %d", name, workerConfig.T.UDP.Workers)
	}
	if workerConfig.T.UDP.BatchSend < 1 {
		workerConfig.T.UDP.BatchSend = 1
	}
	if workerConfig.T.UDP.Metrics < 1 {
		workerConfig.T.UDP.Metrics = workerConfig.T.UDP.BatchSend * 1000
	}
	if workerConfig.T.TCP.Workers < 1 && workerConfig.T.UDP.Workers < 1 {
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
	case "", "none":
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

func mergeConfig(sharedСonfig *SharedConfig, workerConfig *WorkerConfig) {
	if workerConfig.T.TCP.Workers == 0 && workerConfig.T.UDP.Workers == 0 {
		workerConfig.T.TCP = sharedСonfig.T.TCP

		workerConfig.T.UDP = sharedСonfig.T.UDP
	}
	if len(workerConfig.T.Address) == 0 {
		workerConfig.T.Address = sharedСonfig.T.Address
	}
	if len(workerConfig.T.Compress) == 0 {
		workerConfig.T.Compress = sharedСonfig.T.Compress
	}

	if workerConfig.T.SendDelay.IsZero() {
		workerConfig.T.SendDelay = sharedСonfig.T.SendDelay
	}

	if workerConfig.T.RateLimit == 0 {
		workerConfig.T.RateLimit = sharedСonfig.T.RateLimit
	}

	if workerConfig.T.ConTimeout == 0 {
		workerConfig.T.ConTimeout = sharedСonfig.T.ConTimeout
	}

	if workerConfig.T.SendTimeout == 0 {
		workerConfig.T.SendTimeout = sharedСonfig.T.SendTimeout
	}
}
