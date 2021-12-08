package main

import (
	"bufio"
	"carbontest/pkg/base"
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"path"
	"strings"

	"github.com/msaf1980/go-stringutils"
	"github.com/spf13/cobra"
	"github.com/zentures/cityhash"
)

type SplitPolicy int8

const (
	SplitRoundRobin SplitPolicy = iota
	SplitCityHash64
)

var splitPolicyStrings []string = []string{"round-robin", "cityhash64"}

func (p *SplitPolicy) Set(value string) error {
	switch strings.ToLower(value) {
	case "round-robin":
		*p = SplitRoundRobin
	case "cityhash64":
		*p = SplitCityHash64
	default:
		return fmt.Errorf("invalid split policy %s", value)
	}
	return nil
}

func (p *SplitPolicy) String() string {
	return splitPolicyStrings[*p]
}

func (p *SplitPolicy) Type() string {
	return "SplitPolicy"
}

func splitFile(fileName, suffix string, nodes int, policy SplitPolicy, overwrite bool) error {
	dir, name := path.Split(fileName)
	ext := path.Ext(name)
	name = name[0 : len(name)-len(ext)]

	fileNames := make([]string, nodes)
	files := make([]*os.File, nodes)
	writers := make([]BufferedWriter, nodes)

	var (
		file   *os.File
		reader *bufio.Reader
		err    error
	)

	defer func() {
		for i := 0; i < nodes; i++ {
			if files[i] != nil {
				files[i].Close()
			}
		}
		if file != nil {
			file.Close()
		}
	}()

	var compressType base.CompressType

	file, err = os.Open(fileName)
	if err != nil {
		return err
	}
	switch ext {
	case ".gz":
		compressType = base.GZIP
		gzipReader, err := gzip.NewReader(file)
		if err != nil {
			return err
		}
		reader = bufio.NewReader(gzipReader)
	case ".txt":
		compressType = base.NONE
		reader = bufio.NewReader(file)
	default:
		return fmt.Errorf("unknown file format: %s", fileName)
	}

	for i := 0; i < nodes; i++ {
		if len(suffix) == 0 {
			fileNames[i] = path.Join(dir, fmt.Sprintf("%s.%d%s", name, i, ext))
		} else {
			fileNames[i] = path.Join(dir, fmt.Sprintf("%s.%s_%d%s", name, suffix, i, ext))
		}
		if !overwrite {
			files[i], err = os.Open(fileNames[i])
			if err == nil {
				return fmt.Errorf("%s already exist", fileNames[i])
			}
		}
		if files[i], err = os.OpenFile(fileNames[i], os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660); err != nil {
			return err
		}
		switch compressType {
		case base.GZIP:
			writers[i] = gzip.NewWriter(files[i])
		default:
			writers[i] = bufio.NewWriter(files[i])
		}
	}

	n := nodes
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		switch policy {
		case SplitCityHash64:
			n = int(cityhash.CityHash64(stringutils.UnsafeStringBytes(&line), uint32(len(line))) % uint64(nodes))
		default: // round robin
			n = (n + 1) % nodes
		}

		if _, err = writers[n].Write(stringutils.UnsafeStringBytes(&line)); err != nil {
			return fmt.Errorf("%v for %s", err, fileNames[n])
		}
	}

	for i := 0; i < nodes; i++ {
		if err = writers[i].Flush(); err != nil {
			return fmt.Errorf("%v for %s", err, fileNames[n])
		}
		log.Printf("%s written", fileNames[n])
	}

	return nil
}

type SplitConfig struct {
	MetricFiles StringSlice
	Suffix      string
	Nodes       int
	Policy      SplitPolicy
	Overwrite   bool
}

var splitConfig SplitConfig

func splitRun(cmd *cobra.Command, args []string) {
	if len(args) > 0 {
		fmt.Fprintf(os.Stderr, "unhandled args: %v\n", args)
		cmd.Help()
		os.Exit(1)
	}
	if splitConfig.Nodes == 0 || len(splitConfig.MetricFiles) == 0 {
		return
	}
	for _, file := range splitConfig.MetricFiles {
		if err := splitFile(file, splitConfig.Suffix, splitConfig.Nodes, splitConfig.Policy, splitConfig.Overwrite); err != nil {
			log.Fatal(err)
		}
	}
}

func splitFlags(rootCmd *cobra.Command) {
	cmd := &cobra.Command{
		Use:   "split",
		Short: "Split metrics files with distribution policy",
		Run:   splitRun,
	}

	cmd.Flags().SortFlags = false

	// Metrics from file
	cmd.Flags().VarP(&splitConfig.MetricFiles, "file", "f", "metrics file (valid: txt, gz) (format: Name [min[:max[:increment]]")
	cmd.MarkFlagRequired("file")

	cmd.Flags().StringVarP(&splitConfig.Suffix, "suffix", "S", "", "Append suffix to filename")
	cmd.Flags().IntVarP(&splitConfig.Nodes, "nodes", "n", 0, "Split files to N nodes")
	cmd.Flags().VarP(&splitConfig.Policy, "policy", "p", "Split policy [round-robin cityhash64]")
	cmd.Flags().BoolVarP(&splitConfig.Overwrite, "overwrite", "w", false, "Overwrite existing files")

	rootCmd.AddCommand(cmd)
}
