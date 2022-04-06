package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/msaf1980/go-stringutils"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

var (
	config        string
	templateFiles StringSlice
	metricFile    string
)

type Type int

const (
	TypeList Type = iota
	TypeListRandomInt
	TypeListRandomStr
)

var typeStrings []string = []string{"list", "list_rand_int", "list_rand_str"}

func (t *Type) Set(value string) error {
	switch value {
	case "list":
		*t = TypeList
	case "list_rand_int":
		*t = TypeListRandomInt
	case "list_rand_str":
		*t = TypeListRandomStr
	default:
		return fmt.Errorf("invalid type %s", value)
	}
	return nil
}

// UnmarshalYAML for use Aggregation in yaml files
func (t *Type) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var value string
	if err := unmarshal(&value); err != nil {
		return err
	}

	if err := t.Set(value); err != nil {
		return fmt.Errorf("failed to parse '%s' to Type: %v", value, err)
	}

	return nil
}

func (t *Type) String() string {
	return typeStrings[*t]
}

func (t *Type) Type() string {
	return "type"
}

var letterBytes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-")

func randInt(min, max int) int {
	if min == max {
		return min
	} else if min > max {
		min, max = max, min
	}
	return rand.Intn(max-min+1) + min
}

func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return stringutils.UnsafeString(b)
}

func randStringSlice(n, min, max int) []string {
	s := make([]string, n)
	for i := range s {
		length := randInt(min, max)
		s[i] = randString(length)
	}
	return s
}

func randIntSlice(n, min, max int) []string {
	s := make([]string, n)
	for i := range s {
		num := randInt(min, max)
		s[i] = strconv.Itoa(num)
	}
	return s
}

type ValueList []string

type ValueListRand struct {
	Min int `json:"min"`
	Max int `json:"max"`
}

var errValConfig = errors.New("invalid value config")

func newValueListRand(m interface{}) (*ValueListRand, error) {
	mv, ok := m.(map[interface{}]interface{})
	if !ok {
		return nil, errValConfig
	}
	r := &ValueListRand{}
	for k, v := range mv {
		if ks, ok := k.(string); ok {
			switch ks {
			case "min":
				if r.Min, ok = v.(int); !ok {
					return nil, fmt.Errorf("invalid value item %v for '%v'", v, k)
				}
			case "max":
				if r.Max, ok = v.(int); !ok {
					return nil, fmt.Errorf("invalid value item %v for key '%v'", v, k)
				}
			default:
				return nil, fmt.Errorf("unhandled value for '%v'", k)
			}
		} else {
			return nil, fmt.Errorf("invalid value key %v", k)
		}
	}
	return r, nil
}

type ValItem struct {
	Item    string
	SubItem map[string][]string
}

type SubItem struct {
	Type   Type                `yaml:"type"`
	Value  interface{}         `yaml:"value"`
	Sitems int                 `json:"sitems"`
	Sub    map[string]*SubItem `yaml:"sub"`
}

type RootItem struct {
	Sitems int                 `json:"sitems"`
	Sub    map[string]*SubItem `yaml:"sub"`
}

type valuesTemplate struct {
	n       int                 // values to expand
	sub     map[string][]string // map[KEY]value[N]
	subItem []valuesTemplate    // sub[N]
}

func listValues(values []interface{}) []string {
	sv := make([]string, len(values))
	for i, v := range values {
		sv[i] = v.(string)
	}

	return sv
}

func generateValuesTemplate(c map[string]*SubItem, sitems int, sv *valuesTemplate) error {
	var subs map[string]*SubItem
	sItems := 0
	for k, v := range c {
		if sitems == 0 && v.Type == TypeList {
			sitems = len(v.Value.([]interface{}))
		}
		if v.Sub != nil {
			if subs != nil {
				return fmt.Errorf("multiply subitems for key: %v (%v)", k, c)
			} else {
				subs = v.Sub
				sItems = v.Sitems
			}
		}
	}
	if sitems < 1 {
		return fmt.Errorf("invalid subitems: %d (%v)", sitems, c)
	}

	sv.sub = make(map[string][]string)
	for k, v := range c {
		key := "{{" + k + "}}"
		switch v.Type {
		case TypeList:
			sv.sub[key] = listValues(v.Value.([]interface{}))
		case TypeListRandomStr:
			if value, err := newValueListRand(v.Value); err == nil {
				sv.sub[key] = randStringSlice(sitems, value.Min, value.Max)
			} else {
				return fmt.Errorf("%s for key %s: '%v' (%v)", err.Error(), k, v.Value, c)
			}
		case TypeListRandomInt:
			if value, err := newValueListRand(v.Value); err == nil {
				sv.sub[key] = randIntSlice(sitems, value.Min, value.Max)
			} else {
				return fmt.Errorf("%s for key %s: '%v' (%v)", err.Error(), k, v.Value, c)
			}
		default:
			return fmt.Errorf("unhandled type: %s", v.Type.String())
		}
		if sitems != len(sv.sub[key]) {
			return fmt.Errorf("inconsistent subitem length for key %s: %d, want %d (%v)", k, len(sv.sub[key]), sitems, c)
		}
	}

	sv.n = sitems
	if subs != nil {
		sv.subItem = make([]valuesTemplate, sitems)
		for i := 0; i < sitems; i++ {
			if err := generateValuesTemplate(subs, sItems, &sv.subItem[i]); err != nil {
				return err
			}
		}
	}

	return nil
}

func writeValue(w BufferedWriter, template string, sv *valuesTemplate) error {
	if !strings.Contains(template, "{{") {
		io.WriteString(w, template)
		if !strings.HasSuffix(template, "\n") {
			io.WriteString(w, "\n")
		}
		return nil // end of template expand
	}
	for i := 0; i < sv.n; i++ {
		var (
			changed bool
			ch      bool
		)
		if atomic.LoadInt32(&running) == 0 {
			break
		}

		t := template
		for k, v := range sv.sub {
			t, ch = stringutils.ReplaceAll(t, k, v[i])
			if ch && !changed {
				changed = true
			}
		}

		if strings.Contains(t, "{{") {
			if len(sv.subItem) == 0 {
				return fmt.Errorf("template not expanded: %s", t)
			}
			if err := writeValue(w, t, &sv.subItem[i]); err != nil {
				return err
			}
		} else {
			io.WriteString(w, t)
			if !strings.HasSuffix(t, "\n") {
				io.WriteString(w, "\n")
			}
		}
	}

	return nil
}

var reStart = regexp.MustCompile(`{{ +`)
var reEnd = regexp.MustCompile(` +}}`)

func writeValues(w BufferedWriter, templates []string, sv *valuesTemplate) []error {
	var errors []error
	for _, t := range templates {
		if atomic.LoadInt32(&running) == 0 {
			break
		}
		b := reStart.ReplaceAll([]byte(t), []byte("{{"))
		b = reEnd.ReplaceAll(b, []byte("}}"))
		if err := writeValue(w, string(b), sv); err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}

func loadTemplatesFile(filename string) ([]string, error) {
	templates := make([]string, 0, 1024)

	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var reader *bufio.Reader

	if strings.HasSuffix(filename, ".gz") {
		gzipReader, err := gzip.NewReader(f)
		if err != nil {
			return nil, err
		}
		reader = bufio.NewReader(gzipReader)
	} else {
		reader = bufio.NewReader(f)
	}

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		templates = append(templates, string(line))
	}

	return templates, nil
}

func generateRun(cmd *cobra.Command, args []string) {
	if len(args) > 0 {
		fmt.Fprintf(os.Stderr, "unhandled args: %v\n", args)
		cmd.Help()
		os.Exit(1)
	}
	f, err := os.Open(config)
	if err != nil {
		log.Fatal(err)
	}

	body, err := ioutil.ReadAll(f)
	f.Close()
	if err != nil {
		log.Fatal(err)
	}

	var sv valuesTemplate
	var c RootItem

	err = yaml.Unmarshal(body, &c)
	if err != nil {
		log.Fatal(err)
	}

	if err = generateValuesTemplate(c.Sub, c.Sitems, &sv); err != nil {
		log.Fatal(err)
	}

	var templates []string
	for _, templateFile := range templateFiles {
		t, err := loadTemplatesFile(templateFile)
		if err != nil {
			log.Fatal(err)
		}
		templates = append(templates, t...)
	}

	f, err = os.OpenFile(metricFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var w BufferedWriter
	if strings.HasSuffix(metricFile, ".gz") {
		w = gzip.NewWriter(f)
	} else {
		w = bufio.NewWriter(f)
	}

	errs := writeValues(w, templates, &sv)

	if closer, ok := w.(io.Closer); ok {
		err = closer.Close()
	} else {
		err = w.Flush()
	}
	if err != nil {
		log.Fatal(err)
	}

	if len(errs) > 0 {
		for _, err := range errs {
			log.Printf("ERROR: %v", err)
		}
		os.Exit(1)
	}
}

func generateFlags(rootCmd *cobra.Command) {
	cmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate metrics file",
		Run:   generateRun,
	}

	cmd.Flags().SortFlags = false

	cmd.Flags().StringVarP(&config, "config", "c", "", "YAML config")
	cmd.MarkFlagRequired("config")

	cmd.Flags().VarP(&templateFiles, "template", "t", "template files for metric file, replacement string like {{ STRING }}")
	cmd.MarkFlagRequired("template")

	cmd.Flags().StringVarP(&metricFile, "file", "f", "", "generated metrics file (valid: txt, gz)")
	cmd.MarkFlagRequired("file")

	rootCmd.AddCommand(cmd)
}
