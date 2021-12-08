package main

import (
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/msaf1980/go-stringutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func blankLine(n int) string {
	if n <= 0 {
		return ""
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = ' '
	}
	return stringutils.UnsafeString(b)
}

func diffValues(sb *stringutils.Builder, want *valuesTemplate, result *valuesTemplate, n int) {
	sb.WriteString(blankLine(n))
	sb.WriteString("{\n")

	if want.n != result.n {
		sb.WriteString(blankLine(n + 4))
		sb.WriteString("- n = ")
		sb.WriteInt(int64(want.n), 10)
		sb.WriteString(",\n")

		sb.WriteString(blankLine(n + 4))
		sb.WriteString("+ n = ")
		sb.WriteInt(int64(result.n), 10)
		sb.WriteString(",\n")
	}

	sb.WriteString(blankLine(n + 2))
	sb.WriteString("sub = {\n")
	for key, rv := range result.sub {
		sb.WriteString(blankLine(n + 4))
		sb.WriteString(`"`)
		sb.WriteString(key)
		sb.WriteString("\" = {\n")
		if wv, ok := want.sub[key]; ok {
			if len(wv) == len(rv) {
				if reflect.DeepEqual(wv, rv) {
					sb.WriteString(blankLine(n + 6))
					sb.WriteString("..\n")
				} else {
					fmt.Fprintf(sb, "%s- %+v\n", blankLine(n+5), wv)
					fmt.Fprintf(sb, "%s+ %+v\n", blankLine(n+5), rv)
				}
			} else {
				fmt.Fprintf(sb, "%s- %+v\n", blankLine(n+5), wv)
				fmt.Fprintf(sb, "%s+ %+v\n", blankLine(n+5), rv)
			}
		} else {
			fmt.Fprintf(sb, "%s+ %+v\n", blankLine(n+5), rv)
		}
		sb.WriteString(blankLine(n + 4))
		sb.WriteString("}\n")
	}
	for key, wv := range want.sub {
		if _, ok := result.sub[key]; !ok {
			sb.WriteString(blankLine(n + 4))
			sb.WriteString(`"`)
			sb.WriteString(key)
			sb.WriteString("\" = {\n")
			fmt.Fprintf(sb, "%s- %+v\n", blankLine(n+5), wv)
			sb.WriteString(blankLine(n + 4))
			sb.WriteString("}\n")
		}
	}
	sb.WriteString(blankLine(n + 2))
	sb.WriteString("},\n")

	length := len(want.subItem)
	if length < len(result.subItem) {
		length = len(result.subItem)
	}
	if length > 0 {
		sb.WriteString(blankLine(n + 2))
		sb.WriteString("subItem = {\n")
		for i := 0; i < length; i++ {
			if i >= len(want.subItem) {
				diffValues(sb, &valuesTemplate{}, &result.subItem[i], n+4)
			} else if i >= len(result.subItem) {
				diffValues(sb, &want.subItem[i], &valuesTemplate{}, n+4)
			} else {
				diffValues(sb, &want.subItem[i], &result.subItem[i], n+4)
			}
		}
		sb.WriteString(blankLine(n + 2))
		sb.WriteString("},\n")
	}

	sb.WriteString(blankLine(n))
	sb.WriteString("},\n")
}

func Test_generateValuesRand(t *testing.T) {
	body := []byte(`
sub:
  "HOST":
    type: "list"
    value: [ "test1", "test2" ]
    sitems: 10
    sub:
      "PID":
        type: "list_rand_int"
        value:
          min: 1
          max: 65535
      "PROCESS":
        type: "list_rand_str"
        value:
          min: 10
          max: 20
`)

	var sv valuesTemplate
	var c RootItem

	err := yaml.Unmarshal(body, &c)
	if err != nil {
		log.Fatal(err)
	}

	err = generateValuesTemplate(c.Sub, c.Sitems, &sv)
	require.NoError(t, err)

	assert.Equal(t, 2, sv.n)
	assert.Equal(t, sv.sub, map[string][]string{"{{HOST}}": {"test1", "test2"}})

	assert.Equal(t, 2, len(sv.subItem))

	for _, subItem := range sv.subItem {
		assert.Equal(t, 10, subItem.n)

		assert.Equal(t, subItem.n, len(subItem.sub["{{PID}}"]))
		for i, s := range subItem.sub["{{PID}}"] {
			n, err := strconv.Atoi(s)
			if err == nil {
				assert.Truef(t, n >= 1 && n <= 65535, "PID[%d] = %d", i, n)
			}
			assert.NoErrorf(t, err, "PID[%d]", i)
		}

		assert.Equal(t, subItem.n, len(subItem.sub["{{PROCESS}}"]))
		for i, s := range subItem.sub["{{PROCESS}}"] {
			assert.Truef(t, len(s) >= 10 && len(s) <= 20, "PROCESS[%d] = %s", i, s)
		}
	}
}

func Test_generateValuesTemplate(t *testing.T) {
	tests := []struct {
		name               string
		templates          []string
		body               []byte
		wantValuesTemplate valuesTemplate
		wantValues         []string
		wantErr            bool
	}{
		{
			name: "list",
			templates: []string{
				"{{ ENV }}.{{ TEAM }}.{{ HOST }}.cpu.user",
				"{{ ENV }}.{{ TEAM }}.{{ HOST }}.{{PROCESS}}.{{ PID }}.cpu.user",
			},
			body: []byte(`
sub:
  "ENV":
    type: "list"
    value: [ "cloud", "production", "dev" ]
    sub:
      "TEAM":
        type: "list"
        value: [ "SalesLT", "BackLT" ]
        sub:
          "HOST":
            type: "list"
            value: [ "test1", "test2" ]
            sub:
              "PID":
                type: "list"
                value: [ "1", "200" ]
              "PROCESS":
                type: "list"
                value: [ "init", "bash" ]
`),
			wantValuesTemplate: valuesTemplate{
				n: 3,
				sub: map[string][]string{
					"{{ENV}}": {"cloud", "production", "dev"},
				},
				subItem: []valuesTemplate{
					{
						n: 2,
						sub: map[string][]string{
							"{{TEAM}}": {"SalesLT", "BackLT"},
						},
						subItem: []valuesTemplate{
							{
								n: 2,
								sub: map[string][]string{
									"{{HOST}}": {"test1", "test2"},
								},
								subItem: []valuesTemplate{
									{
										n: 2,
										sub: map[string][]string{
											"{{PID}}":     {"1", "200"},
											"{{PROCESS}}": {"init", "bash"},
										},
									},
									{
										n: 2,
										sub: map[string][]string{
											"{{PID}}":     {"1", "200"},
											"{{PROCESS}}": {"init", "bash"},
										},
									},
								},
							},
							{
								n: 2,
								sub: map[string][]string{
									"{{HOST}}": {"test1", "test2"},
								},
								subItem: []valuesTemplate{
									{
										n: 2,
										sub: map[string][]string{
											"{{PID}}":     {"1", "200"},
											"{{PROCESS}}": {"init", "bash"},
										},
									},
									{
										n: 2,
										sub: map[string][]string{
											"{{PID}}":     {"1", "200"},
											"{{PROCESS}}": {"init", "bash"},
										},
									},
								},
							},
						},
					},

					{
						n: 2,
						sub: map[string][]string{
							"{{TEAM}}": {"SalesLT", "BackLT"},
						},
						subItem: []valuesTemplate{
							{
								n: 2,
								sub: map[string][]string{
									"{{HOST}}": {"test1", "test2"},
								},
								subItem: []valuesTemplate{
									{
										n: 2,
										sub: map[string][]string{
											"{{PID}}":     {"1", "200"},
											"{{PROCESS}}": {"init", "bash"},
										},
									},
									{
										n: 2,
										sub: map[string][]string{
											"{{PID}}":     {"1", "200"},
											"{{PROCESS}}": {"init", "bash"},
										},
									},
								},
							},
							{
								n: 2,
								sub: map[string][]string{
									"{{HOST}}": {"test1", "test2"},
								},
								subItem: []valuesTemplate{
									{
										n: 2,
										sub: map[string][]string{
											"{{PID}}":     {"1", "200"},
											"{{PROCESS}}": {"init", "bash"},
										},
									},
									{
										n: 2,
										sub: map[string][]string{
											"{{PID}}":     {"1", "200"},
											"{{PROCESS}}": {"init", "bash"},
										},
									},
								},
							},
						},
					},

					{
						n: 2,
						sub: map[string][]string{
							"{{TEAM}}": {"SalesLT", "BackLT"},
						},
						subItem: []valuesTemplate{
							{
								n: 2,
								sub: map[string][]string{
									"{{HOST}}": {"test1", "test2"},
								},
								subItem: []valuesTemplate{
									{
										n: 2,
										sub: map[string][]string{
											"{{PID}}":     {"1", "200"},
											"{{PROCESS}}": {"init", "bash"},
										},
									},
									{
										n: 2,
										sub: map[string][]string{
											"{{PID}}":     {"1", "200"},
											"{{PROCESS}}": {"init", "bash"},
										},
									},
								},
							},
							{
								n: 2,
								sub: map[string][]string{
									"{{HOST}}": {"test1", "test2"},
								},
								subItem: []valuesTemplate{
									{
										n: 2,
										sub: map[string][]string{
											"{{PID}}":     {"1", "200"},
											"{{PROCESS}}": {"init", "bash"},
										},
									},
									{
										n: 2,
										sub: map[string][]string{
											"{{PID}}":     {"1", "200"},
											"{{PROCESS}}": {"init", "bash"},
										},
									},
								},
							},
						},
					},
				},
			},
			wantValues: []string{
				"cloud.SalesLT.test1.cpu.user",
				"cloud.SalesLT.test2.cpu.user",
				"cloud.BackLT.test1.cpu.user",
				"cloud.BackLT.test2.cpu.user",
				"production.SalesLT.test1.cpu.user",
				"production.SalesLT.test2.cpu.user",
				"production.BackLT.test1.cpu.user",
				"production.BackLT.test2.cpu.user",
				"dev.SalesLT.test1.cpu.user",
				"dev.SalesLT.test2.cpu.user",
				"dev.BackLT.test1.cpu.user",
				"dev.BackLT.test2.cpu.user",

				"cloud.SalesLT.test1.init.1.cpu.user",
				"cloud.SalesLT.test1.bash.200.cpu.user",

				"cloud.SalesLT.test2.init.1.cpu.user",
				"cloud.SalesLT.test2.bash.200.cpu.user",

				"cloud.BackLT.test1.init.1.cpu.user",
				"cloud.BackLT.test1.bash.200.cpu.user",

				"cloud.BackLT.test2.init.1.cpu.user",
				"cloud.BackLT.test2.bash.200.cpu.user",

				"production.SalesLT.test1.init.1.cpu.user",
				"production.SalesLT.test1.bash.200.cpu.user",

				"production.SalesLT.test2.init.1.cpu.user",
				"production.SalesLT.test2.bash.200.cpu.user",

				"production.BackLT.test1.init.1.cpu.user",
				"production.BackLT.test1.bash.200.cpu.user",

				"production.BackLT.test2.init.1.cpu.user",
				"production.BackLT.test2.bash.200.cpu.user",

				"dev.SalesLT.test1.init.1.cpu.user",
				"dev.SalesLT.test1.bash.200.cpu.user",

				"dev.SalesLT.test2.init.1.cpu.user",
				"dev.SalesLT.test2.bash.200.cpu.user",

				"dev.BackLT.test1.init.1.cpu.user",
				"dev.BackLT.test1.bash.200.cpu.user",

				"dev.BackLT.test2.init.1.cpu.user",
				"dev.BackLT.test2.bash.200.cpu.user",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sv valuesTemplate
			var c RootItem

			err := yaml.Unmarshal(tt.body, &c)
			if err != nil {
				log.Fatal(err)
			}

			if err = generateValuesTemplate(c.Sub, c.Sitems, &sv); (err != nil) != tt.wantErr {
				t.Errorf("generateValues() error = %v, wantErr %v", err, tt.wantErr)
			} else if err == nil && !reflect.DeepEqual(tt.wantValuesTemplate, sv) {
				var sb stringutils.Builder
				diffValues(&sb, &tt.wantValuesTemplate, &sv, 0)
				t.Errorf("generateValuesTemplate() error = \n%+v\nwant\n%+v\ndiff\n%s", sv, tt.wantValuesTemplate, sb.String())
			} else {
				var sb stringutils.Builder
				errs := writeValues(&sb, tt.templates, &sv)
				for _, err := range errs {
					assert.NoError(t, err)
				}
				if len(errs) == 0 {
					values := strings.Split(strings.TrimRight(sb.String(), "\n"), "\n")
					assert.Equal(t, tt.wantValues, values)
				}
			}
		})
	}
}

func Test_generateValuesTemplateError(t *testing.T) {
	tests := []struct {
		name       string
		body       []byte
		wantErrSub string
	}{
		{
			name: "list",
			body: []byte(`
sub:
  "ENV":
    type: "list"
    value: [ "cloud", "production", "dev" ]
    sub:
        "PID":
            type: "list"
            value: [ "1", "200" ]
        "PROCESS":
            type: "list"
            value: [ "init", "bash", "fake" ]
`),
			wantErrSub: "inconsistent subitem length for key ",
		},
		{
			name: "list",
			body: []byte(`
sub:
  "ENV":
    type: "list"
    value: [ "cloud", "production", "dev" ]
    subitems: 3
    sub:
        "PID":
            type: "list"
            value: [ "1", "200" ]
        "PROCESS":
            type: "list"
            value: [ "init", "bash", "fake" ]
`),
			wantErrSub: "inconsistent subitem length for key ",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sv valuesTemplate
			var c RootItem

			err := yaml.Unmarshal(tt.body, &c)
			if err != nil {
				log.Fatal(err)
			}

			if err = generateValuesTemplate(c.Sub, c.Sitems, &sv); err != nil && !strings.Contains(err.Error(), tt.wantErrSub) {
				t.Errorf("generateValues() error =\n'%v'\nwant substring\n'%s'", err, tt.wantErrSub)
			} else if err == nil {
				t.Errorf("generateValues() must be errored")
			}
		})
	}
}
