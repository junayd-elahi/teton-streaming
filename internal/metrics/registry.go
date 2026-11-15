// Package metrics is a small lock-free metrics registry that renders the
// Prometheus text exposition format (version 0.0.4). Updates use atomics only,
// so hot paths can call it from many goroutines without contention on a mutex.
package metrics

import (
	"bytes"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

const contentType = "text/plain; version=0.0.4; charset=utf-8"

var (
	metricNameRE = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
	labelNameRE  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

// collector writes the samples of one metric family.
type collector interface {
	typeName() string
	write(w *bytes.Buffer, name string)
}

type family struct {
	name string
	help string
	c    collector
}

// Registry holds metric families in registration order.
type Registry struct {
	mu       sync.Mutex // guards registration only, never the update path
	families []family
	names    map[string]bool
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{names: make(map[string]bool)}
}

func (r *Registry) register(name, help string, c collector) {
	if !metricNameRE.MatchString(name) {
		panic(fmt.Sprintf("metrics: invalid metric name %q", name))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.names[name] {
		panic(fmt.Sprintf("metrics: duplicate metric name %q", name))
	}
	r.names[name] = true
	r.families = append(r.families, family{name: name, help: help, c: c})
}

// Render returns the exposition text for every registered family.
func (r *Registry) Render() []byte {
	r.mu.Lock()
	fams := append([]family(nil), r.families...)
	r.mu.Unlock()

	var b bytes.Buffer
	for _, f := range fams {
		b.WriteString("# HELP " + f.name + " " + escapeHelp(f.help) + "\n")
		b.WriteString("# TYPE " + f.name + " " + f.c.typeName() + "\n")
		f.c.write(&b, f.name)
	}
	return b.Bytes()
}

// Handler serves the exposition text.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write(r.Render())
	})
}

var (
	helpEscaper  = strings.NewReplacer(`\`, `\\`, "\n", `\n`)
	labelEscaper = strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`)
)

func escapeHelp(s string) string       { return helpEscaper.Replace(s) }
func escapeLabelValue(s string) string { return labelEscaper.Replace(s) }

// formatFloat renders a sample value the way the exposition format expects.
func formatFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func writeSample(b *bytes.Buffer, name, labels, value string) {
	b.WriteString(name)
	b.WriteString(labels)
	b.WriteByte(' ')
	b.WriteString(value)
	b.WriteByte('\n')
}

// labelPair renders {key="value"} with the value escaped.
func labelPair(key, value string) string {
	return `{` + key + `="` + escapeLabelValue(value) + `"}`
}

func checkLabelName(name string) {
	if !labelNameRE.MatchString(name) || strings.HasPrefix(name, "__") {
		panic(fmt.Sprintf("metrics: invalid label name %q", name))
	}
}

func checkValues(values []string) {
	if len(values) == 0 {
		panic("metrics: a labelled metric needs at least one label value")
	}
	seen := make(map[string]bool, len(values))
	for _, v := range values {
		if seen[v] {
			panic(fmt.Sprintf("metrics: duplicate label value %q", v))
		}
		seen[v] = true
	}
}
