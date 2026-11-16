package metrics

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"sync/atomic"
)

// Counter is a monotonically increasing count.
type Counter struct{ v atomic.Uint64 }

// Inc adds one.
func (c *Counter) Inc() { c.v.Add(1) }

// Add adds n.
func (c *Counter) Add(n uint64) { c.v.Add(n) }

func (c *Counter) typeName() string { return "counter" }
func (c *Counter) write(b *bytes.Buffer, name string) {
	writeSample(b, name, "", fmt.Sprint(c.v.Load()))
}

// Gauge is a value that can go up and down.
type Gauge struct{ v atomic.Int64 }

// Set replaces the value.
func (g *Gauge) Set(v int64) { g.v.Store(v) }

// Add changes the value by delta (negative to decrease).
func (g *Gauge) Add(delta int64) { g.v.Add(delta) }

func (g *Gauge) typeName() string { return "gauge" }
func (g *Gauge) write(b *bytes.Buffer, name string) {
	writeSample(b, name, "", fmt.Sprint(g.v.Load()))
}

// funcGauge reads its value when rendered.
type funcGauge struct{ fn func() int64 }

func (g funcGauge) typeName() string { return "gauge" }
func (g funcGauge) write(b *bytes.Buffer, name string) {
	writeSample(b, name, "", fmt.Sprint(g.fn()))
}

// Histogram counts observations into fixed buckets. Observe is lock-free.
// Readers may see a scrape that is a few observations out of step between
// buckets, sum and count; each value is itself exact.
type Histogram struct {
	bounds  []float64
	buckets []atomic.Uint64 // per-bucket (not cumulative); last slot is +Inf
	sumBits atomic.Uint64
}

func newHistogram(bounds []float64) *Histogram {
	if len(bounds) == 0 {
		panic("metrics: histogram needs at least one bucket")
	}
	for i, b := range bounds {
		if math.IsNaN(b) || math.IsInf(b, 0) {
			panic("metrics: histogram bounds must be finite")
		}
		if i > 0 && b <= bounds[i-1] {
			panic("metrics: histogram bounds must be strictly increasing")
		}
	}
	return &Histogram{
		bounds:  append([]float64(nil), bounds...),
		buckets: make([]atomic.Uint64, len(bounds)+1),
	}
}

// Observe records one value. A value equal to a bound falls in that bucket.
func (h *Histogram) Observe(v float64) {
	i := sort.SearchFloat64s(h.bounds, v) // first bound >= v
	h.buckets[i].Add(1)
	for {
		old := h.sumBits.Load()
		next := math.Float64bits(math.Float64frombits(old) + v)
		if h.sumBits.CompareAndSwap(old, next) {
			return
		}
	}
}

func (h *Histogram) typeName() string { return "histogram" }
func (h *Histogram) write(b *bytes.Buffer, name string) {
	var cum uint64
	for i, bound := range h.bounds {
		cum += h.buckets[i].Load()
		writeSample(b, name+"_bucket", labelPair("le", formatFloat(bound)), fmt.Sprint(cum))
	}
	cum += h.buckets[len(h.bounds)].Load()
	writeSample(b, name+"_bucket", labelPair("le", "+Inf"), fmt.Sprint(cum))
	writeSample(b, name+"_sum", "", formatFloat(math.Float64frombits(h.sumBits.Load())))
	// _count must equal the +Inf bucket within one scrape.
	writeSample(b, name+"_count", "", fmt.Sprint(cum))
}

// CounterVec is a set of counters over one label with a fixed value set.
type CounterVec struct {
	label    string
	values   []string
	counters map[string]*Counter // read-only after construction
}

// With returns the counter for a declared label value. An undeclared value is
// a programming error and panics, so request data can never create series.
func (v *CounterVec) With(value string) *Counter {
	c, ok := v.counters[value]
	if !ok {
		panic(fmt.Sprintf("metrics: undeclared value %q for label %q", value, v.label))
	}
	return c
}

func (v *CounterVec) typeName() string { return "counter" }
func (v *CounterVec) write(b *bytes.Buffer, name string) {
	for _, val := range v.values {
		writeSample(b, name, labelPair(v.label, val), fmt.Sprint(v.counters[val].v.Load()))
	}
}

// GaugeFuncVec is a gauge family over one label whose values are read at render time.
type GaugeFuncVec struct {
	label  string
	values []string
	fns    map[string]func() int64
}

func (v *GaugeFuncVec) typeName() string { return "gauge" }
func (v *GaugeFuncVec) write(b *bytes.Buffer, name string) {
	for _, val := range v.values {
		writeSample(b, name, labelPair(v.label, val), fmt.Sprint(v.fns[val]()))
	}
}

// NewCounter registers a counter. Counter names must end in _total.
func (r *Registry) NewCounter(name, help string) *Counter {
	checkCounterName(name)
	c := &Counter{}
	r.register(name, help, c)
	return c
}

// NewCounterVec registers a counter per declared value of label.
func (r *Registry) NewCounterVec(name, help, label string, values []string) *CounterVec {
	checkCounterName(name)
	checkLabelName(label)
	checkValues(values)
	v := &CounterVec{label: label, values: append([]string(nil), values...), counters: make(map[string]*Counter, len(values))}
	for _, val := range values {
		v.counters[val] = &Counter{}
	}
	r.register(name, help, v)
	return v
}

// NewGauge registers a settable gauge.
func (r *Registry) NewGauge(name, help string) *Gauge {
	g := &Gauge{}
	r.register(name, help, g)
	return g
}

// NewGaugeFunc registers a gauge whose value is read from fn at render time.
func (r *Registry) NewGaugeFunc(name, help string, fn func() int64) {
	if fn == nil {
		panic("metrics: nil gauge func")
	}
	r.register(name, help, funcGauge{fn: fn})
}

// NewGaugeFuncVec registers a gauge family keyed by label, each read from its own func.
func (r *Registry) NewGaugeFuncVec(name, help, label string, fns map[string]func() int64, order []string) *GaugeFuncVec {
	checkLabelName(label)
	checkValues(order)
	if len(fns) != len(order) {
		panic("metrics: gauge funcs and label order differ")
	}
	for _, val := range order {
		if fns[val] == nil {
			panic(fmt.Sprintf("metrics: missing gauge func for %q", val))
		}
	}
	v := &GaugeFuncVec{label: label, values: append([]string(nil), order...), fns: fns}
	r.register(name, help, v)
	return v
}

// NewHistogram registers a histogram with the given upper bounds (strictly
// increasing, finite). The +Inf bucket is implicit.
func (r *Registry) NewHistogram(name, help string, bounds []float64) *Histogram {
	h := newHistogram(bounds)
	r.register(name, help, h)
	return h
}

func checkCounterName(name string) {
	if len(name) < 6 || name[len(name)-6:] != "_total" {
		panic(fmt.Sprintf("metrics: counter %q must end in _total", name))
	}
}
