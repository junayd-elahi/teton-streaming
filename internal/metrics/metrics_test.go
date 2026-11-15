package metrics

import (
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func mustPanic(t *testing.T, name string, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s: expected panic", name)
		}
	}()
	f()
}

func TestRenderExact(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("req_total", "Requests.\nSecond \\ line.")
	c.Inc()
	c.Add(4)
	g := r.NewGauge("inflight", "In flight.")
	g.Set(7)
	g.Add(-2)
	r.NewGaugeFunc("depth", "Depth.", func() int64 { return 42 })
	v := r.NewCounterVec("ev_total", "By kind.", "kind", []string{"a", `b"c`})
	v.With("a").Add(2)
	v.With(`b"c`).Inc()
	h := r.NewHistogram("dur_seconds", "Duration.", []float64{0.1, 1})
	h.Observe(0.05)
	h.Observe(0.1)
	h.Observe(0.5)
	h.Observe(3)

	want := `# HELP req_total Requests.\nSecond \\ line.
# TYPE req_total counter
req_total 5
# HELP inflight In flight.
# TYPE inflight gauge
inflight 5
# HELP depth Depth.
# TYPE depth gauge
depth 42
# HELP ev_total By kind.
# TYPE ev_total counter
ev_total{kind="a"} 2
ev_total{kind="b\"c"} 1
# HELP dur_seconds Duration.
# TYPE dur_seconds histogram
dur_seconds_bucket{le="0.1"} 2
dur_seconds_bucket{le="1"} 3
dur_seconds_bucket{le="+Inf"} 4
dur_seconds_sum 3.65
dur_seconds_count 4
`
	if got := string(r.Render()); got != want {
		t.Fatalf("render mismatch\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestHistogramBoundaries(t *testing.T) {
	h := newHistogram([]float64{1, 2})
	for _, v := range []float64{0, 1, 1.0000001, 2, 2.5} {
		h.Observe(v)
	}
	got := []uint64{h.buckets[0].Load(), h.buckets[1].Load(), h.buckets[2].Load()}
	want := []uint64{2, 2, 1}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("bucket %d = %d, want %d (all: %v)", i, got[i], want[i], got)
		}
	}
}

func TestConcurrentUpdatesSumExactly(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("c_total", "c")
	v := r.NewCounterVec("v_total", "v", "k", []string{"x", "y"})
	h := r.NewHistogram("h_seconds", "h", []float64{1, 2})
	const workers, per = 64, 2000
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < per; j++ {
				c.Inc()
				v.With("x").Add(2)
				h.Observe(0.5) // exactly representable, so the float sum is exact
			}
		}()
	}
	wg.Wait()
	total := uint64(workers * per)
	if c.v.Load() != total || v.With("x").v.Load() != 2*total || v.With("y").v.Load() != 0 {
		t.Fatalf("counter totals wrong: %d %d", c.v.Load(), v.With("x").v.Load())
	}
	out := string(r.Render())
	for _, line := range []string{
		`h_seconds_bucket{le="1"} 128000`,
		`h_seconds_bucket{le="+Inf"} 128000`,
		"h_seconds_sum 64000",
		"h_seconds_count 128000",
	} {
		if !strings.Contains(out, line+"\n") {
			t.Errorf("missing %q in\n%s", line, out)
		}
	}
}

func TestPanics(t *testing.T) {
	r := NewRegistry()
	r.NewCounter("a_total", "a")
	mustPanic(t, "duplicate name", func() { r.NewGauge("a_total", "a") })
	mustPanic(t, "invalid name", func() { r.NewGauge("bad-name", "x") })
	mustPanic(t, "empty name", func() { r.NewGauge("", "x") })
	mustPanic(t, "counter without _total", func() { r.NewCounter("plain", "x") })
	mustPanic(t, "bad label name", func() { r.NewCounterVec("b_total", "x", "bad-label", []string{"a"}) })
	mustPanic(t, "no label values", func() { r.NewCounterVec("c_total", "x", "k", nil) })
	mustPanic(t, "duplicate label values", func() { r.NewCounterVec("d_total", "x", "k", []string{"a", "a"}) })
	mustPanic(t, "unsorted bounds", func() { r.NewHistogram("e_seconds", "x", []float64{2, 1}) })
	mustPanic(t, "no bounds", func() { r.NewHistogram("f_seconds", "x", nil) })
	mustPanic(t, "nil gauge func", func() { r.NewGaugeFunc("g", "x", nil) })
	v := r.NewCounterVec("h_total", "x", "k", []string{"a"})
	mustPanic(t, "undeclared label value", func() { v.With("zzz") })
}

func TestHandlerContentType(t *testing.T) {
	r := NewRegistry()
	r.NewCounter("x_total", "x").Inc()
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if got := rec.Header().Get("Content-Type"); got != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("content type %q", got)
	}
	if !strings.Contains(rec.Body.String(), "x_total 1\n") {
		t.Fatalf("body %q", rec.Body.String())
	}
}

func TestServiceCatalogue(t *testing.T) {
	start := time.Unix(1700000000, 0)
	s := NewService(func() int64 { return 3 }, func() int64 { return 1 }, func() time.Time { return start })
	s.EventsReceived.With("accepted").Inc()
	s.EventsAccepted.With("fall_warn").Inc()
	s.AlarmVisibleSeconds.Observe(0.2)
	out := string(s.Registry.Render())
	for _, line := range []string{
		`teton_events_received_total{outcome="accepted"} 1`,
		`teton_events_received_total{outcome="storage_error"} 0`,
		`teton_events_accepted_total{type="fall_warn"} 1`,
		`teton_ingest_queue_depth{queue="normal"} 3`,
		`teton_ingest_queue_depth{queue="priority"} 1`,
		`teton_alarm_visible_latency_seconds_bucket{le="0.2"} 1`,
		`teton_read_requests_total{endpoint="alarms_feed"} 0`,
		`teton_process_start_time_seconds 1700000000`,
	} {
		if !strings.Contains(out, line+"\n") {
			t.Errorf("missing %q", line)
		}
	}
}
