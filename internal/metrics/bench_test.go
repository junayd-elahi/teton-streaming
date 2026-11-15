package metrics

import "testing"

func BenchmarkCounterInc(b *testing.B) {
	c := NewRegistry().NewCounter("c_total", "c")
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Inc()
		}
	})
}

func BenchmarkHistogramObserve(b *testing.B) {
	h := NewRegistry().NewHistogram("h_seconds", "h", []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5})
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			h.Observe(0.02)
		}
	})
}
