package main

import (
	"math/rand/v2"
	"testing"
	"time"
)

func TestScheduledAddsTheBurstOnTopOfTheBaseline(t *testing.T) {
	cfg := config{Rate: 5000, BurstRate: 50000, BurstStart: 20 * time.Second, BurstDuration: 30 * time.Second, Duration: 60 * time.Second}
	cases := map[time.Duration]float64{
		0:                0,
		10 * time.Second: 50_000,
		20 * time.Second: 100_000,
		30 * time.Second: 100_000 + 500_000,
		50 * time.Second: 100_000 + 1_500_000,
		60 * time.Second: 100_000 + 1_500_000 + 50_000,
	}
	for elapsed, want := range cases {
		if got := scheduled(cfg, elapsed); got != want {
			t.Errorf("after %v: %v events scheduled, want %v", elapsed, got, want)
		}
	}
}

func TestSummariseReadsExactPercentiles(t *testing.T) {
	micros := make([]uint32, 101)
	for i := range micros {
		micros[i] = uint32((100 - i) * 1000) // 100ms down to 0ms, unsorted
	}
	got := summarise(micros)
	want := percentiles{Count: 101, P50: 50, P95: 95, P99: 99, Max: 100}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if empty := summarise(nil); empty != (percentiles{}) {
		t.Fatalf("no samples: got %+v, want zeros", empty)
	}
}

func TestPickTypeFollowsTheMix(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	counts := map[string]int{}
	const draws = 760_000
	for i := 0; i < draws; i++ {
		counts[pickType(rng)]++
	}
	for _, m := range eventMix {
		expected := float64(draws) * float64(m.weight) / 76
		if got := float64(counts[m.typ]); got < expected*0.95 || got > expected*1.05 {
			t.Errorf("%s: %v draws, want about %v", m.typ, got, expected)
		}
	}
}

func TestFallTrackerCountsMissingAndDuplicates(t *testing.T) {
	f := newFallTracker()
	sent := time.Now()
	f.sent("d", 1, sent)
	f.sent("d", 2, sent)
	f.accepted("d", 1)
	f.accepted("d", 2)
	f.delivered("d:1", sent.Add(40*time.Millisecond))
	f.delivered("d:1", sent.Add(50*time.Millisecond))
	if got := f.missing(); got != 1 {
		t.Errorf("missing %d, want 1", got)
	}
	if f.duplicates != 1 {
		t.Errorf("duplicates %d, want 1", f.duplicates)
	}
	if got := f.latenciesMicros(); len(got) != 1 || got[0] != 40_000 {
		t.Errorf("latencies %v, want [40000]", got)
	}
}
