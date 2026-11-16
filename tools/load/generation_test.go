package main

import (
	"encoding/json"
	"math/rand/v2"
	"testing"
)

func TestTypedGeneratorKeepsEveryRequiredTypeField(t *testing.T) {
	rng := rand.New(rand.NewPCG(17, 41))
	s := sender{}
	seen := map[string]bool{}
	seenFalse, seenZero := false, false
	for seq := int64(1); seq <= 3000; seq++ {
		e := s.eventWithIDs(rng, "device\"quoted", "room\\escaped", seq)
		var payload map[string]any
		if err := json.Unmarshal(e.body, &payload); err != nil {
			t.Fatal(err)
		}
		if payload["device_id"] != "device\"quoted" || payload["room_id"] != "room\\escaped" || payload["seq"] != float64(seq) {
			t.Fatalf("invalid envelope: %s", e.body)
		}
		typ := payload["type"].(string)
		seen[typ] = true
		field := map[string]string{"presence": "in_room", "motion": "magnitude", "sleep_state": "state", "fall_warn": "confidence", "net_status": "rssi"}[typ]
		wantFields := 5
		if field != "" {
			wantFields++
			if _, ok := payload[field]; !ok {
				t.Fatalf("missing %s: %s", field, e.body)
			}
		}
		if len(payload) != wantFields || e.isFall != (typ == "fall_warn") {
			t.Fatalf("wrong event shape: %s", e.body)
		}
		seenFalse = seenFalse || typ == "presence" && payload["in_room"] == false
		seenZero = seenZero || typ == "motion" && payload["magnitude"] == float64(0)
	}
	if len(seen) != len(eventMix) || !seenFalse || !seenZero {
		t.Fatalf("generator coverage types=%v false=%v zero=%v", seen, seenFalse, seenZero)
	}
}

func BenchmarkGenerateCachedEvent(b *testing.B) {
	rng := rand.New(rand.NewPCG(17, 41))
	s := sender{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.eventWithIDs(rng, "load_run_123", "load_run_room_6", int64(i))
	}
}

func BenchmarkGenerateEvent(b *testing.B) {
	rng := rand.New(rand.NewPCG(17, 41))
	s := sender{prefix: "load_run"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.event(rng, 123, int64(i))
	}
}
