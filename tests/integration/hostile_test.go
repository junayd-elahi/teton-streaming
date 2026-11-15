package integration

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
)

func hostile(device, extra string) string {
	return fmt.Sprintf(`{"device_id":"%s","room_id":"room_x","type":"heartbeat","ts":"2026-11-15T11:59:00.000Z","seq":1%s}`, device, extra)
}

// Inputs PostgreSQL cannot store are rejected as invalid, not reported as a
// retryable storage failure.
func TestUnstorableInputIsRejectedAsInvalid(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	cases := map[string]string{
		"NUL in device_id":           hostile(`dev\u0000x`, ``),
		"NUL in an extra field":      hostile("dev_x1", `,"note":"a\u0000b"`),
		"NUL in an extra field name": hostile("dev_x2", `,"a\u0000b":1`),
		"NUL nested in an array":     hostile("dev_x3", `,"notes":[{"deep":"\u0000"}]`),
		"number beyond float range":  hostile("dev_x4", `,"x":1e999999`),
		"number with a huge literal": hostile("dev_x5", `,"x":0.`+strings.Repeat("0", 40)+`1`),
		"device_id too long":         hostile(strings.Repeat("d", 129), ``),
		"data after the object":      hostile("dev_x6", ``) + ` {"more":1}`,
	}
	for name, body := range cases {
		if got := post(t, svc, body); got != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", name, got)
		}
	}
	if got := count(t, svc); got != 0 {
		t.Fatalf("%d events journaled, want 0", got)
	}
	if got := post(t, svc, hostile(strings.Repeat("d", 128), ``)); got != http.StatusOK {
		t.Errorf("device_id of exactly 128 bytes: status %d, want 200", got)
	}
}

// Hostile events sent at the same moment as valid ones must not cause any
// valid event to be refused.
func TestHostileEventsDoNotAffectValidEventsSentAlongside(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	const valid = 200
	statuses := make([]int, valid)
	var wg sync.WaitGroup
	for i := 0; i < valid; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			statuses[i] = post(t, svc, hostile(fmt.Sprintf("dev_ok_%d", i), ``))
		}()
		if i%10 == 0 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				post(t, svc, hostile(fmt.Sprintf("dev_bad_%d", i), `,"x":1e999999`))
			}()
		}
	}
	wg.Wait()
	for i, status := range statuses {
		if status != http.StatusOK {
			t.Fatalf("valid event %d: status %d, want 200", i, status)
		}
	}
	if got := count(t, svc); got != valid {
		t.Fatalf("%d events journaled, want %d", got, valid)
	}
}
