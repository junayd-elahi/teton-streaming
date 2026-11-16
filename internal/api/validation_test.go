package api

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestUnitIntervalExactDecimalEndpoints(t *testing.T) {
	received := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	for _, field := range []struct{ typ, name string }{{"motion", "magnitude"}, {"fall_warn", "confidence"}} {
		for _, tc := range []struct {
			literal string
			valid   bool
		}{
			{"0", true}, {"-0", true}, {"0e999999", true}, {"-0.0e-999999", true},
			{"1", true}, {"1.0000000000000000", true}, {"10e-1", true}, {"0.10000000000000000e1", true},
			{"100000000000000000000e-20", true}, {"1e+00000000000000000000000", true},
			{"0.99999999999999999", true}, {"9.9999999999999999e-1", true}, {"5e-324", true},
			{"1.0000000000000001", false}, {"0.10000000000000001e1", false},
			{"10000000000000001e-16", false}, {"1.00000000000000001e0", false},
			{"-0.0000000000000001", false}, {"-5e-324", false}, {"1e-400", false}, {"-1e-400", false},
		} {
			t.Run(field.typ+"/"+tc.literal, func(t *testing.T) {
				body := fmt.Sprintf(`{"device_id":"d","room_id":"r","type":%q,"seq":1,"ts":"2026-11-15T12:00:00Z",%q:%s}`, field.typ, field.name, tc.literal)
				_, err := parseEvent(strings.NewReader(body), received)
				if (err == nil) != tc.valid {
					t.Fatalf("valid=%v error=%v", tc.valid, err)
				}
			})
		}
	}
}

func TestParseEventCanonicalCompatibility(t *testing.T) {
	received := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	const prefix = `"device_id":"d","extra":{"array":[0.50,{"text":"<>&\u2028","zero":`
	const suffix = `}]},"room_id":"r","seq":1,"ts":"2026-11-15T12:00:00Z","type":"heartbeat"}`
	for _, zero := range []string{"0", "-0.0e999999"} {
		t.Run(zero, func(t *testing.T) {
			// Unknown fields and lexical number forms are part of retry identity.
			body := `{"type":"heartbeat","seq":1,"room_id":"r","ts":"2026-11-15T12:00:00Z","extra":{"array":[0.50,{"text":"<>&\u2028","zero":` + zero + `}]},"device_id":"d"}`
			wantInput := "{" + prefix + zero + suffix
			event, err := parseEvent(strings.NewReader(body), received)
			if err != nil {
				t.Fatal(err)
			}
			wantHash := sha256.Sum256([]byte(wantInput))
			if string(event.PayloadHash) != string(wantHash[:]) {
				t.Fatalf("canonical hash changed: got %x want %x", event.PayloadHash, wantHash)
			}
			wantPayload := "{" + prefix + "0" + suffix
			if string(event.Payload) != wantPayload {
				t.Fatalf("payload %s want %s", event.Payload, wantPayload)
			}
		})
	}
}

func TestZeroNormalizationStillValidatesEveryUnknownField(t *testing.T) {
	received := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	for _, extra := range []string{
		`{"a":0e999999,"z":["\u0000"]}`,
		`{"a":0e999999,"z":{"\u0000":true}}`,
		`{"a":0e999999,"z":1e-400}`,
	} {
		body := `{"device_id":"d","room_id":"r","type":"heartbeat","seq":1,"ts":"2026-11-15T12:00:00Z","extra":` + extra + `}`
		if _, err := parseEvent(strings.NewReader(body), received); err == nil {
			t.Fatalf("accepted hostile unknown field: %s", extra)
		}
	}
}
