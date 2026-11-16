package integration

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
)

// Device IDs are not pre-registered. The challenge's 5,001st device is
// dev_5000, which must be usable by a running service.
func TestFiveThousandAndFirstDeviceNeedsNoRegistration(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	// Seed the first 5,000 device identities in the isolated schema, then keep
	// this same service instance running while the next device arrives.
	conn, err := adminConn(context.Background(), svc.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(context.Background(), `
		INSERT INTO event_log (device_id, seq, room_id, type, ts, payload, payload_hash, received_at)
		SELECT 'dev_' || lpad(n::text, 4, '0'), 1, 'room_seed', 'heartbeat',
		       '2026-11-15T11:59:00Z'::timestamptz, '{}'::jsonb,
		       decode(repeat('00', 32), 'hex'), '2026-11-15T11:59:00Z'::timestamptz
		FROM generate_series(0, 4999) AS n`)
	if err != nil {
		conn.Close(context.Background())
		t.Fatal(err)
	}
	if err := conn.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := count(t, svc); got != 5000 {
		t.Fatalf("seeded %d device events, want 5000", got)
	}
	if got := post(t, svc, heartbeatAt("dev_5000", 1, "2026-11-15T11:59:59.500Z")); got != http.StatusOK {
		t.Fatalf("POST from dev_5000: status %d, want 200", got)
	}
	var got health
	if status := getJSON(t, svc, "/devices/dev_5000/health", &got); status != http.StatusOK {
		t.Fatalf("health for dev_5000: status %d, want 200", status)
	}
	if got.LastHeartbeatTS == nil || *got.LastHeartbeatTS != "2026-11-15T11:59:59.500Z" {
		t.Fatalf("health for dev_5000: last heartbeat %v, want posted timestamp", got.LastHeartbeatTS)
	}
	if got := count(t, svc); got != 5001 {
		t.Fatalf("journal has %d rows after adding device 5,001, want 5001", got)
	}
}

func TestOneHourTimestampBoundaries(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	event := func(device, ts string) string {
		return fmt.Sprintf(`{"device_id":%q,"room_id":"room_time","type":"heartbeat","ts":%q,"seq":1}`, device, ts)
	}
	cases := []struct {
		name string
		body string
		want int
	}{
		{"exactly one hour old accepted", event("dev_old_edge", "2026-11-15T11:00:00.000Z"), http.StatusOK},
		{"exactly one hour ahead accepted", event("dev_future_edge", "2026-11-15T13:00:00.000Z"), http.StatusOK},
		{"just beyond one hour ahead rejected", event("dev_future_outside", "2026-11-15T13:00:00.001Z"), http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := post(t, svc, tc.body); got != tc.want {
				t.Fatalf("status %d, want %d", got, tc.want)
			}
		})
	}
	if got := count(t, svc); got != 2 {
		t.Fatalf("journaled %d events, want only the two accepted boundary events", got)
	}
}

func TestHeartbeatAndFallSameIdentityAcrossWriters(t *testing.T) {
	const device = "dev_cross_lane"
	const ts = "2026-11-15T11:59:00.000Z"
	heartbeatBody := heartbeatAt(device, 7, ts)
	fallBody := fallAt(device, "room_cross_lane", 7, ts, 0.9)

	check := func(t *testing.T, first, second string, firstStatus, secondStatus, wantAlarms int) {
		t.Helper()
		svc := startService(t)
		defer svc.stop()
		reset(t, svc)
		if got := post(t, svc, first); got != firstStatus {
			t.Fatalf("first same-identity request status %d, want %d", got, firstStatus)
		}
		if got := post(t, svc, second); got != secondStatus {
			t.Fatalf("second same-identity request status %d, want %d", got, secondStatus)
		}
		if got := count(t, svc); got != 1 {
			t.Fatalf("journal has %d rows, want one winning identity", got)
		}
		if got := len(alarmsFrom(t, svc, "since=0").Alarms); got != wantAlarms {
			t.Fatalf("alarm count %d, want %d", got, wantAlarms)
		}
	}

	t.Run("heartbeat commits first", func(t *testing.T) {
		check(t, heartbeatBody, fallBody, http.StatusOK, http.StatusConflict, 0)
	})
	t.Run("fall commits first", func(t *testing.T) {
		check(t, fallBody, heartbeatBody, http.StatusOK, http.StatusConflict, 1)
	})
	t.Run("simultaneous requests", func(t *testing.T) {
		svc := startService(t)
		defer svc.stop()
		reset(t, svc)

		start := make(chan struct{})
		statuses := make(chan int, 2)
		var wg sync.WaitGroup
		for _, body := range []string{heartbeatBody, fallBody} {
			wg.Add(1)
			go func(body string) {
				defer wg.Done()
				<-start
				statuses <- post(t, svc, body)
			}(body)
		}
		close(start)
		wg.Wait()
		close(statuses)
		counts := map[int]int{}
		for status := range statuses {
			counts[status]++
		}
		if counts[http.StatusOK] != 1 || counts[http.StatusConflict] != 1 {
			t.Fatalf("same-identity cross-lane statuses = %v, want one 200 and one 409", counts)
		}
		if got := count(t, svc); got != 1 {
			t.Fatalf("journal has %d rows, want one winning identity", got)
		}
		page := alarmsFrom(t, svc, "since=0")
		// The stored event type tells which cross-lane request won. A fall winner
		// must have exactly one alarm; a heartbeat winner must have none.
		conn, err := adminConn(context.Background(), svc.databaseURL)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(context.Background())
		var storedType string
		if err := conn.QueryRow(context.Background(), `SELECT type FROM event_log WHERE device_id=$1 AND seq=$2`, device, 7).Scan(&storedType); err != nil {
			t.Fatal(err)
		}
		wantAlarms := 0
		if storedType == "fall_warn" {
			wantAlarms = 1
		} else if storedType != "heartbeat" {
			t.Fatalf("unexpected winning event type %q", storedType)
		}
		if len(page.Alarms) != wantAlarms {
			t.Fatalf("winning %s produced %d alarms, want %d", storedType, len(page.Alarms), wantAlarms)
		}
	})
}
