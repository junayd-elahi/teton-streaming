package integration

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"testing"
	"time"
)

func getJSON(t *testing.T, svc *service, path string, into any) int {
	t.Helper()
	resp, err := http.Get(svc.srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode
}

func mustPost(t *testing.T, svc *service, body string) {
	t.Helper()
	if got := post(t, svc, body); got != http.StatusOK {
		t.Fatalf("post %s: status %d", body, got)
	}
}

type health struct {
	LastHeartbeatTS *string `json:"last_heartbeat_ts"`
	Availability5m  float64 `json:"availability_5m"`
}

func heartbeatAt(device string, seq int, ts string) string {
	return fmt.Sprintf(`{"device_id":"%s","room_id":"room_h","type":"heartbeat","ts":"%s","seq":%d}`, device, ts, seq)
}

// Clock is 12:05:00. The window is (12:00:00, 12:05:00].
func TestDeviceHealthCountsDistinctSecondsInsideTheWindow(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)
	svc.clock = time.Date(2026, 11, 15, 12, 5, 0, 0, time.UTC)

	mustPost(t, svc, heartbeatAt("dev_h1", 1, "2026-11-15T12:00:00.000Z")) // on the open edge: excluded
	mustPost(t, svc, heartbeatAt("dev_h1", 2, "2026-11-15T12:00:00.500Z")) // oldest slot
	mustPost(t, svc, heartbeatAt("dev_h1", 3, "2026-11-15T12:04:59.200Z")) // newest slot
	mustPost(t, svc, heartbeatAt("dev_h1", 4, "2026-11-15T12:04:59.700Z")) // same slot again
	mustPost(t, svc, heartbeatAt("dev_h1", 5, "2026-11-15T12:05:00.000Z")) // on the closed edge: included, same slot
	mustPost(t, svc, heartbeatAt("dev_h1", 6, "2026-11-15T12:05:30.000Z")) // ahead of the clock: not yet effective
	mustPost(t, svc, heartbeatAt("dev_other", 1, "2026-11-15T12:03:00.000Z"))

	var got health
	if status := getJSON(t, svc, "/devices/dev_h1/health", &got); status != http.StatusOK {
		t.Fatalf("status %d", status)
	}
	if got.LastHeartbeatTS == nil || *got.LastHeartbeatTS != "2026-11-15T12:05:00.000Z" {
		t.Errorf("last_heartbeat_ts %v, want 2026-11-15T12:05:00.000Z", got.LastHeartbeatTS)
	}
	if want := 2.0 / 300; math.Abs(got.Availability5m-want) > 1e-12 {
		t.Errorf("availability_5m %v, want %v", got.Availability5m, want)
	}

	// Thirty seconds later the future heartbeat has become effective.
	svc.clock = svc.clock.Add(30 * time.Second)
	getJSON(t, svc, "/devices/dev_h1/health", &got)
	if *got.LastHeartbeatTS != "2026-11-15T12:05:30.000Z" {
		t.Errorf("after clock advance: last_heartbeat_ts %v", *got.LastHeartbeatTS)
	}
}

func TestDeviceHealthFullAvailabilityAndUnknownDevice(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)
	svc.clock = time.Date(2026, 11, 15, 12, 5, 0, 0, time.UTC)

	// One heartbeat per second for the whole window, sent newest first.
	for i := 0; i < 300; i++ {
		ts := svc.clock.Add(-time.Duration(i) * time.Second).Format("2006-01-02T15:04:05.000Z")
		mustPost(t, svc, heartbeatAt("dev_h2", 300-i, ts))
	}
	var got health
	getJSON(t, svc, "/devices/dev_h2/health", &got)
	if got.Availability5m != 1 {
		t.Errorf("availability_5m %v, want 1", got.Availability5m)
	}

	var unknown health
	if status := getJSON(t, svc, "/devices/dev_never_seen/health", &unknown); status != http.StatusOK {
		t.Fatalf("unknown device: status %d", status)
	}
	if unknown.LastHeartbeatTS != nil || unknown.Availability5m != 0 {
		t.Errorf("unknown device: got %+v, want null and 0", unknown)
	}
}

type occupancy struct {
	InRoom         bool    `json:"in_room"`
	OccupiedPct    float64 `json:"occupied_pct"`
	WindowSeconds  float64 `json:"window_seconds"`
	KnownSeconds   float64 `json:"known_seconds"`
	UnknownSeconds float64 `json:"unknown_seconds"`
}

func presenceAt(room, device string, seq int, ts string, inRoom bool) string {
	return fmt.Sprintf(`{"device_id":"%s","room_id":"%s","type":"presence","ts":"%s","seq":%d,"in_room":%t}`, device, room, ts, seq, inRoom)
}

func occupancyOf(t *testing.T, svc *service, room, window string) occupancy {
	t.Helper()
	var got occupancy
	if status := getJSON(t, svc, "/rooms/"+room+"/occupancy?window="+window, &got); status != http.StatusOK {
		t.Fatalf("occupancy %s %s: status %d", room, window, status)
	}
	return got
}

// Absent at 12:00:00, present at :10, absent at :40. At 12:01:00 the room was
// occupied 30 of the last 60 seconds. A late "present at :50" makes it 40.
func TestOccupancyIntegratesIntervalsAndAppliesLateCorrection(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)
	svc.clock = time.Date(2026, 11, 15, 12, 1, 0, 0, time.UTC)

	mustPost(t, svc, presenceAt("room_a", "dev_a", 1, "2026-11-15T12:00:00.000Z", false))
	mustPost(t, svc, presenceAt("room_a", "dev_a", 2, "2026-11-15T12:00:10.000Z", true))
	mustPost(t, svc, presenceAt("room_a", "dev_a", 3, "2026-11-15T12:00:40.000Z", false))

	got := occupancyOf(t, svc, "room_a", "1m")
	want := occupancy{InRoom: false, OccupiedPct: 0.5, WindowSeconds: 60, KnownSeconds: 60, UnknownSeconds: 0}
	if got != want {
		t.Fatalf("before late event: got %+v, want %+v", got, want)
	}

	mustPost(t, svc, presenceAt("room_a", "dev_a", 9, "2026-11-15T12:00:50.000Z", true))
	got = occupancyOf(t, svc, "room_a", "1m")
	want = occupancy{InRoom: true, OccupiedPct: 40.0 / 60, WindowSeconds: 60, KnownSeconds: 60, UnknownSeconds: 0}
	if got != want {
		t.Fatalf("after late event: got %+v, want %+v", got, want)
	}

	// The same history seen through the 5 minute window: only the last minute
	// is known, so 40 occupied seconds out of 300 with 240 unknown.
	got = occupancyOf(t, svc, "room_a", "5m")
	want = occupancy{InRoom: true, OccupiedPct: 40.0 / 300, WindowSeconds: 300, KnownSeconds: 60, UnknownSeconds: 240}
	if got != want {
		t.Fatalf("5m window: got %+v, want %+v", got, want)
	}
}

// The same events sent in order, reversed and interleaved give the same answer.
func TestOccupancyDoesNotDependOnArrivalOrder(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	svc.clock = time.Date(2026, 11, 15, 12, 1, 0, 0, time.UTC)

	events := []string{
		presenceAt("room_b", "dev_b", 1, "2026-11-15T11:59:00.000Z", false),
		presenceAt("room_b", "dev_b", 2, "2026-11-15T12:00:10.000Z", true),
		presenceAt("room_b", "dev_b", 3, "2026-11-15T12:00:25.000Z", false),
		presenceAt("room_b", "dev_b", 4, "2026-11-15T12:00:30.000Z", true),
		presenceAt("room_b", "dev_b", 5, "2026-11-15T12:00:55.000Z", false),
	}
	orders := map[string][]int{
		"in order":    {0, 1, 2, 3, 4},
		"reversed":    {4, 3, 2, 1, 0},
		"interleaved": {2, 0, 4, 1, 3},
	}
	want := occupancy{InRoom: false, OccupiedPct: 40.0 / 60, WindowSeconds: 60, KnownSeconds: 60, UnknownSeconds: 0}
	for name, order := range orders {
		reset(t, svc)
		for _, i := range order {
			mustPost(t, svc, events[i])
		}
		if got := occupancyOf(t, svc, "room_b", "1m"); got != want {
			t.Errorf("%s: got %+v, want %+v", name, got, want)
		}
	}
}

// Two devices in one room: the room follows the latest transition from either.
// At an equal timestamp the higher device_id is applied last and wins.
func TestOccupancySharedRoomAndEqualTimestamps(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)
	svc.clock = time.Date(2026, 11, 15, 12, 1, 0, 0, time.UTC)

	mustPost(t, svc, presenceAt("room_c", "dev_c1", 1, "2026-11-15T11:59:00.000Z", false))
	mustPost(t, svc, presenceAt("room_c", "dev_c2", 1, "2026-11-15T12:00:20.000Z", true))
	mustPost(t, svc, presenceAt("room_c", "dev_c1", 2, "2026-11-15T12:00:30.000Z", false))
	// Tie at :40. dev_c2 sorts after dev_c1, so "present" is the state from :40.
	mustPost(t, svc, presenceAt("room_c", "dev_c2", 2, "2026-11-15T12:00:40.000Z", true))
	mustPost(t, svc, presenceAt("room_c", "dev_c1", 3, "2026-11-15T12:00:40.000Z", false))

	got := occupancyOf(t, svc, "room_c", "1m")
	want := occupancy{InRoom: true, OccupiedPct: 30.0 / 60, WindowSeconds: 60, KnownSeconds: 60, UnknownSeconds: 0}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestOccupancyUnknownRoomAndBadWindow(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	got := occupancyOf(t, svc, "room_never_seen", "1h")
	want := occupancy{InRoom: false, OccupiedPct: 0, WindowSeconds: 3600, KnownSeconds: 0, UnknownSeconds: 3600}
	if got != want {
		t.Fatalf("unknown room: got %+v, want %+v", got, want)
	}
	var body map[string]any
	if status := getJSON(t, svc, "/rooms/room_a/occupancy?window=2m", &body); status != http.StatusBadRequest {
		t.Fatalf("window=2m: status %d, want 400", status)
	}
}
