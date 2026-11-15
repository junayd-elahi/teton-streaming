package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
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
	LastHeartbeatTS    *string `json:"last_heartbeat_ts"`
	Availability5m     float64 `json:"availability_5m"`
	ClockOffsetSeconds float64 `json:"clock_offset_seconds"`
}

func heartbeatAt(device string, seq int, ts string) string {
	return fmt.Sprintf(`{"device_id":"%s","room_id":"room_h","type":"heartbeat","ts":"%s","seq":%d}`, device, ts, seq)
}

func parseTS(t *testing.T, ts string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

// postLive sends a heartbeat that the service receives `delay` after the
// device stamped it. A negative delay is a device whose clock runs ahead.
func postLive(t *testing.T, svc *service, device string, seq int, ts string, delay time.Duration) {
	t.Helper()
	svc.clock = parseTS(t, ts).Add(delay)
	mustPost(t, svc, heartbeatAt(device, seq, ts))
}

// A device with an accurate clock, read at 12:05:00. The window is
// (12:00:00, 12:05:00].
func TestDeviceHealthCountsDistinctSecondsInsideTheWindow(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	postLive(t, svc, "dev_h1", 1, "2026-11-15T12:00:00.000Z", 0) // on the open edge: excluded
	postLive(t, svc, "dev_h1", 2, "2026-11-15T12:00:00.500Z", 0) // oldest slot
	postLive(t, svc, "dev_h1", 3, "2026-11-15T12:04:59.200Z", 0) // newest slot
	postLive(t, svc, "dev_h1", 4, "2026-11-15T12:04:59.700Z", 0) // same slot again
	postLive(t, svc, "dev_h1", 5, "2026-11-15T12:05:00.000Z", 0) // on the closed edge: included, same slot
	postLive(t, svc, "dev_other", 1, "2026-11-15T12:03:00.000Z", 0)
	svc.clock = time.Date(2026, 11, 15, 12, 5, 0, 0, time.UTC)

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
	if got.ClockOffsetSeconds != 0 {
		t.Errorf("clock_offset_seconds %v, want 0", got.ClockOffsetSeconds)
	}
}

// A healthy device sends one heartbeat a second for five minutes. Whether its
// clock is right, 30 s behind or 30 s ahead, availability is 1.
func TestDeviceHealthIsNotReducedByClockSkew(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	serverEnd := time.Date(2026, 11, 15, 12, 5, 0, 0, time.UTC)
	skews := map[string]time.Duration{"dev_exact": 0, "dev_behind": -30 * time.Second, "dev_ahead": 30 * time.Second}
	for device, skew := range skews {
		for i := 299; i >= 0; i-- {
			serverTime := serverEnd.Add(-time.Duration(i) * time.Second)
			deviceTS := serverTime.Add(skew).Format("2006-01-02T15:04:05.000Z")
			postLive(t, svc, device, 300-i, deviceTS, -skew)
		}
	}
	svc.clock = serverEnd
	for device, skew := range skews {
		var got health
		getJSON(t, svc, "/devices/"+device+"/health", &got)
		if got.Availability5m != 1 {
			t.Errorf("%s: availability_5m %v, want 1", device, got.Availability5m)
		}
		if got.ClockOffsetSeconds != skew.Seconds() {
			t.Errorf("%s: clock_offset_seconds %v, want %v", device, got.ClockOffsetSeconds, skew.Seconds())
		}
	}
}

// A device 30 s behind sends for five minutes and then stops. Two minutes
// later, 120 of its last 300 seconds are silent: availability is 0.6.
func TestDeviceHealthFallsAfterASkewedDeviceStops(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	stopped := time.Date(2026, 11, 15, 12, 5, 0, 0, time.UTC)
	skew := -30 * time.Second
	for i := 299; i >= 0; i-- {
		serverTime := stopped.Add(-time.Duration(i) * time.Second)
		postLive(t, svc, "dev_dead", 300-i, serverTime.Add(skew).Format("2006-01-02T15:04:05.000Z"), -skew)
	}
	svc.clock = stopped.Add(2 * time.Minute)

	var got health
	getJSON(t, svc, "/devices/dev_dead/health", &got)
	if want := 180.0 / 300; math.Abs(got.Availability5m-want) > 1e-12 {
		t.Errorf("availability_5m %v, want %v", got.Availability5m, want)
	}
	if *got.LastHeartbeatTS != "2026-11-15T12:04:30.000Z" {
		t.Errorf("last_heartbeat_ts %v, want the device's own 12:04:30", *got.LastHeartbeatTS)
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
	if unknown.LastHeartbeatTS != nil || unknown.Availability5m != 0 || unknown.ClockOffsetSeconds != 0 {
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

// A device whose clock runs 20 s ahead reports "present". The room is occupied
// now, although that timestamp has not been reached on the server clock; the
// occupied time only starts to count once it is.
func TestInRoomFollowsLatestPresenceEvenWhenDatedAhead(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)
	svc.clock = time.Date(2026, 11, 15, 12, 1, 0, 0, time.UTC)

	mustPost(t, svc, presenceAt("room_d", "dev_d", 1, "2026-11-15T11:59:00.000Z", false))
	mustPost(t, svc, presenceAt("room_d", "dev_d", 2, "2026-11-15T12:01:20.000Z", true))

	got := occupancyOf(t, svc, "room_d", "1m")
	want := occupancy{InRoom: true, OccupiedPct: 0, WindowSeconds: 60, KnownSeconds: 60, UnknownSeconds: 0}
	if got != want {
		t.Fatalf("at 12:01:00: got %+v, want %+v", got, want)
	}

	svc.clock = svc.clock.Add(50 * time.Second) // 12:01:50, 30 s after the transition
	got = occupancyOf(t, svc, "room_d", "1m")
	want = occupancy{InRoom: true, OccupiedPct: 0.5, WindowSeconds: 60, KnownSeconds: 60, UnknownSeconds: 0}
	if got != want {
		t.Fatalf("at 12:01:50: got %+v, want %+v", got, want)
	}
}

// The readiness and metrics endpoints answer, and the metrics count what the
// service just did.
func TestReadinessAndMetricsReflectActivity(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	var ready struct {
		Ready bool `json:"ready"`
	}
	if status := getJSON(t, svc, "/readyz", &ready); status != http.StatusOK || !ready.Ready {
		t.Fatalf("readyz: status %d ready %v, want 200 and true", status, ready.Ready)
	}

	mustPost(t, svc, heartbeatAt("dev_m", 1, "2026-11-15T11:59:00.000Z"))
	mustPost(t, svc, heartbeatAt("dev_m", 1, "2026-11-15T11:59:00.000Z")) // duplicate
	mustPost(t, svc, fallAt("dev_m", "room_m", 2, "2026-11-15T11:59:01.000Z", 0.9))
	post(t, svc, `{`) // invalid

	resp, err := http.Get(svc.srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, line := range []string{
		`teton_events_received_total{outcome="accepted"} 2`,
		`teton_events_received_total{outcome="duplicate"} 1`,
		`teton_events_received_total{outcome="invalid"} 1`,
		`teton_events_accepted_total{type="fall_warn"} 1`,
		`teton_alarms_created_total 1`,
		`teton_ingest_queue_depth{queue="priority"} 0`,
	} {
		if !strings.Contains(text, line+"\n") {
			t.Errorf("metrics output is missing %q", line)
		}
	}
}
