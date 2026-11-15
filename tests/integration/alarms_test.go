package integration

import (
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

type alarm struct {
	EventID    string  `json:"event_id"`
	AlarmID    int64   `json:"alarm_id"`
	DeviceID   string  `json:"device_id"`
	RoomID     string  `json:"room_id"`
	Type       string  `json:"type"`
	TS         string  `json:"ts"`
	Seq        int64   `json:"seq"`
	Confidence float64 `json:"confidence"`
}

type alarmPage struct {
	Alarms     []alarm `json:"alarms"`
	NextCursor int64   `json:"next_cursor"`
}

func fallAt(device, room string, seq int, ts string, confidence float64) string {
	return fmt.Sprintf(`{"device_id":"%s","room_id":"%s","type":"fall_warn","ts":"%s","seq":%d,"confidence":%v}`,
		device, room, ts, seq, confidence)
}

func alarmsFrom(t *testing.T, svc *service, query string) alarmPage {
	t.Helper()
	var page alarmPage
	if status := getJSON(t, svc, "/alarms?"+query, &page); status != http.StatusOK {
		t.Fatalf("GET /alarms?%s: status %d", query, status)
	}
	return page
}

// Jitter copies share device, room, timestamp and confidence but carry new
// sequence numbers. They are all journaled and produce one alarm.
func TestJitterCopiesProduceOneAlarm(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	mustPost(t, svc, fallAt("dev_f1", "room_f", 10, "2026-11-15T11:59:00.000Z", 0.92))
	mustPost(t, svc, fallAt("dev_f1", "room_f", 11, "2026-11-15T11:59:00.000Z", 0.92))
	mustPost(t, svc, fallAt("dev_f1", "room_f", 12, "2026-11-15T11:59:00.000Z", 0.92))
	mustPost(t, svc, fallAt("dev_f1", "room_f", 10, "2026-11-15T11:59:00.000Z", 0.92)) // exact retry

	if got := count(t, svc); got != 3 {
		t.Fatalf("%d journaled events, want 3", got)
	}
	page := alarmsFrom(t, svc, "since=0")
	if len(page.Alarms) != 1 {
		t.Fatalf("%d alarms, want 1", len(page.Alarms))
	}
	want := alarm{EventID: "dev_f1:10", AlarmID: 1, DeviceID: "dev_f1", RoomID: "room_f", Type: "fall_warn",
		TS: "2026-11-15T11:59:00.000Z", Seq: 10, Confidence: 0.92}
	if page.Alarms[0] != want {
		t.Fatalf("got %+v, want %+v", page.Alarms[0], want)
	}
}

// Falls that differ in time, device or confidence are separate alarms.
func TestDistinctFallsStaySeparate(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	mustPost(t, svc, fallAt("dev_f1", "room_f", 1, "2026-11-15T11:59:00.000Z", 0.92))
	mustPost(t, svc, fallAt("dev_f1", "room_f", 2, "2026-11-15T11:59:04.000Z", 0.92)) // four seconds later
	mustPost(t, svc, fallAt("dev_f2", "room_f", 1, "2026-11-15T11:59:00.000Z", 0.92)) // other device
	mustPost(t, svc, fallAt("dev_f1", "room_f", 3, "2026-11-15T11:59:00.000Z", 0.81)) // other confidence

	if got := len(alarmsFrom(t, svc, "since=0").Alarms); got != 4 {
		t.Fatalf("%d alarms, want 4", got)
	}
}

// 30 simultaneous jitter copies of one fall still produce one alarm.
func TestConcurrentJitterCopiesProduceOneAlarm(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	var wg sync.WaitGroup
	for seq := 1; seq <= 30; seq++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mustPost(t, svc, fallAt("dev_f3", "room_f", seq, "2026-11-15T11:59:00.000Z", 0.9))
		}()
	}
	wg.Wait()
	if got := count(t, svc); got != 30 {
		t.Fatalf("%d journaled events, want 30", got)
	}
	if got := len(alarmsFrom(t, svc, "since=0").Alarms); got != 1 {
		t.Fatalf("%d alarms, want 1", got)
	}
}

// The snapshot is ordered by event time. since filters on when the service
// received the fall, so a late fall is returned to a poller that passes its
// last poll time. since is unix seconds or a timestamp.
func TestSnapshotIsOrderedByEventTimeAndFilteredByReceiptTime(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	svc.clock = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	mustPost(t, svc, fallAt("dev_f1", "room_f", 1, "2026-11-15T11:50:00.000Z", 0.9))
	svc.clock = time.Date(2026, 11, 15, 12, 10, 0, 0, time.UTC)
	mustPost(t, svc, fallAt("dev_f1", "room_f", 2, "2026-11-15T11:30:00.000Z", 0.9)) // late: happened first, received last
	mustPost(t, svc, fallAt("dev_f1", "room_f", 3, "2026-11-15T12:09:00.000Z", 0.9))

	all := alarmsFrom(t, svc, "since=0").Alarms
	order := []string{"2026-11-15T11:30:00.000Z", "2026-11-15T11:50:00.000Z", "2026-11-15T12:09:00.000Z"}
	if len(all) != 3 {
		t.Fatalf("%d alarms, want 3", len(all))
	}
	for i, ts := range order {
		if all[i].TS != ts {
			t.Errorf("position %d: ts %s, want %s", i, all[i].TS, ts)
		}
	}

	// A poller that last looked at 12:05:00 gets both falls received after
	// that, including the one that happened at 11:30.
	lastPoll := time.Date(2026, 11, 15, 12, 5, 0, 0, time.UTC)
	for _, since := range []string{fmt.Sprint(lastPoll.Unix()), "2026-11-15T12:05:00.000Z"} {
		got := alarmsFrom(t, svc, "since="+since).Alarms
		if len(got) != 2 || got[0].TS != "2026-11-15T11:30:00.000Z" || got[1].TS != "2026-11-15T12:09:00.000Z" {
			t.Errorf("since=%s: got %+v, want the 11:30 and 12:09 falls", since, got)
		}
	}
	var body map[string]any
	if status := getJSON(t, svc, "/alarms?since=yesterday", &body); status != http.StatusBadRequest {
		t.Errorf("since=yesterday: status %d, want 400", status)
	}
}

// A consumer saves its cursor, the service restarts, new falls arrive
// (including one whose event time is older than anything delivered), and the
// consumer resumes from the cursor without missing or repeating an alarm.
func TestFeedResumesFromCursorAcrossRestartIncludingLateFall(t *testing.T) {
	svc := startService(t)
	reset(t, svc)

	mustPost(t, svc, fallAt("dev_f1", "room_f", 1, "2026-11-15T11:50:00.000Z", 0.9))
	mustPost(t, svc, fallAt("dev_f1", "room_f", 2, "2026-11-15T11:51:00.000Z", 0.9))
	first := alarmsFrom(t, svc, "after_id=0")
	if len(first.Alarms) != 2 || first.NextCursor != 2 {
		t.Fatalf("first read: %d alarms, cursor %d; want 2 and 2", len(first.Alarms), first.NextCursor)
	}
	svc.stop()

	svc = startService(t)
	defer svc.stop()
	mustPost(t, svc, fallAt("dev_f1", "room_f", 3, "2026-11-15T11:52:00.000Z", 0.9))
	mustPost(t, svc, fallAt("dev_f9", "room_f", 1, "2026-11-15T11:05:00.000Z", 0.9)) // replayed from an offline device
	mustPost(t, svc, fallAt("dev_f1", "room_f", 4, "2026-11-15T11:52:00.000Z", 0.9)) // jitter copy

	resumed := alarmsFrom(t, svc, fmt.Sprintf("after_id=%d", first.NextCursor))
	if len(resumed.Alarms) != 2 {
		t.Fatalf("resumed read: %d alarms, want 2", len(resumed.Alarms))
	}
	if resumed.Alarms[0].TS != "2026-11-15T11:52:00.000Z" || resumed.Alarms[1].TS != "2026-11-15T11:05:00.000Z" {
		t.Fatalf("resumed read: got %s then %s, want commit order", resumed.Alarms[0].TS, resumed.Alarms[1].TS)
	}
	if resumed.NextCursor != 4 {
		t.Fatalf("next_cursor %d, want 4", resumed.NextCursor)
	}

	caughtUp := alarmsFrom(t, svc, fmt.Sprintf("after_id=%d", resumed.NextCursor))
	if len(caughtUp.Alarms) != 0 || caughtUp.NextCursor != 4 {
		t.Fatalf("caught up: %d alarms, cursor %d; want 0 and 4", len(caughtUp.Alarms), caughtUp.NextCursor)
	}
}

// A waiting consumer is answered as soon as a fall commits, not when its wait
// runs out. A jitter copy does not wake it with a second alarm.
func TestLongPollReturnsWhenAFallCommits(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	type outcome struct {
		page    alarmPage
		elapsed time.Duration
	}
	done := make(chan outcome, 1)
	started := time.Now()
	go func() {
		page := alarmsFrom(t, svc, "after_id=0&wait=10")
		done <- outcome{page, time.Since(started)}
	}()

	time.Sleep(200 * time.Millisecond) // let the consumer start waiting
	mustPost(t, svc, heartbeatAt("dev_f1", 1, "2026-11-15T11:59:00.000Z"))
	sent := time.Now()
	mustPost(t, svc, fallAt("dev_f1", "room_f", 2, "2026-11-15T11:59:30.000Z", 0.95))

	select {
	case got := <-done:
		if len(got.page.Alarms) != 1 || got.page.NextCursor != 1 {
			t.Fatalf("long poll: %d alarms, cursor %d; want 1 and 1", len(got.page.Alarms), got.page.NextCursor)
		}
		if delay := time.Since(sent); delay > time.Second {
			t.Fatalf("alarm reached the consumer %v after it was sent, want under 1s", delay)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("long poll did not return after a fall was committed")
	}
}

func TestLongPollTimesOutEmptyAndRejectsBadParameters(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	started := time.Now()
	page := alarmsFrom(t, svc, "after_id=0&wait=0.3")
	if len(page.Alarms) != 0 || page.NextCursor != 0 {
		t.Fatalf("empty wait: %d alarms, cursor %d; want 0 and 0", len(page.Alarms), page.NextCursor)
	}
	if elapsed := time.Since(started); elapsed < 300*time.Millisecond {
		t.Fatalf("returned after %v, want at least the 0.3s wait", elapsed)
	}
	var body map[string]any
	for _, query := range []string{"after_id=abc", "after_id=-1", "after_id=0&wait=31", "after_id=0&wait=x"} {
		if status := getJSON(t, svc, "/alarms?"+query, &body); status != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", query, status)
		}
	}
}

func TestAlarmQueryRejectsNonFiniteParameters(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	var body map[string]any
	for _, query := range []string{"since=NaN", "since=Inf", "since=-1", "since=1e300", "after_id=0&wait=NaN", "after_id=0&wait=Inf"} {
		if status := getJSON(t, svc, "/alarms?"+query, &body); status != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", query, status)
		}
	}
}
