// Integration tests for the durable ingest path. They need a real PostgreSQL:
//
//	DATABASE_URL=postgres://... go test ./tests/integration/
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/junayd-elahi/teton-streaming/internal/api"
	"github.com/junayd-elahi/teton-streaming/internal/ingest"
	"github.com/junayd-elahi/teton-streaming/internal/metrics"
	"github.com/junayd-elahi/teton-streaming/internal/store"
	"github.com/junayd-elahi/teton-streaming/migrations"
)

var fixedNow = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

const heartbeat = `{"device_id":"dev_0001","room_id":"room_001","type":"heartbeat","ts":"2026-11-15T11:59:59.500Z","seq":1}`

// service is one running instance: store, writer and HTTP server.
type service struct {
	databaseURL string
	store       *store.Store
	writer      *ingest.Writer
	srv         *httptest.Server
	stopWriter  context.CancelFunc
	// clock is the time the service believes it is. Tests move it.
	clock time.Time
	// dropNextResponse makes the server process the next request fully and then
	// close the connection without replying, like a response lost on the network.
	dropNextResponse bool
}

// startService opens a fresh store, writer and HTTP server, as a process start would.
func startService(t *testing.T) *service {
	t.Helper()
	url := isolatedDatabaseURL(t)
	ctx := context.Background()
	st, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx, migrations.Files); err != nil {
		t.Fatal(err)
	}
	writerCtx, stopWriter := context.WithCancel(ctx)
	var writer *ingest.Writer
	m := metrics.NewService(
		func() int64 { normal, _ := writer.QueueDepths(); return int64(normal) },
		func() int64 { _, priority := writer.QueueDepths(); return int64(priority) },
		time.Now)
	writer = ingest.NewWriter(st, ingest.Config{
		Writers: 4, QueueSize: 1000, PriorityQueueSize: 1000, BatchSize: 100, Linger: time.Millisecond,
		EnqueueTimeout: 2 * time.Second, CommitTimeout: 5 * time.Second,
	}, m)
	go writer.Run(writerCtx)

	svc := &service{databaseURL: url, store: st, writer: writer, stopWriter: stopWriter, clock: fixedNow}
	handler := api.New(writer, st, m, func() time.Time { return svc.clock }).Handler()
	svc.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !svc.dropNextResponse {
			handler.ServeHTTP(w, r)
			return
		}
		svc.dropNextResponse = false
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, r)
		if rec.Code != http.StatusOK {
			t.Errorf("dropped request: status %d, want 200", rec.Code)
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	}))
	return svc
}

func (s *service) stop() {
	s.srv.Close()
	s.stopWriter()
	s.store.Close()
}

// reply is the status and decoded body of one POST /events.
type reply struct {
	status    int
	duplicate bool
}

func postReply(t *testing.T, svc *service, body string) reply {
	t.Helper()
	resp, err := http.Post(svc.srv.URL+"/events", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var decoded struct {
		Duplicate bool `json:"duplicate"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	return reply{status: resp.StatusCode, duplicate: decoded.Duplicate}
}

func post(t *testing.T, svc *service, body string) int {
	t.Helper()
	return postReply(t, svc, body).status
}

func count(t *testing.T, svc *service) int64 {
	t.Helper()
	n, err := svc.store.CountEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func reset(t *testing.T, svc *service) {
	t.Helper()
	conn, err := adminConn(context.Background(), svc.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(context.Background(), `TRUNCATE event_log, alarms RESTART IDENTITY`); err != nil {
		t.Fatal(err)
	}
}
func TestEventSurvivesRestartAndReplayHasNoEffect(t *testing.T) {
	svc := startService(t)
	reset(t, svc)
	if got := post(t, svc, heartbeat); got != http.StatusOK {
		t.Fatalf("first post: status %d", got)
	}
	svc.stop()

	svc = startService(t)
	defer svc.stop()
	if got := count(t, svc); got != 1 {
		t.Fatalf("after restart: %d events, want 1", got)
	}
	for i := 0; i < 100; i++ {
		if got := post(t, svc, heartbeat); got != http.StatusOK {
			t.Fatalf("replay %d: status %d", i, got)
		}
	}
	if got := count(t, svc); got != 1 {
		t.Fatalf("after 100 replays: %d events, want 1", got)
	}
}

func TestReplayWithReorderedKeysIsSameEvent(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)
	reordered := `{"seq":1,"ts":"2026-11-15T11:59:59.500Z","type":"heartbeat","room_id":"room_001","device_id":"dev_0001"}`
	if got := post(t, svc, heartbeat); got != http.StatusOK {
		t.Fatalf("first post: status %d", got)
	}
	if got := post(t, svc, reordered); got != http.StatusOK {
		t.Fatalf("reordered replay: status %d", got)
	}
	if got := count(t, svc); got != 1 {
		t.Fatalf("%d events, want 1", got)
	}
}

func TestSameIdentityDifferentPayloadIsConflict(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)
	changed := strings.Replace(heartbeat, "room_001", "room_002", 1)
	if got := post(t, svc, heartbeat); got != http.StatusOK {
		t.Fatalf("first post: status %d", got)
	}
	if got := post(t, svc, changed); got != http.StatusConflict {
		t.Fatalf("changed payload: status %d, want 409", got)
	}
	if got := count(t, svc); got != 1 {
		t.Fatalf("%d events, want 1", got)
	}
}

func TestValidation(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	event := func(ts string) string {
		return `{"device_id":"dev_0002","room_id":"room_001","type":"heartbeat","ts":"` + ts + `","seq":1}`
	}
	typed := func(typ, extra string) string {
		return `{"device_id":"dev_0004","room_id":"room_001","type":"` + typ + `","ts":"2026-11-15T12:00:00.000Z","seq":1` + extra + `}`
	}
	cases := []struct {
		name string
		body string
		want int
	}{
		{"exactly one hour ahead accepted", event("2026-11-15T13:00:00.000Z"), http.StatusOK},
		{"over one hour ahead rejected", event("2026-11-15T13:00:00.001Z"), http.StatusBadRequest},
		{"not json", `{`, http.StatusBadRequest},
		{"missing device_id", `{"room_id":"room_001","type":"heartbeat","ts":"2026-11-15T12:00:00.000Z","seq":1}`, http.StatusBadRequest},
		{"missing seq", `{"device_id":"dev_0003","room_id":"room_001","type":"heartbeat","ts":"2026-11-15T12:00:00.000Z"}`, http.StatusBadRequest},
		{"unknown type", `{"device_id":"dev_0003","room_id":"room_001","type":"nope","ts":"2026-11-15T12:00:00.000Z","seq":1}`, http.StatusBadRequest},
		{"bad ts", `{"device_id":"dev_0003","room_id":"room_001","type":"heartbeat","ts":"yesterday","seq":1}`, http.StatusBadRequest},
		{"presence without in_room", typed("presence", ``), http.StatusBadRequest},
		{"presence with non-boolean in_room", typed("presence", `,"in_room":"yes"`), http.StatusBadRequest},
		{"motion above 1", typed("motion", `,"magnitude":1.01`), http.StatusBadRequest},
		{"motion without magnitude", typed("motion", ``), http.StatusBadRequest},
		{"fall_warn below 0", typed("fall_warn", `,"confidence":-0.1`), http.StatusBadRequest},
		{"sleep_state with unknown value", typed("sleep_state", `,"state":"dozing"`), http.StatusBadRequest},
		{"net_status with text rssi", typed("net_status", `,"rssi":"weak"`), http.StatusBadRequest},
	}
	for _, c := range cases {
		if got := post(t, svc, c.body); got != c.want {
			t.Errorf("%s: status %d, want %d", c.name, got, c.want)
		}
	}
	if got := count(t, svc); got != 1 {
		t.Fatalf("%d events, want 1 (only the accepted case)", got)
	}
}

func TestEveryEventTypeIsAcceptedWithItsField(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)
	extras := map[string]string{
		"heartbeat":   ``,
		"presence":    `,"in_room":false`,
		"motion":      `,"magnitude":0.81`,
		"sleep_state": `,"state":"asleep"`,
		"fall_warn":   `,"confidence":0.92`,
		"net_status":  `,"rssi":-68`,
	}
	seq := 0
	for typ, extra := range extras {
		seq++
		body := fmt.Sprintf(`{"device_id":"dev_0005","room_id":"room_001","type":"%s","ts":"2026-11-15T12:00:00.000Z","seq":%d%s}`, typ, seq, extra)
		if got := post(t, svc, body); got != http.StatusOK {
			t.Errorf("%s: status %d, want 200", typ, got)
		}
	}
	if got := count(t, svc); got != int64(len(extras)) {
		t.Fatalf("%d events, want %d", got, len(extras))
	}
}

// 50 simultaneous copies of one event: all succeed, one row, and exactly one of
// them is reported as the original.
func TestConcurrentIdenticalRequestsStoreOneEvent(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	const senders = 50
	replies := make([]reply, senders)
	var wg sync.WaitGroup
	for i := range replies {
		wg.Add(1)
		go func() {
			defer wg.Done()
			replies[i] = postReply(t, svc, heartbeat)
		}()
	}
	wg.Wait()

	originals := 0
	for i, r := range replies {
		if r.status != http.StatusOK {
			t.Fatalf("sender %d: status %d", i, r.status)
		}
		if !r.duplicate {
			originals++
		}
	}
	if originals != 1 {
		t.Fatalf("%d requests reported as the original, want 1", originals)
	}
	if got := count(t, svc); got != 1 {
		t.Fatalf("%d events, want 1", got)
	}
}

// 20 simultaneous events sharing one identity but each with different content:
// exactly one wins, the rest are visible conflicts.
func TestConcurrentConflictingRequestsKeepOneAndRejectTheRest(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	const senders = 20
	statuses := make([]int, senders)
	var wg sync.WaitGroup
	for i := range statuses {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := fmt.Sprintf(`{"device_id":"dev_0006","room_id":"room_%03d","type":"heartbeat","ts":"2026-11-15T12:00:00.000Z","seq":1}`, i)
			statuses[i] = post(t, svc, body)
		}()
	}
	wg.Wait()

	accepted, conflicts := 0, 0
	for _, status := range statuses {
		switch status {
		case http.StatusOK:
			accepted++
		case http.StatusConflict:
			conflicts++
		}
	}
	if accepted != 1 || conflicts != senders-1 {
		t.Fatalf("accepted %d, conflicts %d; want 1 and %d", accepted, conflicts, senders-1)
	}
	if got := count(t, svc); got != 1 {
		t.Fatalf("%d events, want 1", got)
	}
}

// The event commits but the reply never reaches the sender. The sender's retry
// must succeed as a duplicate and leave one row.
func TestRetryAfterLostResponseIsDuplicate(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	svc.dropNextResponse = true
	if _, err := http.Post(svc.srv.URL+"/events", "application/json", strings.NewReader(heartbeat)); err == nil {
		t.Fatal("expected the first request to fail with a lost response")
	}
	if got := count(t, svc); got != 1 {
		t.Fatalf("after lost response: %d events, want 1 (committed before the reply)", got)
	}
	retry := postReply(t, svc, heartbeat)
	if retry.status != http.StatusOK || !retry.duplicate {
		t.Fatalf("retry: status %d duplicate %v, want 200 and true", retry.status, retry.duplicate)
	}
	if got := count(t, svc); got != 1 {
		t.Fatalf("after retry: %d events, want 1", got)
	}
}

// A device restarts and reuses sequence number 1 for a later event. The
// timestamp differs, so it is a new event and not a conflict.
func TestReusedSequenceNumberWithNewTimestampIsANewEvent(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	later := strings.Replace(heartbeat, "11:59:59.500Z", "11:59:59.900Z", 1)
	first := postReply(t, svc, heartbeat)
	second := postReply(t, svc, later)
	if first.status != http.StatusOK || second.status != http.StatusOK || second.duplicate {
		t.Fatalf("got %+v then %+v, want two new events", first, second)
	}
	if got := count(t, svc); got != 2 {
		t.Fatalf("%d events, want 2", got)
	}
}
