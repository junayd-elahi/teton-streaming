// Integration tests for the durable ingest path. They need a real PostgreSQL:
//
//	DATABASE_URL=postgres://... go test ./tests/integration/
package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/junayd-elahi/teton-streaming/internal/api"
	"github.com/junayd-elahi/teton-streaming/internal/store"
	"github.com/junayd-elahi/teton-streaming/migrations"
)

var fixedNow = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

const heartbeat = `{"device_id":"dev_0001","room_id":"room_001","type":"heartbeat","ts":"2026-11-15T11:59:59.500Z","seq":1}`

// startService opens a fresh store and HTTP server, as a process start would.
func startService(t *testing.T) (*store.Store, *httptest.Server) {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Fatal("DATABASE_URL is required for integration tests")
	}
	ctx := context.Background()
	st, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx, migrations.Files); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(api.New(st, func() time.Time { return fixedNow }).Handler())
	return st, srv
}

func stopService(st *store.Store, srv *httptest.Server) {
	srv.Close()
	st.Close()
}

func post(t *testing.T, srv *httptest.Server, body string) int {
	t.Helper()
	resp, err := http.Post(srv.URL+"/events", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func count(t *testing.T, st *store.Store) int64 {
	t.Helper()
	n, err := st.CountEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func reset(t *testing.T, st *store.Store) {
	t.Helper()
	if err := st.Reset(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestEventSurvivesRestartAndReplayHasNoEffect(t *testing.T) {
	st, srv := startService(t)
	reset(t, st)
	if got := post(t, srv, heartbeat); got != http.StatusOK {
		t.Fatalf("first post: status %d", got)
	}
	stopService(st, srv)

	st, srv = startService(t)
	defer stopService(st, srv)
	if got := count(t, st); got != 1 {
		t.Fatalf("after restart: %d events, want 1", got)
	}
	for i := 0; i < 100; i++ {
		if got := post(t, srv, heartbeat); got != http.StatusOK {
			t.Fatalf("replay %d: status %d", i, got)
		}
	}
	if got := count(t, st); got != 1 {
		t.Fatalf("after 100 replays: %d events, want 1", got)
	}
}

func TestReplayWithReorderedKeysIsSameEvent(t *testing.T) {
	st, srv := startService(t)
	defer stopService(st, srv)
	reset(t, st)
	reordered := `{"seq":1,"ts":"2026-11-15T11:59:59.500Z","type":"heartbeat","room_id":"room_001","device_id":"dev_0001"}`
	if got := post(t, srv, heartbeat); got != http.StatusOK {
		t.Fatalf("first post: status %d", got)
	}
	if got := post(t, srv, reordered); got != http.StatusOK {
		t.Fatalf("reordered replay: status %d", got)
	}
	if got := count(t, st); got != 1 {
		t.Fatalf("%d events, want 1", got)
	}
}

func TestSameIdentityDifferentPayloadIsConflict(t *testing.T) {
	st, srv := startService(t)
	defer stopService(st, srv)
	reset(t, st)
	changed := strings.Replace(heartbeat, "room_001", "room_002", 1)
	if got := post(t, srv, heartbeat); got != http.StatusOK {
		t.Fatalf("first post: status %d", got)
	}
	if got := post(t, srv, changed); got != http.StatusConflict {
		t.Fatalf("changed payload: status %d, want 409", got)
	}
	if got := count(t, st); got != 1 {
		t.Fatalf("%d events, want 1", got)
	}
}

func TestValidation(t *testing.T) {
	st, srv := startService(t)
	defer stopService(st, srv)
	reset(t, st)

	event := func(ts string) string {
		return `{"device_id":"dev_0002","room_id":"room_001","type":"heartbeat","ts":"` + ts + `","seq":1}`
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
	}
	for _, c := range cases {
		if got := post(t, srv, c.body); got != c.want {
			t.Errorf("%s: status %d, want %d", c.name, got, c.want)
		}
	}
	if got := count(t, st); got != 1 {
		t.Fatalf("%d events, want 1 (only the accepted case)", got)
	}
}
