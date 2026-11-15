package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestFallCommittedBeforeLostReplyIsTrackedAfterDuplicateRetry(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		mu.Lock()
		bodies = append(bodies, string(body))
		first := len(bodies) == 1
		mu.Unlock()
		if first {
			// The service committed this fall, then the connection dropped
			// before the sender received its durable acknowledgement.
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"ok":true,"duplicate":true}`)
	}))
	defer server.Close()

	falls := newFallTracker()
	stats := &senderStats{}
	s := sender{cfg: config{Target: server.URL, Retries: 1}, client: server.Client(), falls: falls, stats: stats}
	s.send(event{body: []byte(`{"device_id":"run_1","seq":1,"type":"fall_warn"}`), isFall: true, deviceID: "run_1", seq: 1})

	mu.Lock()
	if len(bodies) != 2 || bodies[0] != bodies[1] {
		t.Fatalf("request bodies %q, want two identical attempts", bodies)
	}
	mu.Unlock()
	if stats.Retried != 1 || stats.Duplicate != 1 || stats.Accepted != 0 || stats.TransportErr != 0 {
		t.Fatalf("outcomes %+v, want one retry and one duplicate acknowledgement", stats)
	}
	if got := falls.missing(); got != 1 {
		t.Fatalf("missing falls %d, want 1 until the acknowledged fall reaches the feed", got)
	}
	falls.delivered(eventID("run_1", 1), time.Now())
	if got := falls.missing(); got != 0 {
		t.Fatalf("missing falls after delivery %d, want 0", got)
	}
}
