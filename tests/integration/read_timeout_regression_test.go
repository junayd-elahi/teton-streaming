package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestLongPollDatabaseWaitHonorsRequestedDeadline(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	conn, err := adminConn(context.Background(), svc.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	tx, err := conn.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(context.Background(), `LOCK TABLE alarms IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get(svc.srv.URL + "/alarms?after_id=0&wait=0.05")
	if err != nil {
		t.Fatalf("database read exceeded the feed deadline: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("blocked read status %d, want 503", response.StatusCode)
	}
}

func TestLongPollReturnsEmptyPageWhenPeriodicReadHitsLockAtDeadline(t *testing.T) {
	svc := startService(t)
	defer svc.stop()

	conn, err := adminConn(context.Background(), svc.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	lockConn, err := adminConn(context.Background(), svc.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer lockConn.Close(context.Background())

	type result struct {
		status int
		page   alarmPage
		err    error
	}
	resultCh := make(chan result, 1)
	client := &http.Client{Timeout: 3 * time.Second}
	go func() {
		request, err := http.NewRequest(http.MethodGet, svc.srv.URL+"/alarms?after_id=0&wait=1", nil)
		if err != nil {
			resultCh <- result{err: err}
			return
		}
		requestCtx, cancel := context.WithTimeout(request.Context(), 3*time.Second)
		defer cancel()
		response, err := client.Do(request.WithContext(requestCtx))
		if err != nil {
			resultCh <- result{err: err}
			return
		}
		defer response.Body.Close()
		var page alarmPage
		err = json.NewDecoder(response.Body).Decode(&page)
		resultCh <- result{status: response.StatusCode, page: page, err: err}
	}()

	stateCtx, cancelState := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelState()
	waitForFeedActivity := func(description string, condition string) {
		t.Helper()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			var found bool
			err := conn.QueryRow(stateCtx, `
				SELECT EXISTS (
					SELECT 1 FROM pg_stat_activity
					WHERE datname = current_database()
					  AND pid <> pg_backend_pid()
					  AND query LIKE '%FROM alarms WHERE id >%'
					  AND `+condition+`
				)`).Scan(&found)
			if err != nil {
				t.Fatalf("observing %s: %v", description, err)
			}
			if found {
				return
			}
			select {
			case <-stateCtx.Done():
				t.Fatalf("timed out waiting for %s", description)
			case <-ticker.C:
			}
		}
	}

	// An idle backend proves the first empty query completed successfully and
	// the feed entered its notification/poll wait before the table is locked.
	waitForFeedActivity("initial successful feed query", `state = 'idle'`)

	tx, err := lockConn.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(context.Background(), `LOCK TABLE alarms IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}

	// Observe the ticker-triggered follow-up read waiting on the held table
	// lock. The lock stays held while the feed's requested wait expires.
	waitForFeedActivity("periodic feed query waiting on the alarms lock", `state = 'active' AND wait_event_type = 'Lock'`)

	select {
	case got := <-resultCh:
		if got.err != nil {
			t.Fatalf("long poll failed: %v", got.err)
		}
		if got.status != http.StatusOK {
			t.Fatalf("long poll status %d, want 200", got.status)
		}
		if len(got.page.Alarms) != 0 || got.page.NextCursor != 0 {
			t.Fatalf("long poll page %+v, want empty alarms and unchanged cursor 0", got.page)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("long poll did not return by its client deadline")
	}
}
