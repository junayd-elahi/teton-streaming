package integration

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/junayd-elahi/teton-streaming/internal/store"
)

func TestLongPollRecoversACommittedAlarmWithoutNotification(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	type result struct {
		page alarmPage
		err  error
	}
	done := make(chan result, 1)
	go func() {
		client := &http.Client{Timeout: 2 * time.Second}
		response, err := client.Get(svc.srv.URL + "/alarms?after_id=0&wait=10")
		if err != nil {
			done <- result{err: err}
			return
		}
		defer response.Body.Close()
		var page alarmPage
		err = json.NewDecoder(response.Body).Decode(&page)
		done <- result{page: page, err: err}
	}()
	// Observe that the initial empty query has completed before committing a
	// row without the writer's in-process signal.
	conn, err := adminConn(context.Background(), svc.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	deadline := time.Now().Add(time.Second)
	for {
		var queried bool
		err := conn.QueryRow(context.Background(), `
			SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE datname = current_database() AND pid <> pg_backend_pid()
			  AND state = 'idle' AND query LIKE '%FROM alarms WHERE id >%')`).Scan(&queried)
		if err != nil {
			t.Fatal(err)
		}
		if queried {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("feed did not reach its initial wait")
		}
		time.Sleep(5 * time.Millisecond)
	}
	body := []byte(fallAt("dev_unannounced", "room_unannounced", 1, "2026-11-15T11:59:00.000Z", 0.9))
	hash := sha256.Sum256(body)
	committed, err := svc.store.AppendBatch(context.Background(), []store.Event{{
		DeviceID: "dev_unannounced", RoomID: "room_unannounced", Seq: 1, Type: "fall_warn",
		TS: parseTS(t, "2026-11-15T11:59:00.000Z"), ReceivedAt: fixedNow,
		Payload: body, PayloadHash: hash[:],
	}})
	if err != nil || committed.NewAlarms != 1 {
		t.Fatalf("committing alarm: %+v, %v", committed, err)
	}
	got := <-done
	if got.err != nil || len(got.page.Alarms) != 1 || got.page.NextCursor == 0 {
		t.Fatalf("durable feed recovery: %+v, %v", got.page, got.err)
	}
}
