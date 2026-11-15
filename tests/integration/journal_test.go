package integration

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/junayd-elahi/teton-streaming/internal/store"
)

func journalEvent(seq int64, payload string) store.Event {
	hash := sha256.Sum256([]byte(payload))
	return store.Event{
		DeviceID: "dev_0100", Seq: seq, RoomID: "room_001", Type: "heartbeat",
		TS: fixedNow, Payload: []byte(payload), PayloadHash: hash[:], ReceivedAt: fixedNow,
	}
}

// One batch holding a new event, a copy of it, a conflicting reuse of its
// identity, and a second new event; then the same batch again.
func TestAppendBatchClassifiesEachEvent(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	batch := []store.Event{
		journalEvent(1, `{"a":1}`),
		journalEvent(1, `{"a":1}`),
		journalEvent(1, `{"a":2}`),
		journalEvent(2, `{"a":1}`),
	}
	firstResult, err := svc.store.AppendBatch(context.Background(), batch)
	if err != nil {
		t.Fatal(err)
	}
	first := firstResult.Outcomes
	wantFirst := []store.Outcome{store.Inserted, store.Duplicate, store.Conflict, store.Inserted}
	for i := range wantFirst {
		if first[i] != wantFirst[i] {
			t.Errorf("first batch, event %d: outcome %d, want %d", i, first[i], wantFirst[i])
		}
	}

	secondResult, err := svc.store.AppendBatch(context.Background(), batch)
	if err != nil {
		t.Fatal(err)
	}
	second := secondResult.Outcomes
	wantSecond := []store.Outcome{store.Duplicate, store.Duplicate, store.Conflict, store.Duplicate}
	for i := range wantSecond {
		if second[i] != wantSecond[i] {
			t.Errorf("replayed batch, event %d: outcome %d, want %d", i, second[i], wantSecond[i])
		}
	}
	if got := count(t, svc); got != 2 {
		t.Fatalf("%d events, want 2", got)
	}
}
