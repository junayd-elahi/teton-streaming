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

// A batch holding one event the database refuses still commits the others.
// This bypasses request validation to prove the writer isolates the bad event
// by itself.
func TestWriterIsolatesAnEventTheDatabaseRefuses(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	reset(t, svc)

	good := journalEvent(1, `{"a":1}`)
	bad := journalEvent(2, `{"a":1e999999}`)
	_, err := svc.store.AppendBatch(context.Background(), []store.Event{good, bad})
	if !store.IsUnstorable(err) {
		t.Fatalf("batch with a refused event: got %v, want an unstorable error", err)
	}
	if got := count(t, svc); got != 0 {
		t.Fatalf("after failed batch: %d events, want 0", got)
	}

	type submitted struct {
		outcome store.Outcome
		err     error
	}
	results := make(chan submitted, 2)
	block := make(chan struct{})
	go func() { <-block; o, err := svc.writer.Submit(context.Background(), good); results <- submitted{o, err} }()
	go func() {
		<-block
		_, err := svc.writer.Submit(context.Background(), bad)
		results <- submitted{err: err}
	}()
	close(block)
	var accepted, refused int
	for i := 0; i < 2; i++ {
		r := <-results
		switch {
		case r.err == nil && r.outcome == store.Inserted:
			accepted++
		case store.IsUnstorable(r.err):
			refused++
		}
	}
	if accepted != 1 || refused != 1 {
		t.Fatalf("accepted %d, refused %d; want 1 and 1", accepted, refused)
	}
	if got := count(t, svc); got != 1 {
		t.Fatalf("%d events, want 1", got)
	}
}
