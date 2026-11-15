package ingest

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/junayd-elahi/teton-streaming/internal/store"
)

// With no writer running, the queue of one fills and the next submit must fail
// visibly as overloaded instead of waiting forever.
func TestSubmitReportsOverloadWhenQueueStaysFull(t *testing.T) {
	w := NewWriter(nil, Config{QueueSize: 1, BatchSize: 1, EnqueueTimeout: 20 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		_, err := w.Submit(ctx, store.Event{})
		first <- err
	}()
	for len(w.queue) == 0 {
		time.Sleep(time.Millisecond)
	}

	if _, err := w.Submit(context.Background(), store.Event{}); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("second submit: got %v, want ErrOverloaded", err)
	}
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("first submit: got %v, want context.Canceled", err)
	}
}

// refusingJournal rejects any batch containing the event with Seq 13, the way
// PostgreSQL rejects a whole statement over one bad value.
type refusingJournal struct {
	batchSizes []int
}

func (j *refusingJournal) AppendBatch(_ context.Context, events []store.Event) (store.BatchResult, error) {
	j.batchSizes = append(j.batchSizes, len(events))
	outcomes := make([]store.Outcome, len(events))
	for _, e := range events {
		if e.Seq == 13 {
			return store.BatchResult{}, &pgconn.PgError{Code: "22003", Message: "value overflows numeric format"}
		}
	}
	return store.BatchResult{Outcomes: outcomes}, nil
}

// Five events wait in the queue together, one of them unstorable. The writer
// must refuse only that one and commit the other four.
func TestWriterRefusesOnlyTheUnstorableEventInABatch(t *testing.T) {
	journal := &refusingJournal{}
	w := NewWriter(journal, Config{QueueSize: 10, BatchSize: 10, EnqueueTimeout: time.Second})

	seqs := []int64{11, 12, 13, 14, 15}
	errs := make(map[int64]error)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, seq := range seqs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := w.Submit(context.Background(), store.Event{Seq: seq})
			mu.Lock()
			errs[seq] = err
			mu.Unlock()
		}()
	}
	for len(w.queue) < len(seqs) {
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)
	wg.Wait()

	for _, seq := range seqs {
		if seq == 13 {
			if !store.IsUnstorable(errs[seq]) {
				t.Errorf("event 13: got %v, want an unstorable error", errs[seq])
			}
			continue
		}
		if errs[seq] != nil {
			t.Errorf("event %d: got %v, want success", seq, errs[seq])
		}
	}
	// One failed batch of five, then five single-event commits.
	if want := []int{5, 1, 1, 1, 1, 1}; !slices.Equal(journal.batchSizes, want) {
		t.Errorf("batch sizes %v, want %v", journal.batchSizes, want)
	}
}
