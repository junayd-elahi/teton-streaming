package ingest

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/junayd-elahi/teton-streaming/internal/metrics"
	"github.com/junayd-elahi/teton-streaming/internal/store"
)

func testMetrics() *metrics.Service {
	return metrics.NewService(func() int64 { return 0 }, func() int64 { return 0 }, time.Now)
}

// With no writer running, the queue of one fills and the next submit must fail
// visibly as overloaded instead of waiting forever.
func TestSubmitReportsOverloadWhenQueueStaysFull(t *testing.T) {
	w := NewWriter(nil, Config{QueueSize: 1, PriorityQueueSize: 1, BatchSize: 1, EnqueueTimeout: 20 * time.Millisecond, CommitTimeout: time.Second}, testMetrics())

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
	w := NewWriter(journal, Config{QueueSize: 10, PriorityQueueSize: 10, BatchSize: 10, EnqueueTimeout: time.Second, CommitTimeout: time.Second}, testMetrics())

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

// recordingJournal accepts everything and remembers each batch it was given.
type recordingJournal struct {
	mu      sync.Mutex
	batches [][]store.Event
}

func (j *recordingJournal) AppendBatch(_ context.Context, events []store.Event) (store.BatchResult, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.batches = append(j.batches, append([]store.Event(nil), events...))
	return store.BatchResult{Outcomes: make([]store.Outcome, len(events))}, nil
}

// submitAll submits every event from its own goroutine and returns once all of
// them are sitting in the writer's queues.
func submitAll(t *testing.T, w *Writer, events []store.Event) *sync.WaitGroup {
	t.Helper()
	var wg sync.WaitGroup
	for _, e := range events {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := w.Submit(context.Background(), e); err != nil {
				t.Errorf("submit seq %d: %v", e.Seq, err)
			}
		}()
	}
	for {
		normal, priority := w.QueueDepths()
		if normal+priority == len(events) {
			return &wg
		}
		time.Sleep(time.Millisecond)
	}
}

// Twelve heartbeats are queued, then two fall warnings. With batches of five,
// both falls must be in the first batch although they arrived last.
func TestFallWarningsJumpTheBacklog(t *testing.T) {
	journal := &recordingJournal{}
	w := NewWriter(journal, Config{QueueSize: 20, PriorityQueueSize: 20, BatchSize: 5, EnqueueTimeout: time.Second, CommitTimeout: time.Second}, testMetrics())

	var events []store.Event
	for seq := int64(1); seq <= 12; seq++ {
		events = append(events, store.Event{Seq: seq, Type: "heartbeat"})
	}
	events = append(events, store.Event{Seq: 101, Type: "fall_warn"}, store.Event{Seq: 102, Type: "fall_warn"})
	wg := submitAll(t, w, events)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)
	wg.Wait()

	journal.mu.Lock()
	defer journal.mu.Unlock()
	falls := 0
	for _, e := range journal.batches[0] {
		if e.Type == "fall_warn" {
			falls++
		}
	}
	if falls != 2 {
		t.Fatalf("first batch holds %d fall warnings, want 2 (batch: %+v)", falls, journal.batches[0])
	}
	if len(journal.batches[0]) != 5 {
		t.Fatalf("first batch has %d events, want a full batch of 5", len(journal.batches[0]))
	}
}

// A full normal queue must not cause a fall warning to be refused.
func TestFallWarningIsAcceptedWhileNormalQueueIsFull(t *testing.T) {
	journal := &recordingJournal{}
	w := NewWriter(journal, Config{QueueSize: 1, PriorityQueueSize: 1, BatchSize: 5, EnqueueTimeout: 20 * time.Millisecond, CommitTimeout: time.Second}, testMetrics())

	wg := submitAll(t, w, []store.Event{{Seq: 1, Type: "heartbeat"}})
	if _, err := w.Submit(context.Background(), store.Event{Seq: 2, Type: "heartbeat"}); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("second heartbeat: got %v, want ErrOverloaded", err)
	}
	fall := make(chan error, 1)
	go func() {
		_, err := w.Submit(context.Background(), store.Event{Seq: 3, Type: "fall_warn"})
		fall <- err
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)
	if err := <-fall; err != nil {
		t.Fatalf("fall warning: got %v, want success", err)
	}
	wg.Wait()
}

// stalledJournal never answers until its context ends, like a database that
// has stopped responding.
type stalledJournal struct{}

func (stalledJournal) AppendBatch(ctx context.Context, _ []store.Event) (store.BatchResult, error) {
	<-ctx.Done()
	return store.BatchResult{}, ctx.Err()
}

// A stalled database fails the batch after the commit timeout, and the writer
// keeps serving later events.
func TestStalledCommitFailsAfterTimeoutAndWriterKeepsRunning(t *testing.T) {
	w := NewWriter(stalledJournal{}, Config{QueueSize: 5, PriorityQueueSize: 5, BatchSize: 5, EnqueueTimeout: time.Second, CommitTimeout: 50 * time.Millisecond}, testMetrics())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	for attempt := 1; attempt <= 2; attempt++ {
		started := time.Now()
		_, err := w.Submit(context.Background(), store.Event{Seq: int64(attempt), Type: "heartbeat"})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("attempt %d: got %v, want a deadline error", attempt, err)
		}
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("attempt %d: took %v, want about the 50ms commit timeout", attempt, elapsed)
		}
	}
}
