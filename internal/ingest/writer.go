// Package ingest serialises journal writes through one bounded queue and one
// batching writer, so journal order equals commit order.
package ingest

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/junayd-elahi/teton-streaming/internal/store"
)

// ErrOverloaded means the queue stayed full for the whole enqueue timeout.
// The event was not accepted and the sender should retry.
var ErrOverloaded = errors.New("ingest queue full")

// Journal is the storage the writer commits batches to.
type Journal interface {
	AppendBatch(ctx context.Context, events []store.Event) (store.BatchResult, error)
}

type Config struct {
	QueueSize      int           // events waiting for the writer
	BatchSize      int           // maximum events per transaction
	EnqueueTimeout time.Duration // how long a request waits for queue space
}

type result struct {
	outcome store.Outcome
	err     error
}

type request struct {
	event store.Event
	done  chan result
}

type Writer struct {
	journal Journal
	cfg     Config
	queue   chan request

	alarmMu     sync.Mutex
	alarmSignal chan struct{} // closed and replaced whenever a batch commits new alarms
}

func NewWriter(journal Journal, cfg Config) *Writer {
	if cfg.QueueSize < 1 || cfg.BatchSize < 1 || cfg.EnqueueTimeout <= 0 {
		panic("ingest: QueueSize, BatchSize and EnqueueTimeout must be positive")
	}
	return &Writer{
		journal:     journal,
		cfg:         cfg,
		queue:       make(chan request, cfg.QueueSize),
		alarmSignal: make(chan struct{}),
	}
}

// NewAlarms returns a channel that is closed the next time a batch commits at
// least one new alarm. Take the channel before querying for alarms, so an
// alarm committed between the query and the wait is not missed.
func (w *Writer) NewAlarms() <-chan struct{} {
	w.alarmMu.Lock()
	defer w.alarmMu.Unlock()
	return w.alarmSignal
}

func (w *Writer) announceAlarms() {
	w.alarmMu.Lock()
	defer w.alarmMu.Unlock()
	close(w.alarmSignal)
	w.alarmSignal = make(chan struct{})
}

// Submit queues the event and waits until its batch is committed.
// If ctx ends after the event was queued, the event may still be committed;
// retrying it is safe because the journal treats it as a duplicate.
func (w *Writer) Submit(ctx context.Context, e store.Event) (store.Outcome, error) {
	req := request{event: e, done: make(chan result, 1)}
	timer := time.NewTimer(w.cfg.EnqueueTimeout)
	defer timer.Stop()
	select {
	case w.queue <- req:
	case <-timer.C:
		return 0, ErrOverloaded
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	select {
	case r := <-req.done:
		return r.outcome, r.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// Run commits queued events until ctx ends. It takes one event, adds whatever
// else is already waiting up to BatchSize, and commits them together. Batches
// therefore grow on their own under load and stay at one event when idle.
func (w *Writer) Run(ctx context.Context) {
	batch := make([]request, 0, w.cfg.BatchSize)
	for {
		batch = batch[:0]
		select {
		case req := <-w.queue:
			batch = append(batch, req)
		case <-ctx.Done():
			return
		}
		batch = w.drain(batch)
		w.commit(ctx, batch)
	}
}

func (w *Writer) drain(batch []request) []request {
	for len(batch) < w.cfg.BatchSize {
		select {
		case req := <-w.queue:
			batch = append(batch, req)
		default:
			return batch
		}
	}
	return batch
}

func (w *Writer) commit(ctx context.Context, batch []request) {
	events := make([]store.Event, len(batch))
	for i, req := range batch {
		events[i] = req.event
	}
	committed, err := w.journal.AppendBatch(ctx, events)
	if err != nil && len(batch) > 1 && store.IsUnstorable(err) {
		// One event in the batch holds data the database refuses. Commit each
		// event on its own so only that event fails and the rest are not
		// rejected along with it.
		for _, req := range batch {
			w.commit(ctx, []request{req})
		}
		return
	}
	for i, req := range batch {
		if err != nil {
			req.done <- result{err: err}
			continue
		}
		req.done <- result{outcome: committed.Outcomes[i]}
	}
	if committed.NewAlarms > 0 {
		w.announceAlarms()
	}
}
