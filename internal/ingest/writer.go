// Package ingest serialises journal writes through bounded queues and one
// batching writer, so journal order equals commit order.
package ingest

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/junayd-elahi/teton-streaming/internal/metrics"
	"github.com/junayd-elahi/teton-streaming/internal/store"
)

// ErrOverloaded means the queue stayed full for the whole enqueue timeout.
// The event was not accepted and the sender should retry.
var ErrOverloaded = errors.New("ingest queue full")

// priorityType is the event type that must not wait behind the others.
const priorityType = "fall_warn"

// Journal is the storage the writer commits batches to.
type Journal interface {
	AppendBatch(ctx context.Context, events []store.Event) (store.BatchResult, error)
}

type Config struct {
	QueueSize         int           // ordinary events waiting for the writer
	PriorityQueueSize int           // fall warnings waiting for the writer
	BatchSize         int           // maximum events per transaction
	EnqueueTimeout    time.Duration // how long a request waits for queue space
	CommitTimeout     time.Duration // how long one batch may take before it fails
}

type result struct {
	outcome store.Outcome
	err     error
}

type request struct {
	event  store.Event
	queued time.Time
	done   chan result
}

// Writer owns two queues. Fall warnings go to the priority queue, everything
// else to the normal queue. Each batch takes every waiting fall warning first
// and only then fills up with ordinary events, so under a burst a fall waits
// for at most the batch already being committed, never behind the backlog.
// A full normal queue also cannot cause a fall warning to be refused.
type Writer struct {
	journal  Journal
	cfg      Config
	metrics  *metrics.Service
	queue    chan request
	priority chan request

	alarmMu     sync.Mutex
	alarmSignal chan struct{} // closed and replaced whenever a batch commits new alarms
}

func NewWriter(journal Journal, cfg Config, m *metrics.Service) *Writer {
	if cfg.QueueSize < 1 || cfg.PriorityQueueSize < 1 || cfg.BatchSize < 1 ||
		cfg.EnqueueTimeout <= 0 || cfg.CommitTimeout <= 0 {
		panic("ingest: every Config value must be positive")
	}
	return &Writer{
		journal:     journal,
		cfg:         cfg,
		metrics:     m,
		queue:       make(chan request, cfg.QueueSize),
		priority:    make(chan request, cfg.PriorityQueueSize),
		alarmSignal: make(chan struct{}),
	}
}

// QueueDepths returns how many events are waiting in each queue.
func (w *Writer) QueueDepths() (normal, priority int) {
	return len(w.queue), len(w.priority)
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
	req := request{event: e, queued: time.Now(), done: make(chan result, 1)}
	queue := w.queue
	if e.Type == priorityType {
		queue = w.priority
	}
	timer := time.NewTimer(w.cfg.EnqueueTimeout)
	defer timer.Stop()
	select {
	case queue <- req:
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

// Run commits queued events until ctx ends. It waits for one event, adds
// whatever else is already waiting up to BatchSize, and commits them together.
// Batches therefore grow on their own under load and stay at one event when idle.
func (w *Writer) Run(ctx context.Context) {
	batch := make([]request, 0, w.cfg.BatchSize)
	for {
		batch = batch[:0]
		select {
		case req := <-w.priority:
			batch = append(batch, req)
		case req := <-w.queue:
			batch = append(batch, req)
		case <-ctx.Done():
			return
		}
		batch = drain(batch, w.priority, w.cfg.BatchSize)
		batch = drain(batch, w.queue, w.cfg.BatchSize)
		w.observeQueueWait(batch)
		w.commit(ctx, batch)
	}
}

// drain appends requests already waiting in queue until the batch is full.
func drain(batch []request, queue <-chan request, limit int) []request {
	for len(batch) < limit {
		select {
		case req := <-queue:
			batch = append(batch, req)
		default:
			return batch
		}
	}
	return batch
}

func (w *Writer) observeQueueWait(batch []request) {
	now := time.Now()
	for _, req := range batch {
		w.metrics.QueueWaitSeconds.Observe(now.Sub(req.queued).Seconds())
	}
}

func (w *Writer) commit(ctx context.Context, batch []request) {
	events := make([]store.Event, len(batch))
	for i, req := range batch {
		events[i] = req.event
	}
	// A stalled database must fail the batch, not stall the writer forever:
	// the senders get a retryable error and the next batch gets a fresh try.
	commitCtx, cancel := context.WithTimeout(ctx, w.cfg.CommitTimeout)
	started := time.Now()
	committed, err := w.journal.AppendBatch(commitCtx, events)
	cancel()
	w.metrics.BatchCommitSeconds.Observe(time.Since(started).Seconds())
	w.metrics.BatchSize.Observe(float64(len(batch)))
	if err != nil && len(batch) > 1 && store.IsUnstorable(err) {
		w.metrics.BatchSplits.Inc()
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
		if req.event.Type == priorityType && committed.Outcomes[i] == store.Inserted {
			w.metrics.AlarmVisibleSeconds.Observe(time.Since(req.queued).Seconds())
		}
	}
	if committed.NewAlarms > 0 {
		w.metrics.AlarmsCreated.Add(uint64(committed.NewAlarms))
		w.announceAlarms()
	}
}
