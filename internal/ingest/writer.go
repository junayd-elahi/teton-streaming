// Package ingest moves accepted events into the journal through bounded
// queues and a small set of batching writers.
package ingest

import (
	"context"
	"errors"
	"hash/fnv"
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
	Writers           int           // parallel writers for ordinary events
	QueueSize         int           // ordinary events waiting, per writer
	PriorityQueueSize int           // fall warnings waiting for their writer
	BatchSize         int           // maximum events per transaction
	Linger            time.Duration // how long a batch of two or more waits to fill; a lone request never waits; 0 = never wait
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

// Writer runs Writers lanes for ordinary events and one lane for fall warnings.
//
// A device always maps to the same ordinary lane, so its ordinary events keep
// lane enqueue order. Concurrent requests and the separate fall lane do not
// establish global event-time order. Several lanes let PostgreSQL work on
// several batches at once.
//
// Fall warnings never share a lane with anything else. They are committed by
// their own writer without ordinary batching delay. Ordinary events cannot
// consume their queue capacity, but database contention and queued falls can
// still delay them. In the supported single-instance deployment this is the
// only writer that creates alarms, keeping their ids visible in increasing
// order for the feed cursor.
type Writer struct {
	journal  Journal
	cfg      Config
	metrics  *metrics.Service
	lanes    []chan request
	priority chan request

	alarmMu     sync.Mutex
	alarmSignal chan struct{} // closed and replaced whenever a batch commits new alarms
}

func NewWriter(journal Journal, cfg Config, m *metrics.Service) *Writer {
	if cfg.Writers < 1 || cfg.QueueSize < 1 || cfg.PriorityQueueSize < 1 || cfg.BatchSize < 1 ||
		cfg.EnqueueTimeout <= 0 || cfg.CommitTimeout <= 0 || cfg.Linger < 0 {
		panic("ingest: Linger must not be negative and every other Config value must be positive")
	}
	lanes := make([]chan request, cfg.Writers)
	for i := range lanes {
		lanes[i] = make(chan request, cfg.QueueSize)
	}
	return &Writer{
		journal:     journal,
		cfg:         cfg,
		metrics:     m,
		lanes:       lanes,
		priority:    make(chan request, cfg.PriorityQueueSize),
		alarmSignal: make(chan struct{}),
	}
}

// QueueDepths returns how many events are waiting: ordinary events across all
// lanes, and fall warnings.
func (w *Writer) QueueDepths() (normal, priority int) {
	for _, lane := range w.lanes {
		normal += len(lane)
	}
	return normal, len(w.priority)
}

// laneFor picks the lane an event waits in.
func (w *Writer) laneFor(e store.Event) chan request {
	if e.Type == priorityType {
		return w.priority
	}
	h := fnv.New32a()
	h.Write([]byte(e.DeviceID))
	return w.lanes[h.Sum32()%uint32(len(w.lanes))]
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
	queue := w.laneFor(e)
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

// Run starts one writer per lane and returns when ctx ends. An ordinary lane
// lingers to fill its batch only when more than one request is already
// waiting, which shows that senders are concurrent; a lone request is
// committed at once. The fall-warning lane never waits.
func (w *Writer) Run(ctx context.Context) {
	var wg sync.WaitGroup
	start := func(lane <-chan request, linger time.Duration) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.runLane(ctx, lane, linger)
		}()
	}
	start(w.priority, 0)
	for _, lane := range w.lanes {
		start(lane, w.cfg.Linger)
	}
	wg.Wait()
}

// runLane commits one lane's events until ctx ends. It waits for one event,
// gathers more (see gather) and commits them together.
//
// The linger exists because a commit has a fixed cost (one statement, one
// disk flush) whatever the batch size. Without it a fast database is handed
// many tiny batches and spends its time on that fixed cost; a few
// milliseconds of waiting turns them into fewer, fuller ones. It applies only
// when requests are already queued together, because a client that sends one
// request and waits for the reply can never supply a second one, and would
// otherwise pay the whole linger on every request. Under heavy load requests
// pile up while a commit is running, so the next batch starts with more than
// one and lingering still fills batches.
func (w *Writer) runLane(ctx context.Context, lane <-chan request, linger time.Duration) {
	batch := make([]request, 0, w.cfg.BatchSize)
	for {
		batch = batch[:0]
		select {
		case req := <-lane:
			batch = append(batch, req)
		case <-ctx.Done():
			return
		}
		batch = w.gather(batch, lane, linger)
		w.observeQueueWait(batch)
		w.commit(ctx, batch)
	}
}

// gather completes a batch that holds its first request. It takes whatever is
// already waiting. If that is still a single request it stops there: nobody
// else is sending, so waiting would only delay it. If there are several and
// the batch is not full, it waits up to linger for more (until the batch is
// full or the timer fires) and then takes anything else already waiting. A
// linger of 0 never waits.
func (w *Writer) gather(batch []request, lane <-chan request, linger time.Duration) []request {
	batch = drain(batch, lane, w.cfg.BatchSize)
	if len(batch) == 1 || len(batch) >= w.cfg.BatchSize || linger <= 0 {
		return batch
	}
	timer := time.NewTimer(linger)
	defer timer.Stop()
	for len(batch) < w.cfg.BatchSize {
		select {
		case req := <-lane:
			batch = append(batch, req)
		case <-timer.C:
			return drain(batch, lane, w.cfg.BatchSize)
		}
	}
	return batch
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
