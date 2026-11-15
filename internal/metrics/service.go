package metrics

import "time"

// Label value sets. They are declared here so call sites cannot invent series.
var (
	Outcomes   = []string{"accepted", "duplicate", "conflict", "invalid", "unstorable", "overloaded", "storage_error"}
	EventTypes = []string{"heartbeat", "presence", "motion", "sleep_state", "fall_warn", "net_status"}
	Queues     = []string{"normal", "priority"}
	Endpoints  = []string{"health", "occupancy", "alarms_snapshot", "alarms_feed"}
)

// Service is the metric catalogue of the streaming backend.
type Service struct {
	Registry *Registry

	EventsReceived *CounterVec // by outcome
	EventsAccepted *CounterVec // by event type

	BatchSize          *Histogram
	BatchCommitSeconds *Histogram
	QueueWaitSeconds   *Histogram
	IngestSeconds      *Histogram

	AlarmsCreated       *Counter
	AlarmVisibleSeconds *Histogram
	BatchSplits         *Counter

	FeedWaiters   *Gauge
	ReadRequests  *CounterVec // by endpoint
	ReadErrors    *Counter
	StartTimeUnix *Gauge
}

// NewService registers the whole catalogue. queueDepth and priorityDepth are
// read at scrape time. now supplies the process start time.
func NewService(queueDepth, priorityDepth func() int64, now func() time.Time) *Service {
	r := NewRegistry()
	s := &Service{Registry: r}

	s.EventsReceived = r.NewCounterVec("teton_events_received_total",
		"Events received on POST /events, by outcome.", "outcome", Outcomes)
	s.EventsAccepted = r.NewCounterVec("teton_events_accepted_total",
		"Events newly stored (not duplicates), by event type.", "type", EventTypes)

	r.NewGaugeFuncVec("teton_ingest_queue_depth",
		"Events waiting in an ingest queue right now. The priority queue carries fall_warn events.",
		"queue", map[string]func() int64{"normal": queueDepth, "priority": priorityDepth}, Queues)

	s.BatchSize = r.NewHistogram("teton_batch_size_events",
		"Number of events in each committed batch.",
		[]float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000})
	s.BatchCommitSeconds = r.NewHistogram("teton_batch_commit_duration_seconds",
		"Time to write and commit one batch to the database.",
		[]float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5})
	s.QueueWaitSeconds = r.NewHistogram("teton_queue_wait_seconds",
		"Time an event waited in the queue before its batch started.",
		[]float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5})
	s.IngestSeconds = r.NewHistogram("teton_ingest_request_duration_seconds",
		"Time from receiving a POST /events request to sending the response.",
		[]float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5})

	s.AlarmsCreated = r.NewCounter("teton_alarms_created_total",
		"Fall alarms created (duplicate fall_warn copies do not count).")
	s.AlarmVisibleSeconds = r.NewHistogram("teton_alarm_visible_latency_seconds",
		"Time from receiving a fall_warn to its alarm being committed and readable.",
		[]float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5, 0.75, 1, 1.5, 2.5, 5})
	s.BatchSplits = r.NewCounter("teton_batch_splits_total",
		"Batches split and retried in parts because the database refused an event.")

	s.FeedWaiters = r.NewGauge("teton_alarm_feed_waiting_requests",
		"Alarm feed long-poll requests currently waiting for a new alarm.")
	s.ReadRequests = r.NewCounterVec("teton_read_requests_total",
		"Read requests received, by endpoint.", "endpoint", Endpoints)
	s.ReadErrors = r.NewCounter("teton_read_storage_errors_total",
		"Read requests that failed because the database returned an error.")

	s.StartTimeUnix = r.NewGauge("teton_process_start_time_seconds",
		"Unix time at which this process started.")
	s.StartTimeUnix.Set(now().Unix())
	return s
}
