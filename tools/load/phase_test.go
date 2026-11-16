package main

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"
)

type replyTransport func(*http.Request) (*http.Response, error)

func (f replyTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func responseClient(status int) *http.Client {
	return &http.Client{Transport: replyTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(`{"duplicate":false}`)), Header: make(http.Header)}, nil
	})}
}

func TestBoundedOfferPreservesLogicalDeadlinesAndCountsDrops(t *testing.T) {
	cfg := config{Rate: 10000, Duration: 5 * time.Millisecond, Senders: 2}
	start := time.Now().Add(-cfg.Duration)
	tokens, dropped := offer(cfg, start)
	if got := dropped(); got != 48 {
		t.Fatalf("dropped=%d want48", got)
	}
	var deadlines []time.Time
	for a := range tokens {
		deadlines = append(deadlines, a.at)
	}
	if len(deadlines) != 2 || deadlines[0] != start || deadlines[1] != start.Add(100*time.Microsecond) {
		t.Fatalf("unexpected bounded arrivals: %v", deadlines)
	}
}

func TestStrictWindowExpiresWhileDefaultReportsLateStarts(t *testing.T) {
	for _, strict := range []bool{false, true} {
		cfg := config{Rate: 1, Duration: time.Second, Senders: 1, Devices: 1, StrictOfferWindow: strict}
		start := time.Now().Add(-2 * time.Second)
		tokens := make(chan arrival, 1)
		tokens <- arrival{at: start}
		close(tokens)
		stats := &senderStats{}
		s := sender{cfg: cfg, client: responseClient(200), stats: stats, falls: newFallTracker()}
		s.run(tokens, start)
		r := buildReport(cfg, start, time.Since(start), []*senderStats{stats}, 0, s.falls)
		if !r.Reconciles {
			t.Fatalf("strict=%v report does not reconcile: %+v", strict, r)
		}
		if strict {
			if r.Expired != 1 || r.NotSent != 1 || r.Sent != 0 {
				t.Fatalf("strict report %+v", r)
			}
		} else if r.Sent != 1 || r.StartsAfterOffer != 1 || r.DurableACKsAfterOffer != 1 || r.NotSent != 0 {
			t.Fatalf("bounded-drain report %+v", r)
		}
	}
}

func TestLateBaselineCompletionStaysInBaselineCohort(t *testing.T) {
	cfg := config{Rate: 2, Duration: 10 * time.Second, BurstRate: 4, BurstStart: 5 * time.Second, BurstDuration: 2 * time.Second, Senders: 1}
	start := time.Now().Add(-6 * time.Second)
	stats := &senderStats{}
	s := sender{cfg: cfg, client: responseClient(200), stats: stats, falls: newFallTracker(), started: start}
	s.send(event{body: []byte(`{}`), scheduledAt: start.Add(4 * time.Second), generatedAt: time.Now()})
	r := buildReport(cfg, start, 6*time.Second, []*senderStats{stats}, 23, s.falls)
	base, burst := r.Phases[0], r.Phases[1]
	if !r.Reconciles || base.Sent != 1 || base.Accepted != 1 || base.StartsAfterPhase != 1 || base.AcceptedAfterPhase != 1 || burst.Sent != 0 || burst.ActualStarts != 1 || burst.ActualDurableACKs != 1 {
		t.Fatalf("cohorts lost across phase boundary: %+v", r.Phases)
	}
	if base.SendDelay.P95 < 1900 || base.ScheduleACK.P95 < 1900 || base.GenerationACK.P95 >= 1000 {
		t.Fatalf("schedule delay was hidden: %+v", base)
	}
}

func TestAllEventOutcomesReconcileWithinPhase(t *testing.T) {
	cfg := config{Rate: 7, Duration: time.Second, Senders: 1}
	stats := &senderStats{}
	statuses := []int{200, 200, 400, 409, 503, 500, 0}
	index := 0
	client := &http.Client{Transport: replyTransport(func(*http.Request) (*http.Response, error) {
		status := statuses[index]
		index++
		if status == 0 {
			return nil, errors.New("deliberate transport error")
		}
		body := `{"duplicate":false}`
		if index == 2 {
			body = `{"duplicate":true}`
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	start := time.Now()
	s := sender{cfg: cfg, client: client, stats: stats, falls: newFallTracker(), started: start}
	for range statuses {
		s.send(event{body: []byte(`{}`), scheduledAt: start})
	}
	r := buildReport(cfg, start, time.Second, []*senderStats{stats}, 0, s.falls)
	p := r.Phases[0]
	if !r.Reconciles || !p.Reconciles || r.Sent != 7 || p.Accepted != 1 || p.Duplicate != 1 || p.Invalid != 1 || p.Conflict != 1 || p.Overloaded != 1 || p.OtherStatus != 1 || p.TransportErr != 1 || p.ScheduleACK.Count != 2 {
		t.Fatalf("all-outcome accounting failed: %+v", p)
	}
}

func TestBurstFallP95CannotHideBehindAggregate(t *testing.T) {
	r := report{Config: config{MaxFallP95MS: 1000}, Reconciles: true, FallLatency: percentiles{Count: 2000, P95: 10}, Phases: []phaseReport{{Name: "burst", FallPostLatency: percentiles{Count: 1000, P95: 2000}}}}
	r.evaluateValidation()
	if r.ValidationPassed {
		t.Fatal("failing burst must not pass with a healthy aggregate p95")
	}
}

func TestScheduledOffsetsFollowBothRateBoundaries(t *testing.T) {
	cfg := config{Rate: 2, Duration: 10 * time.Second, BurstRate: 4, BurstStart: 5 * time.Second, BurstDuration: 2 * time.Second}
	for n, want := range map[int64]time.Duration{0: 0, 9: 4500 * time.Millisecond, 10: 5 * time.Second, 17: 6750 * time.Millisecond, 18: 7 * time.Second, 23: 9500 * time.Millisecond} {
		if got := scheduledOffset(cfg, n); got != want {
			t.Fatalf("arrival%d offset=%v want%v", n, got, want)
		}
	}
}

func TestZeroBasedFirstAndLastArrivalsAreReleasedBeforeTheirWindowCloses(t *testing.T) {
	cfg := config{Rate: 50000, Duration: 30 * time.Second, Senders: 2000}
	start := time.Unix(0, 0)
	ch := make(chan arrival, cfg.Senders)
	next, missed := dispatchArrivals(cfg, start, start, ch, 0)
	if next != 50 || missed != 0 {
		t.Fatalf("initial bounded prefetch next=%d missed=%d", next, missed)
	}
	first := <-ch
	if first.at != start {
		t.Fatalf("first arrival=%v want start", first.at)
	}
	for len(ch) > 0 {
		<-ch
	}
	total := int64(scheduled(cfg, cfg.Duration))
	next, missed = dispatchArrivals(cfg, start, start.Add(cfg.Duration-offerLookahead), ch, total-50)
	if next != total || missed != 0 {
		t.Fatalf("tail not available before stop next=%d/%d missed=%d", next, total, missed)
	}
	var last arrival
	for len(ch) > 0 {
		last = <-ch
	}
	if want := start.Add(cfg.Duration - 20*time.Microsecond); last.at != want {
		t.Fatalf("last arrival=%v want%v", last.at, want)
	}
}

func TestPrefetchDoesNotDropFutureArrivalsWhenQueueIsFull(t *testing.T) {
	cfg := config{Rate: 10000, Duration: time.Second, Senders: 2}
	start := time.Unix(0, 0)
	ch := make(chan arrival, cfg.Senders)
	next, missed := dispatchArrivals(cfg, start, start, ch, 0)
	if next != 2 || missed != 0 {
		t.Fatalf("future slot prematurely dropped next=%d missed=%d", next, missed)
	}
	<-ch
	next, missed = dispatchArrivals(cfg, start, start.Add(100*time.Microsecond), ch, next)
	if next != 3 || missed != 0 {
		t.Fatalf("future slot not retained next=%d missed=%d", next, missed)
	}
}

func TestPrefetchedArrivalNeverStartsItsPOSTEarly(t *testing.T) {
	cfg := config{Rate: 1, Duration: time.Second, Senders: 1, Devices: 1}
	start := time.Now()
	deadline := start.Add(5 * time.Millisecond)
	tokens := make(chan arrival, 1)
	tokens <- arrival{at: deadline}
	close(tokens)
	var attempted time.Time
	client := &http.Client{Transport: replyTransport(func(*http.Request) (*http.Response, error) {
		attempted = time.Now()
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"duplicate":false}`)), Header: make(http.Header)}, nil
	})}
	s := sender{cfg: cfg, client: client, stats: &senderStats{}, falls: newFallTracker()}
	s.run(tokens, start)
	if attempted.Before(deadline) || attempted.IsZero() {
		t.Fatalf("POST attempt=%v before deadline%v", attempted, deadline)
	}
}

func TestFractionalScheduleUsesTheSameZeroBasedArrivalCountInEveryReport(t *testing.T) {
	cfg := config{Rate: 1.5, Duration: time.Second, Senders: 2}
	if got := prefetched(cfg, cfg.Duration); got != 2 {
		t.Fatalf("scheduled arrival count %d, want 2", got)
	}
	s := &senderStats{Accepted: 2}
	s.phases[0].Sent = 2
	s.phases[0].Accepted = 2
	r := buildReport(cfg, time.Now(), time.Second, []*senderStats{s}, 0, newFallTracker())
	if r.Offered != 2 || !r.Reconciles || r.Phases[0].Scheduled != 2 {
		t.Fatalf("fractional cohort failed reconciliation: %+v", r)
	}
}

func TestAllAcceptedValidationRejectsSentButRefusedEvents(t *testing.T) {
	for _, r := range []report{
		{Config: config{RequireAllAccepted: true}, Reconciles: true},
		{Config: config{RequireAllAccepted: true}, Reconciles: true, Sent: 2, Accepted: 1, Overloaded: 1},
	} {
		r.evaluateValidation()
		if r.ValidationPassed {
			t.Fatalf("unacknowledged or empty run passed: %+v", r)
		}
	}
	r := report{Config: config{RequireAllAccepted: true}, Reconciles: true, Sent: 2, Accepted: 1, Duplicate: 1}
	r.evaluateValidation()
	if !r.ValidationPassed {
		t.Fatalf("all acknowledged events failed: %+v", r)
	}
}

func TestTwoBurstPhasesAndExactBoundaries(t *testing.T) {
	cfg := config{Rate: 10, Duration: 8 * time.Second, BurstRate: 100,
		BurstStart: 2 * time.Second, BurstDuration: time.Second, SecondBurstStart: 5 * time.Second}
	want := []phase{
		{name: "baseline", start: 0, end: 2 * time.Second, rate: 10},
		{name: "burst_1", start: 2 * time.Second, end: 3 * time.Second, rate: 100},
		{name: "between_bursts", start: 3 * time.Second, end: 5 * time.Second, rate: 10},
		{name: "burst_2", start: 5 * time.Second, end: 6 * time.Second, rate: 100},
		{name: "post_burst", start: 6 * time.Second, end: 8 * time.Second, rate: 10},
	}
	got := phases(cfg)
	if len(got) != len(want) {
		t.Fatalf("phase count %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("phase %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	for _, tc := range []struct {
		elapsed time.Duration
		index   int
		end     time.Duration
	}{
		{0, 0, 2 * time.Second},
		{2 * time.Second, 1, 3 * time.Second},
		{3 * time.Second, 2, 5 * time.Second},
		{5 * time.Second, 3, 6 * time.Second},
		{6 * time.Second, 4, 8 * time.Second},
	} {
		if i := phaseAt(cfg, tc.elapsed); i != tc.index {
			t.Errorf("phaseAt(%v)=%d, want %d", tc.elapsed, i, tc.index)
		}
		if end := phaseEnd(cfg, tc.index); end != tc.end {
			t.Errorf("phaseEnd(%d)=%v, want %v", tc.index, end, tc.end)
		}
	}
}

func TestTwoBurstScheduleIntegralAndInverse(t *testing.T) {
	cfg := config{Rate: 10, Duration: 8 * time.Second, BurstRate: 100,
		BurstStart: 2 * time.Second, BurstDuration: time.Second, SecondBurstStart: 5 * time.Second}
	for elapsed, want := range map[time.Duration]float64{
		0: 0, 2 * time.Second: 20, 3 * time.Second: 120,
		5 * time.Second: 140, 6 * time.Second: 240, 8 * time.Second: 260,
	} {
		if got := scheduled(cfg, elapsed); got != want {
			t.Errorf("scheduled(%v)=%v, want %v", elapsed, got, want)
		}
	}
	for n, want := range map[int64]time.Duration{
		0: 0, 19: 1900 * time.Millisecond,
		20: 2 * time.Second, 119: 2990 * time.Millisecond,
		120: 3 * time.Second, 139: 4900 * time.Millisecond,
		140: 5 * time.Second, 239: 5990 * time.Millisecond,
		240: 6 * time.Second, 259: 7900 * time.Millisecond,
	} {
		if got := scheduledOffset(cfg, n); got != want {
			t.Errorf("scheduledOffset(%d)=%v, want %v", n, got, want)
		}
	}
	if got := prefetched(cfg, cfg.Duration); got != 260 {
		t.Fatalf("prefetched total %d, want 260", got)
	}
	if got := phases(cfg); len(got) != maxPhases {
		t.Fatalf("two-burst phase count %d, want max %d", len(got), maxPhases)
	}
}

func TestSecondBurstConfigurationRequiresGapAndFirstBurst(t *testing.T) {
	valid := config{Rate: 10, Duration: 10 * time.Second, BurstRate: 100,
		BurstStart: time.Second, BurstDuration: time.Second, SecondBurstStart: 5 * time.Second}
	if err := validateSecondBurst(valid); err != nil {
		t.Fatalf("valid second burst rejected: %v", err)
	}
	for name, cfg := range map[string]config{
		"without first burst": {Rate: 10, Duration: 10 * time.Second, SecondBurstStart: 5 * time.Second},
		"overlap":             {Rate: 10, Duration: 10 * time.Second, BurstRate: 100, BurstStart: time.Second, BurstDuration: 2 * time.Second, SecondBurstStart: 2500 * time.Millisecond},
		"touching no gap":     {Rate: 10, Duration: 10 * time.Second, BurstRate: 100, BurstStart: time.Second, BurstDuration: 2 * time.Second, SecondBurstStart: 3 * time.Second},
		"at duration":         {Rate: 10, Duration: 10 * time.Second, BurstRate: 100, BurstStart: time.Second, BurstDuration: time.Second, SecondBurstStart: 10 * time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateSecondBurst(cfg); err == nil {
				t.Fatal("invalid second burst accepted")
			}
		})
	}
	valid.SecondBurstStart = 0
	if err := validateSecondBurst(valid); err != nil {
		t.Fatalf("disabled second burst rejected: %v", err)
	}
}

func TestTwoBurstFractionalScheduleReconcilesAndEndsBeforeDuration(t *testing.T) {
	cfg := config{Rate: 1.5, Duration: 10 * time.Second, BurstRate: 2.25,
		BurstStart: 2500 * time.Millisecond, BurstDuration: 1500 * time.Millisecond, SecondBurstStart: 6 * time.Second}
	wantTotal := int64(18) // ceil of 17.0625 integrated arrivals
	if got := prefetched(cfg, cfg.Duration); got != wantTotal {
		t.Fatalf("scheduled total %d, want %d", got, wantTotal)
	}
	var last time.Duration
	for i := int64(0); i < wantTotal; i++ {
		offset := scheduledOffset(cfg, i)
		if offset >= cfg.Duration || i > 0 && offset <= last {
			t.Fatalf("arrival %d offset %v is not increasing within [0,%v)", i, offset, cfg.Duration)
		}
		last = offset
	}
	var scheduledByPhase int64
	for _, p := range phases(cfg) {
		count := int64(math.Ceil(scheduled(cfg, p.end))) - int64(math.Ceil(scheduled(cfg, p.start)))
		scheduledByPhase += count
	}
	if scheduledByPhase != wantTotal {
		t.Fatalf("phase scheduled counts sum to %d, want %d", scheduledByPhase, wantTotal)
	}
}

func TestTwoBurstFallLatenciesStayInTheirOwnBurstCohorts(t *testing.T) {
	cfg := config{Rate: 10, Duration: 8 * time.Second, BurstRate: 100,
		BurstStart: 2 * time.Second, BurstDuration: time.Second, SecondBurstStart: 5 * time.Second,
		MaxFallP95MS: 1000}
	started := time.Unix(100, 0)
	falls := newFallTracker()
	for i, offset := range []time.Duration{2100 * time.Millisecond, 5100 * time.Millisecond} {
		device := fmt.Sprintf("dev_%d", i+1)
		seq := int64(i + 1)
		id := eventID(device, seq)
		sent := started.Add(offset)
		falls.sent(device, seq, sent)
		falls.generated(device, seq, sent, sent)
		falls.accepted(device, seq)
		falls.delivered(id, sent.Add(20*time.Millisecond))
	}
	post, _, scheduled := falls.phaseLatencies(cfg, started)
	if len(post[1]) != 1 || len(post[3]) != 1 || len(scheduled[1]) != 1 || len(scheduled[3]) != 1 {
		t.Fatalf("burst latency cohorts missing: post=%v scheduled=%v", post, scheduled)
	}
	r := buildReport(cfg, started, cfg.Duration, nil, 0, falls)
	r.evaluateValidation()
	checks := map[string]bool{}
	for _, check := range r.ValidationChecks {
		checks[check.Name] = check.Status == "MET"
	}
	if !checks["fall_p95_burst_1"] || !checks["fall_p95_burst_2"] {
		t.Fatalf("both burst latency gates must be present and met: %+v", r.ValidationChecks)
	}
}
