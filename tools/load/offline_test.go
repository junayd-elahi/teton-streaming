package main

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

func TestOfflineWindowSelectionAndBoundaries(t *testing.T) {
	w := offlineWindow{start: 10 * time.Second, duration: 60 * time.Second, percent: 20, capacity: 10}
	for _, tc := range []struct {
		device  int
		elapsed time.Duration
		want    bool
	}{{0, 10 * time.Second, true}, {19, 69 * time.Second, true}, {20, 30 * time.Second, false},
		{1, 9*time.Second + time.Nanosecond, false}, {0, 70 * time.Second, false}, {-1, 30 * time.Second, false}} {
		if got := w.shouldBuffer(tc.device, tc.elapsed); got != tc.want {
			t.Errorf("shouldBuffer(%d, %s) = %v, want %v", tc.device, tc.elapsed, got, tc.want)
		}
	}
}

func TestOfflineBufferBoundedAndPreservesEvent(t *testing.T) {
	w := offlineWindow{start: time.Second, duration: time.Minute, percent: 20, capacity: 1}
	b := newOfflineBuffer(w)
	original := event{body: []byte(`{"ts":"original"}`), isFall: true, deviceID: "d1", seq: 7,
		confidence: .93, generatedAt: time.Unix(11, 2), scheduledAt: time.Unix(10, 1)}
	if !b.append(original) {
		t.Fatal("first append unexpectedly overflowed")
	}
	if b.append(event{body: []byte("second")}) {
		t.Fatal("second append should overflow")
	}
	if got := b.overflowCount(); got != 1 {
		t.Fatalf("overflow count = %d, want 1", got)
	}
	got := b.popReady(61*time.Second-time.Nanosecond, true)
	if len(got) != 0 {
		t.Fatal("event replayed before reconnect")
	}
	got = b.popReady(61*time.Second, true)
	if len(got) != 1 {
		t.Fatalf("replay count = %d, want 1", len(got))
	}
	replayed := got[0]
	if !bytes.Equal(replayed.body, original.body) || replayed.deviceID != original.deviceID || replayed.seq != original.seq ||
		replayed.isFall != original.isFall || replayed.confidence != original.confidence ||
		!replayed.generatedAt.Equal(original.generatedAt) || !replayed.scheduledAt.Equal(original.scheduledAt) {
		t.Fatalf("replayed event changed: got %#v want %#v", replayed, original)
	}
	if len(b.popReady(62*time.Second, true)) != 0 {
		t.Fatal("replayed event was returned more than once")
	}
}

func TestOfflineBufferWaitsForActiveWindowAndReconnect(t *testing.T) {
	b := newOfflineBuffer(offlineWindow{start: time.Second, duration: time.Minute, capacity: 2})
	if !b.append(event{body: []byte("payload")}) {
		t.Fatal("append failed")
	}
	if got := b.popReady(61*time.Second, false); len(got) != 0 {
		t.Fatal("replayed outside active offer window")
	}
	if got := b.popReady(60*time.Second, true); len(got) != 0 {
		t.Fatal("replayed before reconnection")
	}
	if got := b.popReady(61*time.Second, true); len(got) != 1 {
		t.Fatal("did not replay once reconnection was due in active window")
	}
}

func TestOfflineCoverageAndWorkerIndependentAssignments(t *testing.T) {
	w := offlineWindow{start: time.Second, duration: time.Minute, percent: 20, capacity: 1}
	offline := 0
	const devices = 1000
	for ordinal := range devices {
		device := plannedDevice(int64(ordinal), devices)
		if w.shouldBuffer(device, 2*time.Second) {
			offline++
		}
	}
	if offline != devices*w.percent/100 {
		t.Fatalf("assigned %d/%d offline across %d scheduled arrivals", offline, devices, devices)
	}
	// Sender consumption order can differ, but it cannot change the device
	// attached to any offered ordinal or the resulting offline cohort.
	for _, workerCount := range []int{1, 7, 2000} {
		count := 0
		for ordinal := 0; ordinal < devices; ordinal++ {
			worker := ordinal % workerCount
			_ = worker // workers consume tokens; device identity belongs to ordinal
			if w.shouldBuffer(plannedDevice(int64(ordinal), devices), 2*time.Second) {
				count++
			}
		}
		if count != offline {
			t.Fatalf("%d workers changed offline assignment count to %d", workerCount, count)
		}
	}
}

func TestOfflineSequenceCountersAreUniqueUnderConcurrentGeneration(t *testing.T) {
	d := newOfflineDevices("test", 1)
	const n = 500
	seen := make(map[int64]bool, n)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			seq := d.seqs[0].Add(1)
			mu.Lock()
			seen[seq] = true
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(seen) != n || d.seqs[0].Load() != n {
		t.Fatalf("unique sequences=%d counter=%d, want %d", len(seen), d.seqs[0].Load(), n)
	}
}

func TestOfflineDisabledBufferIsLazyAndOverflowIsVisible(t *testing.T) {
	b := newOfflineBuffer(offlineWindow{capacity: 1000})
	if cap(b.events) != 0 {
		t.Fatalf("constructor preallocated %d buffered events", cap(b.events))
	}
	b.window.capacity = 1
	if !b.append(event{body: []byte("one")}) || b.append(event{body: []byte("two")}) {
		t.Fatal("bounded append did not report expected overflow")
	}
	if b.overflowCount() != 1 {
		t.Fatalf("overflow count=%d, want 1", b.overflowCount())
	}
}

func TestOfflineLossCountsAsNotSentAndReconciles(t *testing.T) {
	cfg := config{Senders: 1, Devices: 100, Duration: time.Second, Rate: 4, OfflinePercent: 20}
	s := &senderStats{Accepted: 2, OfflineBuffered: 3, OfflineReplayed: 1, OfflineDropped: 1, OfflineUnflushed: 1}
	r := buildReport(cfg, time.Now(), time.Second, []*senderStats{s}, 0, newFallTracker())
	if !r.Reconciles || r.Sent != 2 || r.NotSent != 2 || r.Offered != 4 {
		t.Fatalf("offline accounting did not reconcile: sent=%d not_sent=%d offered=%d reconciles=%v", r.Sent, r.NotSent, r.Offered, r.Reconciles)
	}
	if r.OfflineBuffered != 3 || r.OfflineReplayed != 1 || r.OfflineDropped != 1 || r.OfflineUnflushed != 1 {
		t.Fatalf("offline counters were not aggregated: %+v", r)
	}
}

func TestRetryBackoffAndStrictWindow(t *testing.T) {
	want := []time.Duration{50, 100, 200, 400, 800, 800}
	for i, milliseconds := range want {
		if got := retryDelay(i, 0); got != milliseconds*time.Millisecond {
			t.Errorf("retryDelay(%d)=%s, want %dms", i, got, milliseconds)
		}
	}
	if got := retryDelay(0, 100*time.Millisecond); got != time.Second {
		t.Fatalf("503 Retry-After delay=%s, want at least 1s", got)
	}
	now := time.Unix(100, 0)
	if retryWithinWindow(now, time.Second, now.Add(time.Second), true) {
		t.Fatal("retry would start at or beyond strict deadline")
	}
	if !retryWithinWindow(now, time.Second, now.Add(time.Second), false) {
		t.Fatal("non-strict retry should be allowed")
	}
	if got := parseRetryAfter("1", now); got != time.Second {
		t.Fatalf("parsed Retry-After=%s, want 1s", got)
	}
}
