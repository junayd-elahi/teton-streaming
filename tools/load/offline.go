package main

import (
	"fmt"
	"sync/atomic"
	"time"
)

// offlineWindow selects a stable fraction of global device indexes for one
// outage interval. A zero capacity disables buffering at construction time.
type offlineWindow struct {
	start, duration time.Duration
	percent         int
	capacity        int
}

// offlineDevices is allocated only for enabled paced offline workloads. Every
// worker shares one identity table and atomic sequence counter per device.
type offlineDevices struct {
	deviceIDs, roomIDs []string
	seqs               []atomic.Int64
}

func newOfflineDevices(prefix string, devices int) *offlineDevices {
	d := &offlineDevices{
		deviceIDs: make([]string, devices), roomIDs: make([]string, devices),
		seqs: make([]atomic.Int64, devices),
	}
	for i := range devices {
		d.deviceIDs[i] = fmt.Sprintf("%s_%d", prefix, i)
		d.roomIDs[i] = fmt.Sprintf("%s_room_%d", prefix, i/20)
	}
	return d
}

// plannedDevice gives each scheduled ordinal a stable device assignment,
// independent of which sender worker consumes that arrival.
func plannedDevice(ordinal int64, devices int) int {
	return int(ordinal % int64(devices))
}

// offlineBuffer belongs to one sender. It stores the original event values;
// bodies and event timestamps are not regenerated during replay.
type offlineBuffer struct {
	window   offlineWindow
	events   []event
	overflow int64
}

func newOfflineBuffer(window offlineWindow) *offlineBuffer {
	return &offlineBuffer{window: window}
}

// shouldBuffer uses the global device index so sender partitioning cannot
// change which devices experience the outage.
func (w offlineWindow) shouldBuffer(globalDevice int, elapsed time.Duration) bool {
	return w.percent > 0 && globalDevice >= 0 && globalDevice%100 < w.percent &&
		elapsed >= w.start && elapsed < w.start+w.duration
}

// append stores an event if space remains. A false result is an explicit
// overflow; callers must account for it instead of silently dropping it.
func (b *offlineBuffer) append(e event) bool {
	if b.window.capacity <= 0 || len(b.events) >= b.window.capacity {
		b.overflow++
		return false
	}
	b.events = append(b.events, e)
	return true
}

// popReady transfers buffered events only after reconnection is due and while
// the offer window is still active. The caller posts each returned event via
// its owning sender, so request latency starts at the replay POST.
func (b *offlineBuffer) popReady(elapsed time.Duration, active bool) []event {
	if !active || elapsed < b.window.start+b.window.duration || len(b.events) == 0 {
		return nil
	}
	ready := b.events
	b.events = nil
	return ready
}

func (b *offlineBuffer) overflowCount() int64 { return b.overflow }

func (b *offlineBuffer) unflushed() int { return len(b.events) }

func retryDelay(attempt int, retryAfter time.Duration) time.Duration {
	delay := 50 * time.Millisecond
	for range min(attempt, 4) {
		delay *= 2
	}
	if retryAfter > 0 {
		// A 503 Retry-After response is authoritative; short values still wait
		// at least one second to avoid immediately hammering a restarting API.
		delay = max(delay, max(time.Second, retryAfter))
	}
	return delay
}

func retryWithinWindow(now time.Time, delay time.Duration, deadline time.Time, strict bool) bool {
	return !strict || now.Add(delay).Before(deadline)
}
