package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

type feedPage struct {
	Alarms []struct {
		EventID string `json:"event_id"`
	} `json:"alarms"`
	NextCursor int64 `json:"next_cursor"`
}

func fetchFeed(client *http.Client, target string, cursor int64, wait int) (feedPage, error) {
	var page feedPage
	resp, err := client.Get(fmt.Sprintf("%s/alarms?after_id=%d&wait=%d", target, cursor, wait))
	if err != nil {
		return page, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return page, fmt.Errorf("alarm feed answered %d", resp.StatusCode)
	}
	return page, json.NewDecoder(resp.Body).Decode(&page)
}

// currentCursor pages to the end of the feed so the run only sees its own alarms.
func currentCursor(client *http.Client, target string) (int64, error) {
	var cursor int64
	for {
		page, err := fetchFeed(client, target, cursor, 0)
		if err != nil {
			return 0, err
		}
		if len(page.Alarms) == 0 {
			return cursor, nil
		}
		cursor = page.NextCursor
	}
}

// followFeed long-polls the alarm feed until stop is closed and records the
// arrival time of every alarm belonging to this run.
func followFeed(client *http.Client, target string, cursor int64, prefix string, falls *fallTracker, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		page, err := fetchFeed(client, target, cursor, 1)
		if err != nil {
			time.Sleep(100 * time.Millisecond) // the service may be restarting
			continue
		}
		now := time.Now()
		for _, a := range page.Alarms {
			if strings.HasPrefix(a.EventID, prefix) {
				falls.delivered(a.EventID, now)
			}
		}
		cursor = page.NextCursor
	}
}

// fallTracker matches each fall this run sent to its arrival on the feed.
// The service names an alarm "<device_id>:<seq>" after the event that created it.
type fallTracker struct {
	mu          sync.Mutex
	sentAt      map[string]time.Time
	acceptedIDs map[string]bool
	deliveredAt map[string]time.Time
	duplicates  int64
}

func newFallTracker() *fallTracker {
	return &fallTracker{
		sentAt:      make(map[string]time.Time),
		acceptedIDs: make(map[string]bool),
		deliveredAt: make(map[string]time.Time),
	}
}

func eventID(deviceID string, seq int64) string { return fmt.Sprintf("%s:%d", deviceID, seq) }

func (f *fallTracker) sent(deviceID string, seq int64, at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sentAt[eventID(deviceID, seq)] = at
}

func (f *fallTracker) accepted(deviceID string, seq int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acceptedIDs[eventID(deviceID, seq)] = true
}

func (f *fallTracker) delivered(id string, at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, seen := f.deliveredAt[id]; seen {
		f.duplicates++
		return
	}
	f.deliveredAt[id] = at
}

// missing counts accepted falls the feed has not delivered.
func (f *fallTracker) missing() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n int64
	for id := range f.acceptedIDs {
		if _, ok := f.deliveredAt[id]; !ok {
			n++
		}
	}
	return n
}

func (f *fallTracker) waitForDelivery(limit time.Duration) {
	deadline := time.Now().Add(limit)
	for f.missing() > 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
}

// latenciesMicros returns, for every delivered fall, the time from just before
// its POST was sent to its arrival on the feed.
func (f *fallTracker) latenciesMicros() []uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]uint32, 0, len(f.deliveredAt))
	for id, at := range f.deliveredAt {
		out = append(out, uint32(at.Sub(f.sentAt[id]).Microseconds()))
	}
	return out
}
