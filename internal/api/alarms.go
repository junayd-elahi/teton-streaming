package api

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/junayd-elahi/teton-streaming/internal/store"
)

const (
	maxFeedWait   = 30 * time.Second
	feedPageLimit = 1000
	// maxUnixSeconds is 9999-12-31T23:59:59Z.
	maxUnixSeconds = 253402300799
)

// getAlarms serves two views of the same alarms:
//
//	GET /alarms?since=<ts>                  alarms received since, ordered by event time
//	GET /alarms?after_id=<cursor>&wait=<s>  resumable feed in commit order
func (s *Server) getAlarms(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Has("after_id") {
		s.getAlarmFeed(w, r)
		return
	}
	s.getAlarmSnapshot(w, r)
}

// getAlarmSnapshot returns every alarm received at or after since, ordered by
// event time. since is unix seconds (0 means everything) or an ISO 8601
// timestamp.
func (s *Server) getAlarmSnapshot(w http.ResponseWriter, r *http.Request) {
	s.metrics.ReadRequests.With("alarms_snapshot").Inc()
	since, err := parseSince(r.URL.Query().Get("since"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), readQueryTimeout)
	defer cancel()
	alarms, err := s.store.AlarmsSince(ctx, since)
	if err != nil {
		s.metrics.ReadErrors.Inc()
		slog.Error("alarm snapshot query failed", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "storage unavailable, retry"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"alarms": alarmObjects(alarms)})
}

// getAlarmFeed returns alarms committed after the cursor. With wait, an empty
// result is held open until a new alarm commits or the wait runs out. The
// consumer stores next_cursor and passes it as after_id next time; that works
// across disconnects and service restarts because the cursor is the alarm's
// durable id.
func (s *Server) getAlarmFeed(w http.ResponseWriter, r *http.Request) {
	s.metrics.ReadRequests.With("alarms_feed").Inc()
	query := r.URL.Query()
	cursor, err := strconv.ParseInt(query.Get("after_id"), 10, 64)
	if err != nil || cursor < 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "after_id must be a non-negative integer"})
		return
	}
	var wait time.Duration
	if query.Has("wait") {
		seconds, err := strconv.ParseFloat(query.Get("wait"), 64)
		// Written as "not inside the range" so NaN is rejected too.
		if err != nil || !(seconds >= 0 && seconds <= maxFeedWait.Seconds()) {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "wait must be between 0 and 30 seconds"})
			return
		}
		wait = time.Duration(seconds * float64(time.Second))
	}

	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	budget := readQueryTimeout
	if wait > 0 {
		budget = wait
	}
	feedCtx, cancel := context.WithTimeout(r.Context(), budget)
	defer cancel()
	// Notifications accelerate reads; committed database rows remain the
	// source of truth if a notification is lost after an ambiguous response.
	var poll <-chan time.Time
	if wait > 0 {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		poll = ticker.C
	}
	s.metrics.FeedWaiters.Add(1)
	defer s.metrics.FeedWaiters.Add(-1)
	hadSuccessfulRead := false
	for {
		// Take the signal before querying so a commit in between still wakes us.
		newAlarms := s.writer.NewAlarms()
		queryCtx, cancelQuery := context.WithTimeout(feedCtx, readQueryTimeout)
		alarms, err := s.store.AlarmsAfter(queryCtx, cursor, feedPageLimit)
		cancelQuery()
		if err != nil {
			if hadSuccessfulRead && wait > 0 && errors.Is(feedCtx.Err(), context.DeadlineExceeded) {
				writeJSON(w, http.StatusOK, map[string]any{"alarms": []any{}, "next_cursor": cursor})
				return
			}
			s.metrics.ReadErrors.Inc()
			slog.Error("alarm feed query failed", "err", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "storage unavailable, retry"})
			return
		}
		hadSuccessfulRead = true
		if len(alarms) > 0 {
			writeJSON(w, http.StatusOK, map[string]any{
				"alarms":      alarmObjects(alarms),
				"next_cursor": alarms[len(alarms)-1].ID,
			})
			return
		}
		select {
		case <-newAlarms:
		case <-poll:
		case <-deadline.C:
			writeJSON(w, http.StatusOK, map[string]any{"alarms": []any{}, "next_cursor": cursor})
			return
		case <-r.Context().Done():
			return
		}
	}
}

func parseSince(raw string) (time.Time, error) {
	if raw == "" {
		return time.Unix(0, 0), nil
	}
	if seconds, err := strconv.ParseFloat(raw, 64); err == nil {
		// Unix seconds between 1970 and the year 9999; this also excludes NaN and infinities.
		if !(seconds >= 0 && seconds <= maxUnixSeconds) {
			return time.Time{}, errors.New("since is out of range")
		}
		whole := math.Floor(seconds)
		return time.Unix(int64(whole), int64((seconds-whole)*1e9)), nil
	}
	if ts, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return ts, nil
	}
	return time.Time{}, errors.New("since must be unix seconds or an ISO 8601 timestamp")
}

// alarmObjects returns the fields Teton's evaluator documents (event_id,
// room_id, ts, confidence), the rest of the original fall_warn event, and the
// feed cursor and receipt time. Public event ids are persisted. The migration
// preserves previously published ids; new alarms receive unique ids.
func alarmObjects(alarms []store.Alarm) []map[string]any {
	objects := make([]map[string]any, len(alarms))
	for i, a := range alarms {
		objects[i] = map[string]any{
			"event_id":    a.EventID,
			"alarm_id":    a.ID,
			"device_id":   a.DeviceID,
			"room_id":     a.RoomID,
			"type":        "fall_warn",
			"ts":          a.TS.UTC().Format(timestampLayout),
			"seq":         a.Seq,
			"confidence":  a.Confidence,
			"received_ts": a.ReceivedAt.UTC().Format(timestampLayout),
		}
	}
	return objects
}
