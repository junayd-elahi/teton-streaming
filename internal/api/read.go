package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/junayd-elahi/teton-streaming/internal/aggregation"
	"github.com/junayd-elahi/teton-streaming/internal/store"
)

// timestampLayout matches the event schema: UTC, millisecond precision.
const timestampLayout = "2006-01-02T15:04:05.000Z"

var occupancyWindows = map[string]time.Duration{
	"1m": time.Minute,
	"5m": 5 * time.Minute,
	"1h": time.Hour,
}

// getDeviceHealth reports the latest heartbeat and the share of the last 300
// one-second slots that contain a heartbeat, both by device event time. A
// device never seen returns null and 0.
func (s *Server) getDeviceHealth(w http.ResponseWriter, r *http.Request) {
	last, slots, err := s.store.DeviceHealth(r.Context(), r.PathValue("device_id"), s.now())
	if err != nil {
		slog.Error("device health query failed", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "storage unavailable, retry"})
		return
	}
	var lastHeartbeat any
	if last != nil {
		lastHeartbeat = last.UTC().Format(timestampLayout)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"last_heartbeat_ts": lastHeartbeat,
		"availability_5m":   float64(slots) / store.HeartbeatWindow.Seconds(),
	})
}

// getRoomOccupancy reports the room state now and the share of the window it
// was occupied. Time before the room's first known state is unknown: it adds
// to unknown_seconds and never to occupied time, so occupied_pct is a lower
// bound when unknown_seconds is above zero.
func (s *Server) getRoomOccupancy(w http.ResponseWriter, r *http.Request) {
	window, ok := occupancyWindows[r.URL.Query().Get("window")]
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "window must be 1m, 5m or 1h"})
		return
	}
	end := s.now()
	start := end.Add(-window)
	predecessor, transitions, err := s.store.PresenceTimeline(r.Context(), r.PathValue("room_id"), start, end)
	if err != nil {
		slog.Error("occupancy query failed", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "storage unavailable, retry"})
		return
	}
	occ := aggregation.IntegrateOccupancy(start, end, predecessor, transitions)
	writeJSON(w, http.StatusOK, map[string]any{
		"in_room":         occ.InRoom,
		"occupied_pct":    occ.OccupiedSeconds / window.Seconds(),
		"window_seconds":  window.Seconds(),
		"known_seconds":   occ.KnownSeconds,
		"unknown_seconds": window.Seconds() - occ.KnownSeconds,
	})
}
