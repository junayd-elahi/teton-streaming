// Package api holds the HTTP handlers and request validation.
package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/junayd-elahi/teton-streaming/internal/ingest"
	"github.com/junayd-elahi/teton-streaming/internal/store"
)

const (
	maxFutureSkew = time.Hour
	maxBodyBytes  = 64 << 10
	// maxIDLength keeps device and room ids far below PostgreSQL's index row limit.
	maxIDLength = 128
	// maxNumberLength bounds a numeric literal so it always fits the jsonb column.
	maxNumberLength = 32
)

var eventTypes = map[string]bool{
	"heartbeat": true, "presence": true, "motion": true,
	"sleep_state": true, "fall_warn": true, "net_status": true,
}

type Server struct {
	writer *ingest.Writer
	store  *store.Store
	now    func() time.Time
}

func New(w *ingest.Writer, st *store.Store, now func() time.Time) *Server {
	return &Server{writer: w, store: st, now: now}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", s.postEvent)
	mux.HandleFunc("GET /devices/{device_id}/health", s.getDeviceHealth)
	mux.HandleFunc("GET /rooms/{room_id}/occupancy", s.getRoomOccupancy)
	mux.HandleFunc("GET /alarms", s.getAlarms)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})
	return mux
}

// postEvent responds only after the event is committed, so a 200 is a durable
// acknowledgement and a retry of the same event is safe.
func (s *Server) postEvent(w http.ResponseWriter, r *http.Request) {
	received := s.now()
	event, err := parseEvent(http.MaxBytesReader(w, r.Body, maxBodyBytes), received)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	outcome, err := s.writer.Submit(r.Context(), event)
	switch {
	case errors.Is(err, ingest.ErrOverloaded):
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "overloaded, retry"})
	case store.IsUnstorable(err):
		slog.Warn("event refused by storage", "device_id", event.DeviceID, "seq", event.Seq, "err", err)
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "event holds a value that cannot be stored"})
	case err != nil:
		slog.Error("journal write failed", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "storage unavailable, retry"})
	case outcome == store.Conflict:
		slog.Warn("identity conflict", "device_id", event.DeviceID, "seq", event.Seq)
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "device_id and seq already used with different content"})
	default:
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "duplicate": outcome == store.Duplicate})
	}
}

// parseEvent validates the envelope and type-specific fields and builds the
// canonical payload.
// Canonical form is the JSON object re-encoded with sorted keys and numbers
// kept exactly as sent.
func parseEvent(body io.Reader, received time.Time) (store.Event, error) {
	dec := json.NewDecoder(body)
	dec.UseNumber()
	var raw map[string]any
	if err := dec.Decode(&raw); err != nil {
		return store.Event{}, fmt.Errorf("invalid json: %w", err)
	}
	if raw == nil {
		return store.Event{}, errors.New("body must be a json object")
	}
	if dec.More() {
		return store.Event{}, errors.New("body must hold exactly one json object")
	}
	if err := validateStorable(raw); err != nil {
		return store.Event{}, err
	}

	deviceID, err := requiredID(raw, "device_id")
	if err != nil {
		return store.Event{}, err
	}
	roomID, err := requiredID(raw, "room_id")
	if err != nil {
		return store.Event{}, err
	}
	typ, err := requiredString(raw, "type")
	if err != nil {
		return store.Event{}, err
	}
	if !eventTypes[typ] {
		return store.Event{}, fmt.Errorf("unknown type %q", typ)
	}
	tsText, err := requiredString(raw, "ts")
	if err != nil {
		return store.Event{}, err
	}
	ts, err := time.Parse(time.RFC3339Nano, tsText)
	if err != nil {
		return store.Event{}, fmt.Errorf("ts: %w", err)
	}
	if ts.After(received.Add(maxFutureSkew)) {
		return store.Event{}, errors.New("ts is more than 1 hour in the future")
	}
	seqNum, ok := raw["seq"].(json.Number)
	if !ok {
		return store.Event{}, errors.New("seq is required and must be a number")
	}
	seq, err := seqNum.Int64()
	if err != nil || seq < 0 {
		return store.Event{}, errors.New("seq must be a non-negative integer")
	}

	if err := validateTypeFields(typ, raw); err != nil {
		return store.Event{}, err
	}

	var canonical bytes.Buffer
	enc := json.NewEncoder(&canonical)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(raw); err != nil {
		return store.Event{}, err
	}
	payload := bytes.TrimSpace(canonical.Bytes())
	hash := sha256.Sum256(payload)

	return store.Event{
		DeviceID:    deviceID,
		Seq:         seq,
		RoomID:      roomID,
		Type:        typ,
		TS:          ts.UTC(),
		Payload:     payload,
		PayloadHash: hash[:],
		ReceivedAt:  received.UTC(),
	}, nil
}

// validateTypeFields checks the one extra field each event type carries.
func validateTypeFields(typ string, raw map[string]any) error {
	switch typ {
	case "presence":
		if _, ok := raw["in_room"].(bool); !ok {
			return errors.New("in_room is required and must be a boolean")
		}
	case "motion":
		return requiredUnitInterval(raw, "magnitude")
	case "fall_warn":
		return requiredUnitInterval(raw, "confidence")
	case "sleep_state":
		state, _ := raw["state"].(string)
		if state != "asleep" && state != "awake" && state != "unknown" {
			return errors.New("state is required and must be asleep, awake or unknown")
		}
	case "net_status":
		if _, err := requiredNumber(raw, "rssi"); err != nil {
			return err
		}
	}
	return nil
}

func requiredNumber(raw map[string]any, key string) (float64, error) {
	num, ok := raw[key].(json.Number)
	if !ok {
		return 0, fmt.Errorf("%s is required and must be a number", key)
	}
	v, err := num.Float64()
	if err != nil {
		return 0, fmt.Errorf("%s is not a valid number", key)
	}
	return v, nil
}

func requiredUnitInterval(raw map[string]any, key string) error {
	v, err := requiredNumber(raw, key)
	if err != nil {
		return err
	}
	if v < 0 || v > 1 {
		return fmt.Errorf("%s must be between 0 and 1", key)
	}
	return nil
}

// validateStorable rejects values PostgreSQL cannot hold in a jsonb column,
// anywhere in the event including fields this service does not interpret:
// NUL characters in text, and numbers too long or too large to represent.
func validateStorable(value any) error {
	switch v := value.(type) {
	case string:
		if strings.ContainsRune(v, 0) {
			return errors.New("text must not contain NUL characters")
		}
	case json.Number:
		if len(v) > maxNumberLength {
			return fmt.Errorf("number literals must be at most %d characters", maxNumberLength)
		}
		if _, err := v.Float64(); err != nil {
			return errors.New("number is out of range")
		}
	case map[string]any:
		for key, item := range v {
			if err := validateStorable(key); err != nil {
				return err
			}
			if err := validateStorable(item); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range v {
			if err := validateStorable(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func requiredID(raw map[string]any, key string) (string, error) {
	v, err := requiredString(raw, key)
	if err != nil {
		return "", err
	}
	if len(v) > maxIDLength {
		return "", fmt.Errorf("%s must be at most %d bytes", key, maxIDLength)
	}
	return v, nil
}

func requiredString(raw map[string]any, key string) (string, error) {
	v, ok := raw[key].(string)
	if !ok || v == "" {
		return "", fmt.Errorf("%s is required and must be a non-empty string", key)
	}
	return v, nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}
