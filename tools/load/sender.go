package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"time"
)

const tsLayout = "2006-01-02T15:04:05.000Z"

// Event mix of Teton's generator: heartbeat 50, motion 15, presence 4,
// sleep_state 3, net_status 3, fall_warn 1.
var eventMix = []struct {
	typ    string
	weight int
}{
	{"heartbeat", 50}, {"motion", 15}, {"presence", 4}, {"sleep_state", 3}, {"net_status", 3}, {"fall_warn", 1},
}

func newClient(senders int) *http.Client {
	transport := &http.Transport{
		MaxIdleConns:        senders,
		MaxIdleConnsPerHost: senders,
		MaxConnsPerHost:     senders,
	}
	return &http.Client{Transport: transport, Timeout: 15 * time.Second}
}

// senderStats is written by one sender only and read after it has finished.
type senderStats struct {
	Accepted, Duplicate, Invalid, Conflict int64
	Overloaded, OtherStatus, TransportErr  int64
	Retried                                int64
	latenciesMicros                        []uint32 // one per request that got a response
}

type sender struct {
	id     int
	cfg    config
	client *http.Client
	prefix string
	falls  *fallTracker
	stats  *senderStats
}

// run sends events until the duration ends (unpaced) or the token channel
// closes (paced). Each sender owns the devices whose index is congruent to its
// id, so a device's sequence numbers only ever come from one goroutine.
func (s *sender) run(tokens <-chan struct{}, started time.Time) {
	rng := rand.New(rand.NewPCG(uint64(s.id), uint64(started.UnixNano())))
	owned := (s.cfg.Devices - s.id + s.cfg.Senders - 1) / s.cfg.Senders
	seqs := make([]int64, owned)
	deadline := started.Add(s.cfg.Duration)
	for {
		if tokens == nil {
			if time.Now().After(deadline) {
				return
			}
		} else if _, ok := <-tokens; !ok {
			return
		}
		slot := rng.IntN(owned)
		device := s.id + slot*s.cfg.Senders
		seqs[slot]++
		s.send(s.event(rng, device, seqs[slot]))
	}
}

type event struct {
	body       []byte
	isFall     bool
	deviceID   string
	seq        int64
	confidence float64
}

func (s *sender) event(rng *rand.Rand, device int, seq int64) event {
	typ := pickType(rng)
	e := event{deviceID: fmt.Sprintf("%s_%d", s.prefix, device), seq: seq, isFall: typ == "fall_warn"}
	fields := map[string]any{
		"device_id": e.deviceID,
		"room_id":   fmt.Sprintf("%s_room_%d", s.prefix, device/20),
		"type":      typ,
		"ts":        time.Now().UTC().Format(tsLayout),
		"seq":       seq,
	}
	switch typ {
	case "presence":
		fields["in_room"] = rng.IntN(2) == 0
	case "motion":
		fields["magnitude"] = float64(rng.IntN(101)) / 100
	case "sleep_state":
		fields["state"] = []string{"asleep", "awake", "unknown"}[rng.IntN(3)]
	case "fall_warn":
		e.confidence = float64(70+rng.IntN(30)) / 100
		fields["confidence"] = e.confidence
	case "net_status":
		fields["rssi"] = -90 + rng.IntN(41)
	}
	body, err := json.Marshal(fields)
	if err != nil {
		panic(err)
	}
	e.body = body
	return e
}

func pickType(rng *rand.Rand) string {
	n := rng.IntN(76) // sum of the weights
	for _, m := range eventMix {
		if n < m.weight {
			return m.typ
		}
		n -= m.weight
	}
	panic("event mix weights do not sum to 76")
}

// send posts one event, retrying a 503 or transport error with the same body.
// The final attempt decides which outcome the event is counted under.
func (s *sender) send(e event) {
	if e.isFall {
		s.falls.sent(e.deviceID, e.seq, time.Now())
	}
	for attempt := 0; ; attempt++ {
		status, duplicate, err := s.post(e.body)
		retryable := err != nil || status == http.StatusServiceUnavailable
		if retryable && attempt < s.cfg.Retries {
			s.stats.Retried++
			continue
		}
		switch {
		case err != nil:
			s.stats.TransportErr++
		case status == http.StatusOK && duplicate:
			s.stats.Duplicate++
			// An earlier attempt may have committed before its response was
			// lost. Its duplicate acknowledgement still proves this run's
			// fall is durable and must be checked for feed delivery.
			if e.isFall && attempt > 0 {
				s.falls.accepted(e.deviceID, e.seq)
			}
		case status == http.StatusOK:
			s.stats.Accepted++
			if e.isFall {
				s.falls.accepted(e.deviceID, e.seq)
			}
		case status == http.StatusBadRequest:
			s.stats.Invalid++
		case status == http.StatusConflict:
			s.stats.Conflict++
		case status == http.StatusServiceUnavailable:
			s.stats.Overloaded++
		default:
			s.stats.OtherStatus++
		}
		return
	}
}

func (s *sender) post(body []byte) (status int, duplicate bool, err error) {
	started := time.Now()
	resp, err := s.client.Post(s.cfg.Target+"/events", "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, false, err
	}
	s.stats.latenciesMicros = append(s.stats.latenciesMicros, uint32(time.Since(started).Microseconds()))
	var reply struct {
		Duplicate bool `json:"duplicate"`
	}
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(raw, &reply); err != nil {
			return 0, false, err
		}
	}
	return resp.StatusCode, reply.Duplicate, nil
}
