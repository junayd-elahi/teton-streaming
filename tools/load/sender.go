package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
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
type eventCounts struct {
	Accepted, Duplicate, Invalid, Conflict int64
	Overloaded, OtherStatus, TransportErr  int64
	Retried                                int64
}

type senderStats struct {
	Accepted, Duplicate, Invalid, Conflict         int64
	Overloaded, OtherStatus, TransportErr, Retried int64
	Expired                                        int64
	OfflineBuffered, OfflineReplayed               int64
	OfflineDropped, OfflineUnflushed               int64
	actualStarts, actualACKs                       [maxPhases]int64
	startsAfterOffer, acksAfterOffer               int64
	phases                                         [maxPhases]phaseStats
	latenciesMicros                                []uint32 // one per request that got a response
}

type phaseStats struct {
	eventCounts
	Sent, StartsWithinPhase, StartsAfterPhase, AcceptedWithinPhase, AcceptedAfterPhase int64
	sendDelay, generationACK, scheduledACK                                             []uint32
}

type sender struct {
	id             int
	cfg            config
	client         *http.Client
	prefix         string
	falls          *fallTracker
	stats          *senderStats
	started        time.Time
	offlineDevices *offlineDevices
	offlineBuffer  *offlineBuffer
}

// run sends events until the duration ends (unpaced) or the token channel
// closes (paced). Each sender owns the devices whose index is congruent to its
// id, so a device's sequence numbers only ever come from one goroutine.
func (s *sender) run(tokens <-chan arrival, started time.Time) {
	s.started = started
	rng := rand.New(rand.NewPCG(uint64(s.id), uint64(started.UnixNano())))
	owned := (s.cfg.Devices - s.id + s.cfg.Senders - 1) / s.cfg.Senders
	var seqs []int64
	var deviceIDs, roomIDs []string
	if s.offlineDevices == nil {
		seqs = make([]int64, owned)
		deviceIDs, roomIDs = make([]string, owned), make([]string, owned)
		for slot := range owned {
			device := s.id + slot*s.cfg.Senders
			deviceIDs[slot] = fmt.Sprintf("%s_%d", s.prefix, device)
			roomIDs[slot] = fmt.Sprintf("%s_room_%d", s.prefix, device/20)
		}
	}
	deadline := started.Add(s.cfg.Duration)
	if s.cfg.OfflinePercent > 0 {
		s.offlineBuffer = newOfflineBuffer(offlineWindow{start: s.cfg.OfflineStart, duration: s.cfg.OfflineDuration,
			percent: s.cfg.OfflinePercent, capacity: s.cfg.OfflineBuffer})
	}
	for {
		if s.offlineBuffer != nil {
			s.flushOffline(time.Now())
		}
		generatedAt := time.Now()
		scheduledAt := generatedAt
		var a arrival
		if tokens == nil {
			if time.Now().After(deadline) {
				return
			}
		} else {
			var ok bool
			a, ok = <-tokens
			if !ok {
				if s.offlineBuffer != nil {
					s.flushOffline(time.Now())
					s.stats.OfflineUnflushed += int64(s.offlineBuffer.unflushed())
				}
				return
			}
			scheduledAt = a.at
			if wait := time.Until(scheduledAt); wait > 0 {
				time.Sleep(wait)
			}
			generatedAt = time.Now()
			if s.cfg.StrictOfferWindow && !generatedAt.Before(deadline) {
				s.stats.Expired++
				continue
			}
		}
		var e event
		deviceIndex := -1
		if s.offlineDevices != nil && tokens != nil {
			deviceIndex = a.device
			seq := s.offlineDevices.seqs[deviceIndex].Add(1)
			// Offline simulation models generation on the device's scheduled
			// clock, before network buffering or delayed worker execution.
			e = s.eventWithIDsAt(rng, s.offlineDevices.deviceIDs[deviceIndex], s.offlineDevices.roomIDs[deviceIndex], seq, a.at)
		} else {
			slot := rng.IntN(owned)
			seqs[slot]++
			e = s.eventWithIDs(rng, deviceIDs[slot], roomIDs[slot], seqs[slot])
		}
		e.generatedAt, e.scheduledAt = generatedAt, scheduledAt
		// Strict mode includes event construction in the offer window.
		if s.cfg.StrictOfferWindow && !time.Now().Before(deadline) {
			s.stats.Expired++
			continue
		}
		if deviceIndex >= 0 && s.offlineBuffer.window.shouldBuffer(deviceIndex, generatedAt.Sub(started)) {
			if s.offlineBuffer.append(e) {
				s.stats.OfflineBuffered++
			} else {
				s.stats.OfflineDropped++
			}
			continue
		}
		s.send(e)
	}
}

func (s *sender) flushOffline(now time.Time) {
	elapsed := now.Sub(s.started)
	ready := s.offlineBuffer.popReady(elapsed, now.Before(s.started.Add(s.cfg.Duration)))
	for i, e := range ready {
		if !time.Now().Before(s.started.Add(s.cfg.Duration)) {
			s.stats.OfflineUnflushed += int64(len(ready) - i)
			return
		}
		if !s.send(e) {
			s.stats.OfflineUnflushed += int64(len(ready) - i - 1)
			return
		}
		s.stats.OfflineReplayed++
	}
}

type event struct {
	body                     []byte
	isFall                   bool
	deviceID                 string
	seq                      int64
	confidence               float64
	generatedAt, scheduledAt time.Time
}

func (s *sender) event(rng *rand.Rand, device int, seq int64) event {
	return s.eventWithIDs(rng, fmt.Sprintf("%s_%d", s.prefix, device), fmt.Sprintf("%s_room_%d", s.prefix, device/20), seq)
}

// Typed JSON avoids allocating and sorting a map for every simulated event.
// Pointers keep false and zero type-specific values present in the payload.
type generatedPayload struct {
	DeviceID   string   `json:"device_id"`
	RoomID     string   `json:"room_id"`
	Type       string   `json:"type"`
	TS         string   `json:"ts"`
	Seq        int64    `json:"seq"`
	InRoom     *bool    `json:"in_room,omitempty"`
	Magnitude  *float64 `json:"magnitude,omitempty"`
	State      string   `json:"state,omitempty"`
	Confidence *float64 `json:"confidence,omitempty"`
	RSSI       *int     `json:"rssi,omitempty"`
}

func (s *sender) eventWithIDs(rng *rand.Rand, deviceID, roomID string, seq int64) event {
	return s.eventWithIDsAt(rng, deviceID, roomID, seq, time.Now())
}

func (s *sender) eventWithIDsAt(rng *rand.Rand, deviceID, roomID string, seq int64, at time.Time) event {
	typ := pickType(rng)
	e := event{deviceID: deviceID, seq: seq, isFall: typ == "fall_warn"}
	fields := generatedPayload{
		DeviceID: deviceID, RoomID: roomID, Type: typ,
		TS: at.UTC().Format(tsLayout), Seq: seq,
	}
	switch typ {
	case "presence":
		value := rng.IntN(2) == 0
		fields.InRoom = &value
	case "motion":
		value := float64(rng.IntN(101)) / 100
		fields.Magnitude = &value
	case "sleep_state":
		fields.State = []string{"asleep", "awake", "unknown"}[rng.IntN(3)]
	case "fall_warn":
		e.confidence = float64(70+rng.IntN(30)) / 100
		fields.Confidence = &e.confidence
	case "net_status":
		value := -90 + rng.IntN(41)
		fields.RSSI = &value
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
func (s *sender) send(e event) bool {
	before := s.stats.counts()
	sentAt := time.Now()
	// One captured admission time governs strict expiration and start metrics.
	if s.cfg.StrictOfferWindow && !sentAt.Before(s.started.Add(s.cfg.Duration)) {
		s.stats.Expired++
		return false
	}
	durable := false
	if e.generatedAt.IsZero() {
		e.generatedAt = sentAt
	}
	if e.scheduledAt.IsZero() {
		e.scheduledAt = e.generatedAt
	}
	cohort := phaseAt(s.cfg, e.scheduledAt.Sub(s.started))
	p := &s.stats.phases[cohort]
	p.Sent++
	if sentAt.Sub(s.started) < s.cfg.Duration {
		s.stats.actualStarts[phaseAt(s.cfg, sentAt.Sub(s.started))]++
	} else {
		s.stats.startsAfterOffer++
	}
	if sentAt.Sub(s.started) < phaseEnd(s.cfg, cohort) {
		p.StartsWithinPhase++
	} else {
		p.StartsAfterPhase++
	}
	p.sendDelay = append(p.sendDelay, uint32(max(0, sentAt.Sub(e.scheduledAt).Microseconds())))
	defer func() {
		p.eventCounts.add(s.stats.counts().difference(before))
		if durable {
			done := time.Now()
			if done.Sub(s.started) < s.cfg.Duration {
				s.stats.actualACKs[phaseAt(s.cfg, done.Sub(s.started))]++
			} else {
				s.stats.acksAfterOffer++
			}
			p.generationACK = append(p.generationACK, uint32(done.Sub(e.generatedAt).Microseconds()))
			p.scheduledACK = append(p.scheduledACK, uint32(max(0, done.Sub(e.scheduledAt).Microseconds())))
			if done.Sub(s.started) < phaseEnd(s.cfg, cohort) {
				p.AcceptedWithinPhase++
			} else {
				p.AcceptedAfterPhase++
			}
		}
	}()
	if e.isFall {
		s.falls.sent(e.deviceID, e.seq, sentAt)
		s.falls.generated(e.deviceID, e.seq, e.generatedAt, e.scheduledAt)
	}
	for attempt := 0; ; attempt++ {
		status, duplicate, retryAfter, err := s.post(e.body)
		retryable := err != nil || status == http.StatusServiceUnavailable
		if retryable && attempt < s.cfg.Retries && (!s.cfg.StrictOfferWindow || time.Now().Before(s.started.Add(s.cfg.Duration))) {
			delay := retryDelay(attempt, retryAfter)
			if retryWithinWindow(time.Now(), delay, s.started.Add(s.cfg.Duration), s.cfg.StrictOfferWindow) {
				s.stats.Retried++
				time.Sleep(delay)
				continue
			}
		}
		switch {
		case err != nil:
			s.stats.TransportErr++
		case status == http.StatusOK && duplicate:
			durable = true
			s.stats.Duplicate++
			// An earlier attempt may have committed before its response was
			// lost. Its duplicate acknowledgement still proves this run's
			// fall is durable and must be checked for feed delivery.
			if e.isFall && attempt > 0 {
				s.falls.accepted(e.deviceID, e.seq)
			}
		case status == http.StatusOK:
			durable = true
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
		return true
	}
}

func (s *sender) post(body []byte) (status int, duplicate bool, retryAfter time.Duration, err error) {
	started := time.Now()
	resp, err := s.client.Post(s.cfg.Target+"/events", "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, false, 0, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, false, 0, err
	}
	if resp.StatusCode == http.StatusServiceUnavailable {
		retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
	}
	s.stats.latenciesMicros = append(s.stats.latenciesMicros, uint32(time.Since(started).Microseconds()))
	var reply struct {
		Duplicate bool `json:"duplicate"`
	}
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(raw, &reply); err != nil {
			return 0, false, 0, err
		}
	}
	return resp.StatusCode, reply.Duplicate, retryAfter, nil
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil && at.After(now) {
		return at.Sub(now)
	}
	return 0
}

func (s *senderStats) counts() eventCounts {
	return eventCounts{Accepted: s.Accepted, Duplicate: s.Duplicate, Invalid: s.Invalid, Conflict: s.Conflict, Overloaded: s.Overloaded, OtherStatus: s.OtherStatus, TransportErr: s.TransportErr, Retried: s.Retried}
}
