package main

import (
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"time"
)

type percentiles struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
}

// summarise sorts the samples and uses the interpolated type-7 quantile. Raw
// samples take four bytes each: five minutes at 50,000 events/s is 60 MB per
// full-rate series. Request, schedule-to-send and two ACK series can together
// require 240 MB before capacity slack. Peak memory is higher because report
// construction aggregates per-sender slices into additional slices.
func summarise(micros []uint32) percentiles {
	if len(micros) == 0 {
		return percentiles{}
	}
	slices.Sort(micros)
	at := func(q float64) float64 {
		position := q * float64(len(micros)-1)
		lo := int(math.Floor(position))
		hi := int(math.Ceil(position))
		fraction := position - float64(lo)
		value := float64(micros[lo])*(1-fraction) + float64(micros[hi])*fraction
		return value / 1000
	}
	return percentiles{Count: len(micros), P50: at(0.50), P95: at(0.95), P99: at(0.99), Max: at(1)}
}

type report struct {
	Started        time.Time      `json:"started"`
	ElapsedSeconds float64        `json:"elapsed_seconds"`
	Config         config         `json:"config"`
	Environment    map[string]any `json:"environment"`

	Offered               int64         `json:"offered"`
	NotSent               int64         `json:"not_sent"`
	Accepted              int64         `json:"accepted"`
	Duplicate             int64         `json:"duplicate"`
	Invalid               int64         `json:"invalid_400"`
	Conflict              int64         `json:"conflict_409"`
	Overloaded            int64         `json:"overloaded_503"`
	OtherStatus           int64         `json:"other_status"`
	TransportErr          int64         `json:"transport_errors"`
	Retried               int64         `json:"retried_attempts"`
	OfflineBuffered       int64         `json:"offline_buffered"`
	OfflineReplayed       int64         `json:"offline_replayed"`
	OfflineDropped        int64         `json:"offline_dropped"`
	OfflineUnflushed      int64         `json:"offline_unflushed"`
	StartsAfterOffer      int64         `json:"starts_after_offer_window"`
	DurableACKsAfterOffer int64         `json:"durable_acks_after_offer_window"`
	Sent                  int64         `json:"sent"`
	Expired               int64         `json:"expired_before_send"`
	Phases                []phaseReport `json:"phases"`
	QueueCapacity         int           `json:"scheduled_queue_capacity"`
	Reconciles            bool          `json:"reconciles"`

	AcceptedPerSecond float64     `json:"accepted_per_second"`
	RequestMeanMS     float64     `json:"request_mean_ms"`
	RequestLatency    percentiles `json:"request_latency"`

	FallsSent             int         `json:"falls_sent"`
	FallsAccepted         int         `json:"falls_accepted"`
	FallsDelivered        int         `json:"falls_delivered"`
	FallsMissing          int64       `json:"falls_missing"`
	FallsDuplicated       int64       `json:"falls_duplicated"`
	FallLatency           percentiles `json:"fall_post_to_feed_latency"`
	FallGenerationLatency percentiles `json:"fall_generation_to_feed_latency"`
	FallScheduleLatency   percentiles `json:"fall_schedule_to_feed_latency"`

	ValidationStatus    string            `json:"validation_status"`
	ValidationRequested bool              `json:"validation_requested"`
	ValidationPassed    bool              `json:"validation_passed"`
	ValidationChecks    []validationCheck `json:"validation_checks,omitempty"`
}

type validationCheck struct {
	Name      string `json:"name"`
	Status    string `json:"status"`
	Observed  string `json:"observed"`
	Threshold string `json:"threshold"`
}

func (cfg config) validationRequested() bool {
	return cfg.RequireFullOffer || cfg.RequireAllAccepted || cfg.RequireNoLoss || cfg.MinAcceptedRate > 0 || cfg.MaxRequestP95MS > 0 || cfg.MaxFallP95MS > 0
}

// evaluateValidation keeps saturation runs useful for diagnosis while making
// requested acceptance criteria explicit and machine-readable.
func (r *report) evaluateValidation() {
	r.ValidationRequested = r.Config.validationRequested()
	if !r.ValidationRequested {
		r.ValidationStatus = "diagnostic"
		return
	}
	r.ValidationPassed = true
	add := func(name string, passed bool, observed, threshold string) {
		status := "MET"
		if !passed {
			status = "UNMET"
			r.ValidationPassed = false
		}
		r.ValidationChecks = append(r.ValidationChecks, validationCheck{Name: name, Status: status, Observed: observed, Threshold: threshold})
	}
	add("counts_reconcile", r.Reconciles, fmt.Sprintf("%v", r.Reconciles), "true")
	if r.Config.RequireFullOffer {
		passed := r.Config.Rate > 0 && r.Offered > 0 && r.NotSent == 0
		observed := fmt.Sprintf("offered=%d, not_sent=%d, rate=%.0f/s", r.Offered, r.NotSent, r.Config.Rate)
		add("full_offer_sent", passed, observed, "paced rate > 0, offered > 0 and not_sent=0")
	}
	if r.Config.RequireNoLoss {
		passed := r.FallsAccepted > 0 && r.FallsMissing == 0
		observed := fmt.Sprintf("accepted_falls=%d, missing=%d", r.FallsAccepted, r.FallsMissing)
		add("fall_feed_no_loss", passed, observed, "at least one accepted fall and missing=0")
	}
	if r.Config.RequireAllAccepted {
		acknowledged := r.Accepted + r.Duplicate
		passed := r.Sent > 0 && acknowledged == r.Sent
		add("all_sent_acknowledged", passed, fmt.Sprintf("sent=%d, acknowledged=%d", r.Sent, acknowledged),
			"at least one sent event and every sent event acknowledged")
	}
	if r.Config.MinAcceptedRate > 0 {
		add("minimum_accepted_rate", r.AcceptedPerSecond >= r.Config.MinAcceptedRate,
			fmt.Sprintf("%.3f/s", r.AcceptedPerSecond), fmt.Sprintf("at least %.3f/s", r.Config.MinAcceptedRate))
	}
	if r.Config.MaxRequestP95MS > 0 {
		passed := r.RequestLatency.Count > 0 && r.RequestLatency.P95 <= r.Config.MaxRequestP95MS
		add("request_p95", passed, fmt.Sprintf("count=%d, p95=%.3fms", r.RequestLatency.Count, r.RequestLatency.P95),
			fmt.Sprintf("count > 0 and p95 <= %.3fms", r.Config.MaxRequestP95MS))
	}
	if r.Config.MaxFallP95MS > 0 {
		passed := r.FallLatency.Count > 0 && r.FallLatency.P95 <= r.Config.MaxFallP95MS
		add("fall_post_to_feed_p95", passed, fmt.Sprintf("count=%d, p95=%.3fms", r.FallLatency.Count, r.FallLatency.P95),
			fmt.Sprintf("count > 0 and aggregate p95 <= %.3fms", r.Config.MaxFallP95MS))
	}
	if r.Config.MaxFallP95MS > 0 {
		for _, p := range r.Phases {
			// A phase without falls is reported as unsampled; the aggregate
			// criterion still requires at least one observed fall.
			if p.FallPostLatency.Count == 0 {
				continue
			}
			add("fall_p95_"+p.Name, p.FallPostLatency.P95 <= r.Config.MaxFallP95MS,
				fmt.Sprintf("count=%d, p95=%.3fms", p.FallPostLatency.Count, p.FallPostLatency.P95), fmt.Sprintf("phase p95 <= %.3fms", r.Config.MaxFallP95MS))
		}
	}
	if r.ValidationPassed {
		r.ValidationStatus = "met"
	} else {
		r.ValidationStatus = "unmet"
	}
}

func buildReport(cfg config, started time.Time, elapsed time.Duration, senders []*senderStats, notSent int64, falls *fallTracker) report {
	r := report{Started: started, ElapsedSeconds: elapsed.Seconds(), Config: cfg, Environment: environment(), NotSent: notSent}
	r.QueueCapacity = offerCapacity(cfg)
	definitions := phases(cfg)
	r.Phases = make([]phaseReport, len(definitions))
	var phaseSamples [maxPhases]phaseStats
	for i, p := range definitions {
		r.Phases[i] = phaseReport{Name: p.name, StartSeconds: p.start.Seconds(), EndSeconds: p.end.Seconds(), Rate: p.rate,
			Scheduled: min(int64(math.Ceil(scheduled(cfg, cfg.Duration))), int64(math.Ceil(scheduled(cfg, p.end)))) - min(int64(math.Ceil(scheduled(cfg, cfg.Duration))), int64(math.Ceil(scheduled(cfg, p.start))))}
	}
	var latencies []uint32
	for _, s := range senders {
		r.Accepted += s.Accepted
		r.Duplicate += s.Duplicate
		r.Invalid += s.Invalid
		r.Conflict += s.Conflict
		r.Overloaded += s.Overloaded
		r.OtherStatus += s.OtherStatus
		r.TransportErr += s.TransportErr
		r.Retried += s.Retried
		r.Expired += s.Expired
		r.StartsAfterOffer += s.startsAfterOffer
		r.DurableACKsAfterOffer += s.acksAfterOffer
		for i := range definitions {
			r.Phases[i].ActualStarts += s.actualStarts[i]
			r.Phases[i].ActualDurableACKs += s.actualACKs[i]
		}
		for i := range definitions {
			dst, src := &phaseSamples[i], &s.phases[i]
			dst.eventCounts.add(src.eventCounts)
			dst.Sent += src.Sent
			dst.StartsWithinPhase += src.StartsWithinPhase
			dst.StartsAfterPhase += src.StartsAfterPhase
			dst.AcceptedWithinPhase += src.AcceptedWithinPhase
			dst.AcceptedAfterPhase += src.AcceptedAfterPhase
			dst.sendDelay = append(dst.sendDelay, src.sendDelay...)
			dst.generationACK = append(dst.generationACK, src.generationACK...)
			dst.scheduledACK = append(dst.scheduledACK, src.scheduledACK...)
		}
		latencies = append(latencies, s.latenciesMicros...)
		r.OfflineBuffered += s.OfflineBuffered
		r.OfflineReplayed += s.OfflineReplayed
		r.OfflineDropped += s.OfflineDropped
		r.OfflineUnflushed += s.OfflineUnflushed
	}
	sent := r.Accepted + r.Duplicate + r.Invalid + r.Conflict + r.Overloaded + r.OtherStatus + r.TransportErr
	r.Sent = sent
	r.NotSent += r.Expired
	r.NotSent += r.OfflineDropped + r.OfflineUnflushed
	for i := range r.Phases {
		src := phaseSamples[i]
		dst := &r.Phases[i]
		dst.Sent = src.Sent
		dst.Accepted = src.Accepted
		dst.Duplicate = src.Duplicate
		dst.Invalid = src.Invalid
		dst.Conflict = src.Conflict
		dst.Overloaded = src.Overloaded
		dst.OtherStatus = src.OtherStatus
		dst.TransportErr = src.TransportErr
		dst.NotSent = dst.Scheduled - dst.Sent
		if cfg.Rate == 0 {
			dst.Scheduled = dst.Sent
			dst.NotSent = 0
		}
		dst.StartsWithinPhase = src.StartsWithinPhase
		dst.StartsAfterPhase = src.StartsAfterPhase
		dst.AcceptedWithinPhase = src.AcceptedWithinPhase
		dst.AcceptedAfterPhase = src.AcceptedAfterPhase
		dst.AcceptedPerSecond = float64(src.Accepted) / (dst.EndSeconds - dst.StartSeconds)
		dst.SendDelay = summarise(src.sendDelay)
		dst.GenerationACK = summarise(src.generationACK)
		dst.ScheduleACK = summarise(src.scheduledACK)
		dst.Reconciles = dst.NotSent >= 0 && dst.Scheduled == dst.Sent+dst.NotSent && dst.Sent == src.Accepted+src.Duplicate+src.Invalid+src.Conflict+src.Overloaded+src.OtherStatus+src.TransportErr
	}
	r.Offered = sent + r.NotSent
	if cfg.Rate > 0 {
		// Paced: what was offered comes from the schedule, independently of what the senders did.
		r.Offered = int64(math.Ceil(scheduled(cfg, cfg.Duration)))
	}
	r.Reconciles = r.Offered == sent+r.NotSent
	r.AcceptedPerSecond = float64(r.Accepted) / elapsed.Seconds()
	var totalMicros uint64
	for _, v := range latencies {
		totalMicros += uint64(v)
	}
	if len(latencies) > 0 {
		r.RequestMeanMS = float64(totalMicros) / float64(len(latencies)) / 1000
	}
	r.RequestLatency = summarise(latencies)

	r.FallsSent = len(falls.sentAt)
	r.FallsAccepted = len(falls.acceptedIDs)
	r.FallsDelivered = len(falls.deliveredAt)
	r.FallsMissing = falls.missing()
	r.FallsDuplicated = falls.duplicates
	r.FallLatency = summarise(falls.latenciesMicros())
	r.FallGenerationLatency = summarise(falls.latencyFrom(falls.generatedAt))
	r.FallScheduleLatency = summarise(falls.latencyFrom(falls.scheduledAt))
	post, generated, scheduled := falls.phaseLatencies(cfg, started)
	for i := range r.Phases {
		r.Phases[i].FallPostLatency = summarise(post[i])
		r.Phases[i].FallGenerationLatency = summarise(generated[i])
		r.Phases[i].FallScheduleLatency = summarise(scheduled[i])
		r.Reconciles = r.Reconciles && r.Phases[i].Reconciles
	}
	return r
}

func (r report) print(w io.Writer) {
	fmt.Fprintf(w, "target %s, %d senders, %d devices, %.1fs, %v CPUs\n",
		r.Config.Target, r.Config.Senders, r.Config.Devices, r.ElapsedSeconds, r.Environment["num_cpu"])
	if r.Config.Rate > 0 {
		fmt.Fprintf(w, "offered rate %.0f/s", r.Config.Rate)
		if r.Config.BurstRate > 0 {
			fmt.Fprintf(w, ", burst %.0f/s from %v for %v", r.Config.BurstRate, r.Config.BurstStart, r.Config.BurstDuration)
			if r.Config.SecondBurstStart > 0 {
				fmt.Fprintf(w, ", second burst from %v for %v", r.Config.SecondBurstStart, r.Config.BurstDuration)
			}
		}
		fmt.Fprintln(w)
	}
	fmt.Fprintf(w, "\nevents\n")
	fmt.Fprintf(w, "  offered            %d\n", r.Offered)
	fmt.Fprintf(w, "  accepted           %d  (%.0f/s)\n", r.Accepted, r.AcceptedPerSecond)
	fmt.Fprintf(w, "  duplicate          %d\n", r.Duplicate)
	fmt.Fprintf(w, "  rejected 400/409   %d / %d\n", r.Invalid, r.Conflict)
	fmt.Fprintf(w, "  overloaded 503     %d\n", r.Overloaded)
	fmt.Fprintf(w, "  other / transport  %d / %d\n", r.OtherStatus, r.TransportErr)
	fmt.Fprintf(w, "  sent / expired     %d / %d (strict-window expiration only)\n", r.Sent, r.Expired)
	fmt.Fprintf(w, "  starts/ACKs after offer window %d / %d\n", r.StartsAfterOffer, r.DurableACKsAfterOffer)
	fmt.Fprintf(w, "  not sent           %d  (scheduled arrivals dropped or expired before POST)\n", r.NotSent)
	fmt.Fprintf(w, "  retried attempts   %d\n", r.Retried)
	if r.Config.OfflinePercent > 0 {
		fmt.Fprintf(w, "  offline buffered/replayed/dropped/unflushed %d / %d / %d / %d\n", r.OfflineBuffered, r.OfflineReplayed, r.OfflineDropped, r.OfflineUnflushed)
	}
	fmt.Fprintf(w, "  counts add up      %v\n", r.Reconciles)
	fmt.Fprintf(w, "\nrequest latency (ms)   p50 %.1f  p95 %.1f  p99 %.1f  max %.1f\n",
		r.RequestLatency.P50, r.RequestLatency.P95, r.RequestLatency.P99, r.RequestLatency.Max)
	fmt.Fprintf(w, "  request mean       %.3fms\n", r.RequestMeanMS)
	fmt.Fprintf(w, "\nfalls\n")
	fmt.Fprintf(w, "  sent / accepted    %d / %d\n", r.FallsSent, r.FallsAccepted)
	fmt.Fprintf(w, "  delivered on feed  %d  (missing %d, duplicated %d)\n", r.FallsDelivered, r.FallsMissing, r.FallsDuplicated)
	fmt.Fprintf(w, "  post to feed (ms)  p50 %.1f  p95 %.1f  p99 %.1f  max %.1f\n",
		r.FallLatency.P50, r.FallLatency.P95, r.FallLatency.P99, r.FallLatency.Max)
	for _, p := range r.Phases {
		fmt.Fprintf(w, "\nphase %s [%.3f, %.3f)s: scheduled %d, sent %d, accepted %d (cohort %.0f/phase-second), not sent %d, starts in/after phase %d/%d, ACK in/after phase %d/%d, reconciles %v\n", p.Name, p.StartSeconds, p.EndSeconds, p.Scheduled, p.Sent, p.Accepted, p.AcceptedPerSecond, p.NotSent, p.StartsWithinPhase, p.StartsAfterPhase, p.AcceptedWithinPhase, p.AcceptedAfterPhase, p.Reconciles)
		fmt.Fprintf(w, "  schedule-to-send p95 %.3fms, schedule-to-ACK p95 %.3fms, generation-to-ACK p95 %.3fms\n", p.SendDelay.P95, p.ScheduleACK.P95, p.GenerationACK.P95)
		fmt.Fprintf(w, "  schedule-to-send max %.3fms, fall post-to-feed p95 %.3fms, fall schedule-to-feed p95 %.3fms\n", p.SendDelay.Max, p.FallPostLatency.P95, p.FallScheduleLatency.P95)
		fmt.Fprintf(w, "  actual starts/ACKs during phase %d / %d\n", p.ActualStarts, p.ActualDurableACKs)
	}
	if !r.ValidationRequested {
		fmt.Fprintln(w, "\nvalidation: NOT REQUESTED (diagnostic run; no pass claim)")
		return
	}
	fmt.Fprintf(w, "\nvalidation: %s\n", strings.ToUpper(r.ValidationStatus))
	for _, check := range r.ValidationChecks {
		fmt.Fprintf(w, "  %-22s %s  observed=%s; required=%s\n", check.Name, check.Status, check.Observed, check.Threshold)
	}
}

type phaseReport struct {
	ActualStarts          int64       `json:"actual_starts_during_phase"`
	ActualDurableACKs     int64       `json:"actual_durable_acks_during_phase"`
	Name                  string      `json:"name"`
	StartSeconds          float64     `json:"start_seconds"`
	EndSeconds            float64     `json:"end_seconds"`
	Rate                  float64     `json:"scheduled_rate"`
	Scheduled             int64       `json:"scheduled"`
	Sent                  int64       `json:"sent"`
	NotSent               int64       `json:"not_sent"`
	Accepted              int64       `json:"accepted"`
	Duplicate             int64       `json:"duplicate"`
	Invalid               int64       `json:"invalid_400"`
	Conflict              int64       `json:"conflict_409"`
	Overloaded            int64       `json:"overloaded_503"`
	OtherStatus           int64       `json:"other_status"`
	TransportErr          int64       `json:"transport_errors"`
	StartsWithinPhase     int64       `json:"starts_within_phase"`
	StartsAfterPhase      int64       `json:"starts_after_phase"`
	AcceptedWithinPhase   int64       `json:"durable_ack_within_phase"`
	AcceptedAfterPhase    int64       `json:"durable_ack_after_phase"`
	AcceptedPerSecond     float64     `json:"accepted_per_phase_second"`
	Reconciles            bool        `json:"reconciles"`
	SendDelay             percentiles `json:"schedule_to_send_latency"`
	GenerationACK         percentiles `json:"generation_to_ack_latency"`
	ScheduleACK           percentiles `json:"schedule_to_ack_latency"`
	FallPostLatency       percentiles `json:"fall_post_to_feed_latency"`
	FallGenerationLatency percentiles `json:"fall_generation_to_feed_latency"`
	FallScheduleLatency   percentiles `json:"fall_schedule_to_feed_latency"`
}
