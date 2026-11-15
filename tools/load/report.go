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
// samples take four bytes each: five minutes at 50,000 events/s is 60 MB of
// sample data; peak memory is higher because report construction aggregates
// the per-sender slices into another slice.
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

	Offered      int64 `json:"offered"`
	NotSent      int64 `json:"not_sent"`
	Accepted     int64 `json:"accepted"`
	Duplicate    int64 `json:"duplicate"`
	Invalid      int64 `json:"invalid_400"`
	Conflict     int64 `json:"conflict_409"`
	Overloaded   int64 `json:"overloaded_503"`
	OtherStatus  int64 `json:"other_status"`
	TransportErr int64 `json:"transport_errors"`
	Retried      int64 `json:"retried_attempts"`
	Reconciles   bool  `json:"reconciles"`

	AcceptedPerSecond float64     `json:"accepted_per_second"`
	RequestLatency    percentiles `json:"request_latency"`

	FallsSent       int         `json:"falls_sent"`
	FallsAccepted   int         `json:"falls_accepted"`
	FallsDelivered  int         `json:"falls_delivered"`
	FallsMissing    int64       `json:"falls_missing"`
	FallsDuplicated int64       `json:"falls_duplicated"`
	FallLatency     percentiles `json:"fall_post_to_feed_latency"`

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
	return cfg.RequireFullOffer || cfg.RequireNoLoss || cfg.MinAcceptedRate > 0 || cfg.MaxRequestP95MS > 0 || cfg.MaxFallP95MS > 0
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
	if r.ValidationPassed {
		r.ValidationStatus = "met"
	} else {
		r.ValidationStatus = "unmet"
	}
}

func buildReport(cfg config, started time.Time, elapsed time.Duration, senders []*senderStats, notSent int64, falls *fallTracker) report {
	r := report{Started: started, ElapsedSeconds: elapsed.Seconds(), Config: cfg, Environment: environment(), NotSent: notSent}
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
		latencies = append(latencies, s.latenciesMicros...)
	}
	sent := r.Accepted + r.Duplicate + r.Invalid + r.Conflict + r.Overloaded + r.OtherStatus + r.TransportErr
	r.Offered = sent + notSent
	if cfg.Rate > 0 {
		// Paced: what was offered comes from the schedule, independently of what the senders did.
		r.Offered = int64(scheduled(cfg, cfg.Duration))
	}
	r.Reconciles = r.Offered == sent+notSent
	r.AcceptedPerSecond = float64(r.Accepted) / elapsed.Seconds()
	r.RequestLatency = summarise(latencies)

	r.FallsSent = len(falls.sentAt)
	r.FallsAccepted = len(falls.acceptedIDs)
	r.FallsDelivered = len(falls.deliveredAt)
	r.FallsMissing = falls.missing()
	r.FallsDuplicated = falls.duplicates
	r.FallLatency = summarise(falls.latenciesMicros())
	return r
}

func (r report) print(w io.Writer) {
	fmt.Fprintf(w, "target %s, %d senders, %d devices, %.1fs, %v CPUs\n",
		r.Config.Target, r.Config.Senders, r.Config.Devices, r.ElapsedSeconds, r.Environment["num_cpu"])
	if r.Config.Rate > 0 {
		fmt.Fprintf(w, "offered rate %.0f/s", r.Config.Rate)
		if r.Config.BurstRate > 0 {
			fmt.Fprintf(w, ", burst %.0f/s from %v for %v", r.Config.BurstRate, r.Config.BurstStart, r.Config.BurstDuration)
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
	fmt.Fprintf(w, "  not sent           %d  (scheduled, but no sender was free)\n", r.NotSent)
	fmt.Fprintf(w, "  retried attempts   %d\n", r.Retried)
	fmt.Fprintf(w, "  counts add up      %v\n", r.Reconciles)
	fmt.Fprintf(w, "\nrequest latency (ms)   p50 %.1f  p95 %.1f  p99 %.1f  max %.1f\n",
		r.RequestLatency.P50, r.RequestLatency.P95, r.RequestLatency.P99, r.RequestLatency.Max)
	fmt.Fprintf(w, "\nfalls\n")
	fmt.Fprintf(w, "  sent / accepted    %d / %d\n", r.FallsSent, r.FallsAccepted)
	fmt.Fprintf(w, "  delivered on feed  %d  (missing %d, duplicated %d)\n", r.FallsDelivered, r.FallsMissing, r.FallsDuplicated)
	fmt.Fprintf(w, "  post to feed (ms)  p50 %.1f  p95 %.1f  p99 %.1f  max %.1f\n",
		r.FallLatency.P50, r.FallLatency.P95, r.FallLatency.P99, r.FallLatency.Max)
	if !r.ValidationRequested {
		fmt.Fprintln(w, "\nvalidation: NOT REQUESTED (diagnostic run; no pass claim)")
		return
	}
	fmt.Fprintf(w, "\nvalidation: %s\n", strings.ToUpper(r.ValidationStatus))
	for _, check := range r.ValidationChecks {
		fmt.Fprintf(w, "  %-22s %s  observed=%s; required=%s\n", check.Name, check.Status, check.Observed, check.Threshold)
	}
}
