package main

import (
	"fmt"
	"io"
	"slices"
	"time"
)

type percentiles struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
}

// summarise sorts the samples and reads exact percentiles from them. Samples
// are four bytes each, so a five-minute run at 50,000 events/s fits in 60 MB;
// exact values are worth that over the error of a bucketed histogram.
func summarise(micros []uint32) percentiles {
	if len(micros) == 0 {
		return percentiles{}
	}
	slices.Sort(micros)
	at := func(q float64) float64 {
		return float64(micros[int(q*float64(len(micros)-1))]) / 1000
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
}
