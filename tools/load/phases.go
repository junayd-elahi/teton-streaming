package main

import (
	"fmt"
	"time"
)

const maxPhases = 5

// Phases follow configured rate boundaries, not completion time. A slow burst's
// late acknowledgements stay in the burst's latency cohort.
type phase struct {
	name       string
	start, end time.Duration
	rate       float64
}

func validateSecondBurst(cfg config) error {
	if cfg.SecondBurstStart == 0 {
		return nil
	}
	if cfg.BurstRate <= 0 || cfg.BurstDuration <= 0 || cfg.BurstStart >= cfg.Duration {
		return fmt.Errorf("-second-burst-start requires a configured first burst")
	}
	if cfg.SecondBurstStart >= cfg.Duration {
		return fmt.Errorf("-second-burst-start must be before -duration")
	}
	firstEnd := cfg.BurstStart + cfg.BurstDuration
	if cfg.SecondBurstStart <= firstEnd {
		return fmt.Errorf("-second-burst-start must leave a positive gap after the first burst")
	}
	return nil
}

// fillPhasePlan writes at most five phases to caller-owned fixed storage.
// Runtime paths use this helper directly, avoiding per-event allocations.
func fillPhasePlan(cfg config, out *[maxPhases]phase) int {
	if cfg.BurstRate <= 0 || cfg.BurstDuration <= 0 || cfg.BurstStart >= cfg.Duration {
		out[0] = phase{name: "steady", end: cfg.Duration, rate: cfg.Rate}
		return 1
	}

	firstEnd := min(cfg.Duration, cfg.BurstStart+cfg.BurstDuration)
	count := 0
	if cfg.BurstStart > 0 {
		out[count] = phase{name: "baseline", start: 0, end: cfg.BurstStart, rate: cfg.Rate}
		count++
	}
	firstName := "burst"
	if cfg.SecondBurstStart > 0 {
		firstName = "burst_1"
	}
	out[count] = phase{name: firstName, start: cfg.BurstStart, end: firstEnd, rate: cfg.BurstRate}
	count++

	if cfg.SecondBurstStart > 0 {
		out[count] = phase{name: "between_bursts", start: firstEnd, end: cfg.SecondBurstStart, rate: cfg.Rate}
		count++
		secondEnd := min(cfg.Duration, cfg.SecondBurstStart+cfg.BurstDuration)
		out[count] = phase{name: "burst_2", start: cfg.SecondBurstStart, end: secondEnd, rate: cfg.BurstRate}
		count++
		if secondEnd < cfg.Duration {
			out[count] = phase{name: "post_burst", start: secondEnd, end: cfg.Duration, rate: cfg.Rate}
			count++
		}
		return count
	}

	if firstEnd < cfg.Duration {
		out[count] = phase{name: "post_burst", start: firstEnd, end: cfg.Duration, rate: cfg.Rate}
		count++
	}
	return count
}

// phases returns descriptions for report construction only. Scheduling paths
// use fillPhasePlan with fixed arrays instead.
func phases(cfg config) []phase {
	var plan [maxPhases]phase
	count := fillPhasePlan(cfg, &plan)
	return plan[:count]
}

func phaseAt(cfg config, elapsed time.Duration) int {
	var plan [maxPhases]phase
	count := fillPhasePlan(cfg, &plan)
	for i := 0; i < count; i++ {
		if elapsed < plan[i].end {
			return i
		}
	}
	return count - 1
}

func phaseEnd(cfg config, index int) time.Duration {
	var plan [maxPhases]phase
	count := fillPhasePlan(cfg, &plan)
	if index >= 0 && index < count {
		return plan[index].end
	}
	return cfg.Duration
}

func (c *eventCounts) add(d eventCounts) {
	c.Accepted += d.Accepted
	c.Duplicate += d.Duplicate
	c.Invalid += d.Invalid
	c.Conflict += d.Conflict
	c.Overloaded += d.Overloaded
	c.OtherStatus += d.OtherStatus
	c.TransportErr += d.TransportErr
	c.Retried += d.Retried
}
func (c eventCounts) difference(d eventCounts) eventCounts {
	return eventCounts{Accepted: c.Accepted - d.Accepted, Duplicate: c.Duplicate - d.Duplicate,
		Invalid: c.Invalid - d.Invalid, Conflict: c.Conflict - d.Conflict, Overloaded: c.Overloaded - d.Overloaded,
		OtherStatus: c.OtherStatus - d.OtherStatus, TransportErr: c.TransportErr - d.TransportErr, Retried: c.Retried - d.Retried}
}
