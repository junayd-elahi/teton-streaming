package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRequestedValidationFailsDespiteReconciledCounters(t *testing.T) {
	r := report{
		Config:     config{Rate: 100, RequireFullOffer: true, RequireNoLoss: true, MinAcceptedRate: 50, MaxRequestP95MS: 100},
		Reconciles: true, NotSent: 20, AcceptedPerSecond: 10,
		FallsAccepted: 4, FallsMissing: 1,
		RequestLatency: percentiles{Count: 80, P95: 200},
	}
	r.evaluateValidation()
	if !r.Reconciles {
		t.Fatal("test setup must have reconciled counters")
	}
	if !r.ValidationRequested || r.ValidationPassed || r.ValidationStatus != "unmet" {
		t.Fatalf("validation should fail independently of reconciliation: %+v", r)
	}
	if len(r.ValidationChecks) != 5 {
		t.Fatalf("got %d checks, want reconciliation plus four requested criteria", len(r.ValidationChecks))
	}
	for _, check := range r.ValidationChecks {
		if check.Name != "counts_reconcile" && check.Status != "UNMET" {
			t.Errorf("%s status %q, want UNMET", check.Name, check.Status)
		}
	}
	var out bytes.Buffer
	r.print(&out)
	if !strings.Contains(out.String(), "validation: UNMET") {
		t.Fatalf("report does not show failed requested validation:\n%s", out.String())
	}
}

func TestDiagnosticRunDoesNotClaimValidationPass(t *testing.T) {
	r := report{Config: config{}, Reconciles: true}
	r.evaluateValidation()
	if r.ValidationRequested || r.ValidationPassed || r.ValidationStatus != "diagnostic" {
		t.Fatalf("unexpected diagnostic status: %+v", r)
	}
	var out bytes.Buffer
	r.print(&out)
	if !strings.Contains(out.String(), "NOT REQUESTED (diagnostic run; no pass claim)") {
		t.Fatalf("diagnostic status is not explicit:\n%s", out.String())
	}
}

func TestRequestedValidationCanPass(t *testing.T) {
	r := report{
		Config:     config{Rate: 100, RequireFullOffer: true, RequireNoLoss: true, MinAcceptedRate: 50, MaxRequestP95MS: 100},
		Reconciles: true, Offered: 100, AcceptedPerSecond: 75,
		FallsAccepted: 2, FallsMissing: 0,
		RequestLatency: percentiles{Count: 100, P95: 20},
	}
	r.evaluateValidation()
	if !r.ValidationPassed || r.ValidationStatus != "met" {
		t.Fatalf("criteria should pass: %+v", r)
	}
}

func TestFullOfferValidationRequiresActualEvents(t *testing.T) {
	r := report{Config: config{Rate: 0.01, RequireFullOffer: true}, Reconciles: true}
	r.evaluateValidation()
	if r.ValidationPassed {
		t.Fatal("a zero-event schedule cannot validate a full offer")
	}
}

func TestSummariseInterpolatesTailForSmallSamples(t *testing.T) {
	got := summarise([]uint32{100_000, 0})
	if got.P50 != 50 || got.P95 != 95 || got.P99 != 99 {
		t.Fatalf("type-7 percentiles for [0ms, 100ms] = %+v, want p50=50 p95=95 p99=99", got)
	}
}

func TestFallLatencyValidationUsesAggregateP95AndRequiresSamples(t *testing.T) {
	late := report{
		Config:      config{MaxFallP95MS: 100},
		Reconciles:  true,
		FallLatency: percentiles{Count: 10, P95: 150},
	}
	late.evaluateValidation()
	if late.ValidationPassed || late.ValidationStatus != "unmet" || len(late.ValidationChecks) != 2 {
		t.Fatalf("aggregate fall p95 above threshold should fail: %+v", late)
	}
	if got := late.ValidationChecks[1]; got.Name != "fall_post_to_feed_p95" || got.Status != "UNMET" || !strings.Contains(got.Threshold, "aggregate p95") {
		t.Fatalf("unexpected fall latency check: %+v", got)
	}

	empty := report{Config: config{MaxFallP95MS: 100}, Reconciles: true}
	empty.evaluateValidation()
	if empty.ValidationPassed || empty.ValidationStatus != "unmet" {
		t.Fatalf("zero fall samples must not pass latency validation: %+v", empty)
	}
	if got := empty.ValidationChecks[1]; !strings.Contains(got.Observed, "count=0") {
		t.Fatalf("zero-sample result not reported: %+v", got)
	}
}
