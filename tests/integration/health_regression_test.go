package integration

import (
	"testing"
	"time"
)

func TestOldHeartbeatReplayCannotRefreshDeviceAvailability(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	for i := 0; i < 300; i++ {
		ts := fixedNow.Add(-10*time.Minute - time.Duration(i)*time.Second)
		mustPost(t, svc, heartbeatAt("dev_replayed", i+1, ts.Format(time.RFC3339Nano)))
	}
	var got health
	getJSON(t, svc, "/devices/dev_replayed/health", &got)
	if got.Availability5m != 0 || got.ClockOffsetSeconds != 0 {
		t.Fatalf("old replay refreshed health: %+v", got)
	}
}

func TestFutureOutlierCannotHideSubsequentHealthyHeartbeats(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	end := fixedNow.Add(5 * time.Minute)
	svc.clock = end
	mustPost(t, svc, heartbeatAt("dev_outlier", 999, end.Add(59*time.Minute).Format(time.RFC3339Nano)))
	for i := 299; i >= 0; i-- {
		at := end.Add(-time.Duration(i) * time.Second)
		postLive(t, svc, "dev_outlier", 300-i, at.Format(time.RFC3339Nano), 0)
	}
	svc.clock = end
	var got health
	getJSON(t, svc, "/devices/dev_outlier/health", &got)
	if got.Availability5m != 1 || got.ClockOffsetSeconds != 0 {
		t.Fatalf("future outlier poisoned calibration: %+v", got)
	}
}

func TestNormalClockSkewWithTransportDelayRetainsCoverage(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	end := fixedNow.Add(5 * time.Minute)
	for _, skew := range []time.Duration{-30 * time.Second, 30 * time.Second} {
		device := "dev_ahead_delayed"
		if skew < 0 {
			device = "dev_behind_delayed"
		}
		for i := 299; i >= 0; i-- {
			at := end.Add(-time.Duration(i)*time.Second + skew)
			postLive(t, svc, device, 300-i, at.Format(time.RFC3339Nano), -skew+5*time.Millisecond)
		}
		svc.clock = end.Add(5 * time.Millisecond)
		var got health
		getJSON(t, svc, "/devices/"+device+"/health", &got)
		if got.Availability5m != 1 {
			t.Fatalf("normal skew plus delay lost coverage: %+v", got)
		}
	}
}
