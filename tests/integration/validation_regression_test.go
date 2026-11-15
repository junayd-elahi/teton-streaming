package integration

import (
	"net/http"
	"strings"
	"testing"
)

func TestEventRequestMustEndAfterOneCompleteDocument(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	for _, suffix := range []string{"]", "}", "null", "{}", strings.Repeat(" ", 70<<10)} {
		if got := post(t, svc, heartbeat+suffix); got != http.StatusBadRequest {
			t.Errorf("trailing suffix of %d bytes: status %d, want 400", len(suffix), got)
		}
	}
	if got := count(t, svc); got != 0 {
		t.Fatalf("invalid document stored %d events", got)
	}
	if got := post(t, svc, heartbeat+" \n\t"); got != http.StatusOK {
		t.Fatalf("valid trailing whitespace: status %d", got)
	}
}

func TestZeroExponentStorageKeepsRetryIdentity(t *testing.T) {
	svc := startService(t)
	defer svc.stop()
	body := strings.TrimSuffix(heartbeat, "}") + `,"extra":{"zero":0e2147483648,"nested":[0e-2147483648]}}`
	first := postReply(t, svc, body)
	if first.status != http.StatusOK || first.duplicate {
		t.Fatalf("zero-valued exponent: %+v", first)
	}
	retry := postReply(t, svc, body)
	if retry.status != http.StatusOK || !retry.duplicate {
		t.Fatalf("zero-valued exponent retry: %+v", retry)
	}
	if got := count(t, svc); got != 1 {
		t.Fatalf("retry stored %d rows, want 1", got)
	}
	// Lexically different content retains the original conflict semantics.
	changed := strings.Replace(body, "0e2147483648", "0e2147483647", 1)
	if got := post(t, svc, changed); got != http.StatusConflict {
		t.Fatalf("changed canonical input: status %d, want 409", got)
	}
}
