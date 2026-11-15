package ingest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/junayd-elahi/teton-streaming/internal/store"
)

type committedAlarmWithLookupFailure struct{}

func (committedAlarmWithLookupFailure) AppendBatch(context.Context, []store.Event) (store.BatchResult, error) {
	return store.BatchResult{NewAlarms: 1}, errors.New("classification unavailable after commit")
}

func TestCommittedAlarmIsAnnouncedWhenClassificationFails(t *testing.T) {
	w := NewWriter(committedAlarmWithLookupFailure{}, Config{
		Writers: 1, QueueSize: 1, PriorityQueueSize: 1, BatchSize: 1,
		EnqueueTimeout: time.Second, CommitTimeout: time.Second,
	}, testMetrics())
	signal := w.NewAlarms()
	done := make(chan result, 1)
	w.commit(context.Background(), []request{{event: store.Event{Type: "fall_warn"}, done: done}})
	if reply := <-done; reply.err == nil {
		t.Fatal("caller must receive the classification failure")
	}
	select {
	case <-signal:
	default:
		t.Fatal("committed alarm did not wake feed readers")
	}
}
