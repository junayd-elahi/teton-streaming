package ingest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/junayd-elahi/teton-streaming/internal/store"
)

// With no writer running, the queue of one fills and the next submit must fail
// visibly as overloaded instead of waiting forever.
func TestSubmitReportsOverloadWhenQueueStaysFull(t *testing.T) {
	w := NewWriter(nil, Config{QueueSize: 1, BatchSize: 1, EnqueueTimeout: 20 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		_, err := w.Submit(ctx, store.Event{})
		first <- err
	}()
	for len(w.queue) == 0 {
		time.Sleep(time.Millisecond)
	}

	if _, err := w.Submit(context.Background(), store.Event{}); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("second submit: got %v, want ErrOverloaded", err)
	}
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("first submit: got %v, want context.Canceled", err)
	}
}
