package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/junayd-elahi/teton-streaming/internal/api"
	"github.com/junayd-elahi/teton-streaming/internal/ingest"
	"github.com/junayd-elahi/teton-streaming/internal/metrics"
	"github.com/junayd-elahi/teton-streaming/internal/store"
	"github.com/junayd-elahi/teton-streaming/migrations"
)

// minReadConns is the number of pool connections kept free of writers.
const minReadConns = 2

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	databaseURL := mustEnv("DATABASE_URL")
	listenAddr := mustEnv("LISTEN_ADDR")
	cfg := ingest.Config{
		Writers:           mustEnvInt("INGEST_WRITERS"),
		QueueSize:         mustEnvInt("INGEST_QUEUE_SIZE"),
		PriorityQueueSize: mustEnvInt("INGEST_PRIORITY_QUEUE_SIZE"),
		BatchSize:         mustEnvInt("INGEST_BATCH_SIZE"),
		EnqueueTimeout:    time.Duration(mustEnvInt("INGEST_ENQUEUE_TIMEOUT_MS")) * time.Millisecond,
		CommitTimeout:     time.Duration(mustEnvInt("INGEST_COMMIT_TIMEOUT_MS")) * time.Millisecond,
	}

	ctx := context.Background()
	st, err := store.Open(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Migrate(ctx, migrations.Files); err != nil {
		return err
	}
	// Every writer holds a connection while it commits. Leave some for reads.
	if needed := cfg.Writers + 1 + minReadConns; st.MaxConns() < needed {
		return fmt.Errorf("database pool has %d connections, need at least %d for %d writers: set pool_max_conns in DATABASE_URL",
			st.MaxConns(), needed, cfg.Writers)
	}

	// The metrics read the queue depths from the writer, and the writer
	// reports to the metrics, so the depth funcs close over the variable.
	var writer *ingest.Writer
	m := metrics.NewService(
		func() int64 { normal, _ := writer.QueueDepths(); return int64(normal) },
		func() int64 { _, priority := writer.QueueDepths(); return int64(priority) },
		time.Now)
	writer = ingest.NewWriter(st, cfg, m)
	go writer.Run(ctx)

	server := &http.Server{
		Addr:    listenAddr,
		Handler: api.New(writer, st, m, time.Now).Handler(),
		// Bound how long a client may take to send a request, so slow or idle
		// connections cannot pile up. WriteTimeout must exceed the 30 s maximum
		// long-poll wait of the alarm feed.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      40 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	slog.Info("listening", "addr", listenAddr)
	return server.ListenAndServe()
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("missing required environment variable", "key", key)
		os.Exit(1)
	}
	return v
}

func mustEnvInt(key string) int {
	n, err := strconv.Atoi(mustEnv(key))
	if err != nil {
		slog.Error("environment variable must be an integer", "key", key)
		os.Exit(1)
	}
	return n
}
