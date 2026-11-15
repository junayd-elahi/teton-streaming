package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/junayd-elahi/teton-streaming/internal/api"
	"github.com/junayd-elahi/teton-streaming/internal/store"
	"github.com/junayd-elahi/teton-streaming/migrations"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	databaseURL := mustEnv("DATABASE_URL")
	listenAddr := mustEnv("LISTEN_ADDR")

	ctx := context.Background()
	st, err := store.Open(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Migrate(ctx, migrations.Files); err != nil {
		return err
	}

	slog.Info("listening", "addr", listenAddr)
	return http.ListenAndServe(listenAddr, api.New(st, time.Now).Handler())
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("missing required environment variable", "key", key)
		os.Exit(1)
	}
	return v
}
