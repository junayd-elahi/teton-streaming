package integration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
)

var testDatabaseURLs sync.Map

// Each test owns one random schema. Reopening a service within that test
// reuses its schema so restart tests retain their committed state.
func isolatedDatabaseURL(t *testing.T) string {
	t.Helper()
	if url, ok := testDatabaseURLs.Load(t); ok {
		return url.(string)
	}
	baseURL := os.Getenv("DATABASE_URL")
	if baseURL == "" {
		t.Fatal("DATABASE_URL is required for integration tests")
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	schema := "teton_test_" + hex.EncodeToString(random[:])
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, baseURL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize())
	conn.Close(ctx)
	if err != nil {
		t.Fatal(err)
	}
	testURL := baseURL + " search_path=" + schema
	if parsed, err := url.Parse(baseURL); err == nil && (parsed.Scheme == "postgres" || parsed.Scheme == "postgresql") {
		query := parsed.Query()
		query.Set("search_path", schema)
		parsed.RawQuery = query.Encode()
		testURL = parsed.String()
	}
	testDatabaseURLs.Store(t, testURL)
	t.Cleanup(func() {
		testDatabaseURLs.Delete(t)
		conn, err := pgx.Connect(ctx, baseURL)
		if err != nil {
			t.Errorf("cleaning test schema: %v", err)
			return
		}
		defer conn.Close(ctx)
		if _, err := conn.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Errorf("cleaning test schema: %v", err)
		}
	})
	return testURL
}

func TestIntegrationServicesHaveIsolatedState(t *testing.T) {
	for _, room := range []string{"room_first", "room_second"} {
		t.Run(room, func(t *testing.T) {
			svc := startService(t)
			defer svc.stop()
			body := strings.Replace(heartbeat, "room_001", room, 1)
			if got := post(t, svc, body); got != http.StatusOK {
				t.Fatalf("isolated event: status %d", got)
			}
			if got := count(t, svc); got != 1 {
				t.Fatalf("isolated schema has %d events, want 1", got)
			}
		})
	}
}
