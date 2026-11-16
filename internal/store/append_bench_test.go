package store

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/junayd-elahi/teton-streaming/migrations"
)

// BenchmarkAppendBatch runs only against an explicitly supplied PostgreSQL.
// Each case owns its schema and retains synchronous production commit behavior.
func BenchmarkAppendBatch(b *testing.B) {
	for _, tc := range []struct {
		name, typ string
		size      int
		replay    bool
	}{
		{"heartbeat1", "heartbeat", 1, false},
		{"heartbeat64", "heartbeat", 64, false},
		{"heartbeat256", "heartbeat", 256, false},
		{"fall64", "fall_warn", 64, false},
		{"replay64", "heartbeat", 64, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			raw := os.Getenv("DATABASE_URL")
			if raw == "" {
				b.Skip("DATABASE_URL required")
			}
			ctx := context.Background()
			conn, err := connectStoreTestDatabase(ctx, raw)
			if err != nil {
				b.Fatal(err)
			}
			defer conn.Close(ctx)
			schema := fmt.Sprintf("storage_bench_%d", time.Now().UnixNano())
			if _, err := conn.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
				b.Fatal(err)
			}
			defer conn.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
			testURL, err := storeTestURL(raw, schema)
			if err != nil {
				b.Fatal(err)
			}
			s, err := Open(ctx, testURL)
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			if err := s.Migrate(ctx, migrations.Files); err != nil {
				b.Fatal(err)
			}
			var syncCommit, fsync string
			if err := conn.QueryRow(ctx, "SHOW synchronous_commit").Scan(&syncCommit); err != nil {
				b.Fatal(err)
			}
			if err := conn.QueryRow(ctx, "SHOW fsync").Scan(&fsync); err != nil {
				b.Fatal(err)
			}
			if syncCommit != "on" || fsync != "on" {
				b.Fatalf("unsafe settings synchronous_commit=%s fsync=%s", syncCommit, fsync)
			}
			payload := []byte(`{"device_id":"dev_0100","seq":1,"room_id":"room_001","type":"heartbeat","ts":"2026-11-15T12:00:00Z","confidence":0.91}`)
			hash := sha256.Sum256(payload)
			events := make([]Event, tc.size)
			at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
			for i := range events {
				events[i] = Event{DeviceID: fmt.Sprintf("dev_%04d", i%5000), Seq: int64(i), RoomID: "room_001", Type: tc.typ, TS: at.Add(time.Duration(i) * time.Microsecond), Payload: payload, PayloadHash: hash[:], ReceivedAt: at}
			}
			if tc.replay {
				if _, err := s.AppendBatch(ctx, events); err != nil {
					b.Fatal(err)
				}
			}
			var walStart string
			if err := conn.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()::text").Scan(&walStart); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if !tc.replay {
					for j := range events {
						events[j].Seq = int64(i*tc.size + j)
						events[j].TS = at.Add(time.Duration(i*tc.size+j) * time.Microsecond)
					}
				}
				result, err := s.AppendBatch(ctx, events)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Outcomes) != tc.size {
					b.Fatal("wrong outcome count")
				}
				expected := Inserted
				if tc.replay {
					expected = Duplicate
				}
				for _, out := range result.Outcomes {
					if out != expected {
						b.Fatalf("outcome %v expected %v", out, expected)
					}
				}
			}
			b.StopTimer()
			var rows, relationBytes, walBytes int64
			if err := conn.QueryRow(ctx, "SELECT count(*), pg_total_relation_size($1::regclass) FROM "+pgx.Identifier{schema, "event_log"}.Sanitize(), schema+".event_log").Scan(&rows, &relationBytes); err != nil {
				b.Fatal(err)
			}
			if err := conn.QueryRow(ctx, "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), $1::pg_lsn)::bigint", walStart).Scan(&walBytes); err != nil {
				b.Fatal(err)
			}
			if !tc.replay {
				b.ReportMetric(float64(relationBytes)/float64(rows), "relationB/event")
				b.ReportMetric(float64(walBytes)/float64(rows), "WALB/event")
			}
		})
	}
}
