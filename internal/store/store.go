// Package store owns all PostgreSQL access: schema migration and the event journal.
package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrConflict means a (device_id, seq) identity was reused with different content.
var ErrConflict = errors.New("source identity reused with different payload")

// Event is one validated source event ready to be journaled.
type Event struct {
	DeviceID    string
	Seq         int64
	RoomID      string
	Type        string
	TS          time.Time
	Payload     []byte // canonical JSON
	PayloadHash []byte // sha256 of Payload
	ReceivedAt  time.Time
}

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// Migrate applies every embedded .sql file not yet recorded in schema_migrations,
// in filename order, each in its own transaction.
func (s *Store) Migrate(ctx context.Context, files fs.FS) error {
	if _, err := s.pool.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY)`); err != nil {
		return err
	}
	names, err := fs.Glob(files, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		if err := s.applyMigration(ctx, files, name); err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
	}
	return nil
}

func (s *Store) applyMigration(ctx context.Context, files fs.FS, name string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var applied bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, name).Scan(&applied); err != nil {
		return err
	}
	if applied {
		return nil
	}
	sql, err := fs.ReadFile(files, name)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, string(sql)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, name); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Append commits the event to the journal. It returns inserted=false for an exact
// retry of an already-journaled event, and ErrConflict when the identity exists
// with different content.
func (s *Store) Append(ctx context.Context, e Event) (inserted bool, err error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO event_log (device_id, seq, room_id, type, ts, payload, payload_hash, received_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (device_id, seq) DO NOTHING`,
		e.DeviceID, e.Seq, e.RoomID, e.Type, e.TS, e.Payload, e.PayloadHash, e.ReceivedAt)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 1 {
		return true, nil
	}
	var existing []byte
	if err := s.pool.QueryRow(ctx,
		`SELECT payload_hash FROM event_log WHERE device_id = $1 AND seq = $2`,
		e.DeviceID, e.Seq).Scan(&existing); err != nil {
		return false, err
	}
	if !bytes.Equal(existing, e.PayloadHash) {
		return false, ErrConflict
	}
	return false, nil
}

// CountEvents returns the number of journaled events.
func (s *Store) CountEvents(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM event_log`).Scan(&n)
	return n, err
}

// Reset empties the journal. Used by integration tests only.
func (s *Store) Reset(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `TRUNCATE event_log RESTART IDENTITY`)
	return err
}
