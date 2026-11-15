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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/junayd-elahi/teton-streaming/internal/aggregation"
)

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

// IsUnstorable reports whether err means PostgreSQL refused the data itself
// (a value it cannot represent or index), as opposed to being unavailable.
// Retrying such an event can never succeed.
func IsUnstorable(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	class := pgErr.Code[:2]
	return class == "22" || class == "54" // data exception, program limit exceeded
}

// Outcome says what happened to one event in a committed batch.
type Outcome int

const (
	Inserted  Outcome = iota // new journal row
	Duplicate                // exact retry of an already-journaled event, no effect
	Conflict                 // identity already journaled with different content
)

// BatchResult reports what one committed batch did.
type BatchResult struct {
	Outcomes  []Outcome // one per event, in batch order
	NewAlarms int       // logical falls first seen in this batch
}

// AppendBatch commits the events to the journal in one transaction, in slice
// order, and reports one Outcome per event. Every newly journaled fall_warn
// also creates its alarm in the same transaction, unless an alarm for the same
// logical fall already exists. A (device_id, seq) identity that is
// already journaled, or that appears earlier in the same batch, is not inserted
// again: it is a Duplicate if its payload hash matches the journaled row and a
// Conflict otherwise.
func (s *Store) AppendBatch(ctx context.Context, events []Event) (BatchResult, error) {
	n := len(events)
	deviceIDs := make([]string, n)
	seqs := make([]int64, n)
	roomIDs := make([]string, n)
	types := make([]string, n)
	tss := make([]time.Time, n)
	payloads := make([]string, n)
	hashes := make([][]byte, n)
	receivedAts := make([]time.Time, n)
	for i, e := range events {
		deviceIDs[i], seqs[i], roomIDs[i], types[i] = e.DeviceID, e.Seq, e.RoomID, e.Type
		tss[i], payloads[i], hashes[i], receivedAts[i] = e.TS, string(e.Payload), e.PayloadHash, e.ReceivedAt
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return BatchResult{}, err
	}
	defer tx.Rollback(ctx)

	// Insert in arrival order so journal ids follow it, then derive alarms
	// from the rows this statement actually inserted.
	var newAlarms int
	if err := tx.QueryRow(ctx, `
		WITH inserted AS (
			INSERT INTO event_log (device_id, seq, room_id, type, ts, payload, payload_hash, received_at)
			SELECT device_id, seq, room_id, type, ts, payload::jsonb, payload_hash, received_at
			FROM unnest($1::text[], $2::bigint[], $3::text[], $4::text[], $5::timestamptz[],
			            $6::text[], $7::bytea[], $8::timestamptz[])
			     WITH ORDINALITY AS t(device_id, seq, room_id, type, ts, payload, payload_hash, received_at, ord)
			ORDER BY ord
			ON CONFLICT (device_id, seq) DO NOTHING
			RETURNING id, device_id, seq, room_id, type, ts, payload, received_at
		), new_alarms AS (
			INSERT INTO alarms (device_id, room_id, ts, confidence, seq, received_at)
			SELECT device_id, room_id, ts, (payload->>'confidence')::float8, seq, received_at
			FROM inserted
			WHERE type = 'fall_warn'
			ORDER BY id
			ON CONFLICT (device_id, room_id, ts, confidence) DO NOTHING
			RETURNING 1
		)
		SELECT count(*) FROM new_alarms`,
		deviceIDs, seqs, roomIDs, types, tss, payloads, hashes, receivedAts).Scan(&newAlarms); err != nil {
		return BatchResult{}, err
	}

	// Read back the journaled row for every identity in the batch. A row whose id
	// is new in this transaction (xmin = current transaction) was inserted by it.
	type journaled struct {
		hash        []byte
		insertedNow bool
	}
	type identity struct {
		deviceID string
		seq      int64
	}
	rows, err := tx.Query(ctx, `
		SELECT e.device_id, e.seq, e.payload_hash, e.xmin::text = pg_current_xact_id()::xid::text
		FROM event_log e
		JOIN (SELECT DISTINCT * FROM unnest($1::text[], $2::bigint[]) AS k(device_id, seq)) k
		  ON e.device_id = k.device_id AND e.seq = k.seq`,
		deviceIDs, seqs)
	if err != nil {
		return BatchResult{}, err
	}
	stored := make(map[identity]journaled, n)
	for rows.Next() {
		var id identity
		var j journaled
		if err := rows.Scan(&id.deviceID, &id.seq, &j.hash, &j.insertedNow); err != nil {
			rows.Close()
			return BatchResult{}, err
		}
		stored[id] = j
	}
	if err := rows.Err(); err != nil {
		return BatchResult{}, err
	}

	outcomes := make([]Outcome, n)
	claimed := make(map[identity]bool, n) // identities whose new row is already attributed
	for i, e := range events {
		id := identity{e.DeviceID, e.Seq}
		j, ok := stored[id]
		if !ok {
			return BatchResult{}, fmt.Errorf("event %s/%d missing from journal after insert", e.DeviceID, e.Seq)
		}
		switch {
		case !bytes.Equal(j.hash, e.PayloadHash):
			outcomes[i] = Conflict
		case j.insertedNow && !claimed[id]:
			outcomes[i] = Inserted
			claimed[id] = true
		default:
			outcomes[i] = Duplicate
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return BatchResult{}, err
	}
	return BatchResult{Outcomes: outcomes, NewAlarms: newAlarms}, nil
}

// CountEvents returns the number of journaled events.
func (s *Store) CountEvents(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM event_log`).Scan(&n)
	return n, err
}

// Reset empties the journal and alarms. Used by integration tests only.
func (s *Store) Reset(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `TRUNCATE event_log, alarms RESTART IDENTITY`)
	return err
}

// HeartbeatWindow is the length of the device availability window.
const HeartbeatWindow = 300 * time.Second

// DeviceHealth returns the latest heartbeat at or before `at`, and how many
// distinct one-second slots of (at - 5 minutes, at] contain a heartbeat. Slots
// are counted back from `at`, so there are exactly 300 of them. Both use the
// device's own event time.
func (s *Store) DeviceHealth(ctx context.Context, deviceID string, at time.Time) (last *time.Time, slots int, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT
			(SELECT max(ts) FROM event_log
			 WHERE type = 'heartbeat' AND device_id = $1 AND ts <= $2),
			(SELECT count(DISTINCT floor(extract(epoch FROM ($2::timestamptz - ts))))
			 FROM event_log
			 WHERE type = 'heartbeat' AND device_id = $1
			   AND ts > $2::timestamptz - make_interval(secs => $3) AND ts <= $2)`,
		deviceID, at, HeartbeatWindow.Seconds()).Scan(&last, &slots)
	return last, slots, err
}

// PresenceTimeline returns the room's last presence transition at or before
// start (nil if none) and every transition in (start, end], ordered by event
// time, then device_id, then seq.
func (s *Store) PresenceTimeline(ctx context.Context, roomID string, start, end time.Time) (*aggregation.Transition, []aggregation.Transition, error) {
	var predecessor *aggregation.Transition
	var p aggregation.Transition
	err := s.pool.QueryRow(ctx, `
		SELECT ts, (payload->>'in_room')::boolean FROM event_log
		WHERE type = 'presence' AND room_id = $1 AND ts <= $2
		ORDER BY ts DESC, device_id DESC, seq DESC
		LIMIT 1`, roomID, start).Scan(&p.At, &p.InRoom)
	switch {
	case err == nil:
		predecessor = &p
	case !errors.Is(err, pgx.ErrNoRows):
		return nil, nil, err
	}

	rows, err := s.pool.Query(ctx, `
		SELECT ts, (payload->>'in_room')::boolean FROM event_log
		WHERE type = 'presence' AND room_id = $1 AND ts > $2 AND ts <= $3
		ORDER BY ts, device_id, seq`, roomID, start, end)
	if err != nil {
		return nil, nil, err
	}
	transitions, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (aggregation.Transition, error) {
		var t aggregation.Transition
		err := row.Scan(&t.At, &t.InRoom)
		return t, err
	})
	if err != nil {
		return nil, nil, err
	}
	return predecessor, transitions, nil
}

// Alarm is one logical fall.
type Alarm struct {
	ID         int64 // feed cursor, increasing in commit order
	DeviceID   string
	RoomID     string
	TS         time.Time // the device's original event time
	Confidence float64
	Seq        int64 // sequence number of the first event seen for this fall
	ReceivedAt time.Time
}

const alarmColumns = `id, device_id, room_id, ts, confidence, seq, received_at`

func scanAlarm(row pgx.CollectableRow) (Alarm, error) {
	var a Alarm
	err := row.Scan(&a.ID, &a.DeviceID, &a.RoomID, &a.TS, &a.Confidence, &a.Seq, &a.ReceivedAt)
	return a, err
}

// AlarmsSince returns every alarm with event time at or after since, ordered
// by event time.
func (s *Store) AlarmsSince(ctx context.Context, since time.Time) ([]Alarm, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+alarmColumns+` FROM alarms WHERE ts >= $1 ORDER BY ts, id`, since)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanAlarm)
}

// AlarmsAfter returns up to limit alarms committed after the cursor, in commit
// order. A late fall gets a new, higher id even though its event time is old,
// so a consumer resuming from its cursor still receives it.
func (s *Store) AlarmsAfter(ctx context.Context, cursor int64, limit int) ([]Alarm, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+alarmColumns+` FROM alarms WHERE id > $1 ORDER BY id LIMIT $2`, cursor, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanAlarm)
}
