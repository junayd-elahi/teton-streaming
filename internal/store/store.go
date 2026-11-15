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
	TS          time.Time // microsecond precision, as PostgreSQL stores it
	Payload     []byte    // canonical JSON
	PayloadHash []byte    // sha256 of Payload
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

// MaxConns is the size of the connection pool.
func (s *Store) MaxConns() int { return int(s.pool.Config().MaxConns) }

// Ping checks that the database answers.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

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

// AppendBatch commits the events to the journal atomically, in slice order, and reports one Outcome per event. Every newly journaled fall_warn
// also creates its alarm in the same transaction, unless an alarm for the same
// logical fall already exists. An identity (device_id, seq, ts) that is
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

	// One statement, so the events and their alarms commit together or not at
	// all. It inserts in arrival order, derives alarms from the rows it
	// actually inserted, and returns the identities of those rows.
	rows, err := s.pool.Query(ctx, `
		WITH inserted AS (
			INSERT INTO event_log (device_id, seq, room_id, type, ts, payload, payload_hash, received_at)
			SELECT device_id, seq, room_id, type, ts, payload::jsonb, payload_hash, received_at
			FROM unnest($1::text[], $2::bigint[], $3::text[], $4::text[], $5::timestamptz[],
			            $6::text[], $7::bytea[], $8::timestamptz[])
			     WITH ORDINALITY AS t(device_id, seq, room_id, type, ts, payload, payload_hash, received_at, ord)
			ORDER BY ord
			ON CONFLICT (device_id, seq, ts) DO NOTHING
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
		SELECT device_id, seq, ts, (SELECT count(*) FROM new_alarms) FROM inserted`,
		deviceIDs, seqs, roomIDs, types, tss, payloads, hashes, receivedAts)
	if err != nil {
		return BatchResult{}, err
	}
	var newAlarms int
	inserted := make(map[identity]bool, n)
	for rows.Next() {
		var id identity
		var ts time.Time
		if err := rows.Scan(&id.deviceID, &id.seq, &ts, &newAlarms); err != nil {
			rows.Close()
			return BatchResult{}, err
		}
		id.tsMicros = ts.UnixMicro()
		inserted[id] = true
	}
	if err := rows.Err(); err != nil {
		return BatchResult{}, err
	}

	// Classify each event. The first event for an identity this batch inserted
	// is the new row. A later copy in the same batch is compared with that
	// first one. An identity the batch did not insert was journaled before, and
	// only those need a look at the stored hash.
	outcomes := make([]Outcome, n)
	firstHash := make(map[identity][]byte, len(inserted))
	var earlier []int // indexes of events whose identity was journaled before this batch
	for i, e := range events {
		id := identityOf(e)
		switch first, claimed := firstHash[id]; {
		case claimed:
			outcomes[i] = compareHashes(first, e.PayloadHash)
		case inserted[id]:
			outcomes[i] = Inserted
			firstHash[id] = e.PayloadHash
		default:
			earlier = append(earlier, i)
		}
	}
	if len(earlier) > 0 {
		stored, err := s.storedHashes(ctx, events, earlier)
		if err != nil {
			return BatchResult{}, err
		}
		for _, i := range earlier {
			hash, ok := stored[identityOf(events[i])]
			if !ok {
				return BatchResult{}, fmt.Errorf("event %s/%d neither inserted nor already journaled",
					events[i].DeviceID, events[i].Seq)
			}
			outcomes[i] = compareHashes(hash, events[i].PayloadHash)
		}
	}
	return BatchResult{Outcomes: outcomes, NewAlarms: newAlarms}, nil
}

// identity is what makes two events the same event.
type identity struct {
	deviceID string
	seq      int64
	tsMicros int64
}

func identityOf(e Event) identity { return identity{e.DeviceID, e.Seq, e.TS.UnixMicro()} }

func compareHashes(stored, sent []byte) Outcome {
	if bytes.Equal(stored, sent) {
		return Duplicate
	}
	return Conflict
}

// storedHashes returns the journaled payload hash for the events at the given
// indexes.
func (s *Store) storedHashes(ctx context.Context, events []Event, indexes []int) (map[identity][]byte, error) {
	deviceIDs := make([]string, len(indexes))
	seqs := make([]int64, len(indexes))
	tss := make([]time.Time, len(indexes))
	for k, i := range indexes {
		deviceIDs[k], seqs[k], tss[k] = events[i].DeviceID, events[i].Seq, events[i].TS
	}
	rows, err := s.pool.Query(ctx, `
		SELECT e.device_id, e.seq, e.ts, e.payload_hash
		FROM unnest($1::text[], $2::bigint[], $3::timestamptz[]) AS k(device_id, seq, ts)
		JOIN event_log e ON e.device_id = k.device_id AND e.seq = k.seq AND e.ts = k.ts`,
		deviceIDs, seqs, tss)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	stored := make(map[identity][]byte, len(indexes))
	for rows.Next() {
		var id identity
		var ts time.Time
		var hash []byte
		if err := rows.Scan(&id.deviceID, &id.seq, &ts, &hash); err != nil {
			return nil, err
		}
		id.tsMicros = ts.UnixMicro()
		stored[id] = hash
	}
	return stored, rows.Err()
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

// DeviceHealth describes a device's heartbeats as of one server instant.
type DeviceHealth struct {
	LastHeartbeat *time.Time    // latest heartbeat by device time, nil if never seen
	Slots         int           // distinct one-second slots with a heartbeat in the window
	ClockOffset   time.Duration // estimated device clock minus server clock
}

// DeviceHealth counts heartbeats over the device's own last five minutes.
//
// Device clocks drift, so "now" on the server is not "now" on the device. The
// offset is estimated from the device's latest heartbeat as its timestamp
// minus the time the server received it. The window is then
// (deviceNow - 300s, deviceNow] in device time, and Slots is the number of
// distinct one-second slots in it that hold a heartbeat, counted back from
// deviceNow so there are exactly 300. A device that stopped sending keeps its
// last known offset, so its window keeps moving and empties as time passes.
func (s *Store) DeviceHealth(ctx context.Context, deviceID string, at time.Time) (DeviceHealth, error) {
	var h DeviceHealth
	var last, received time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT ts, received_at FROM event_log
		WHERE type = 'heartbeat' AND device_id = $1
		ORDER BY ts DESC
		LIMIT 1`, deviceID).Scan(&last, &received)
	if errors.Is(err, pgx.ErrNoRows) {
		return h, nil
	}
	if err != nil {
		return h, err
	}
	h.LastHeartbeat = &last
	h.ClockOffset = last.Sub(received)
	deviceNow := at.Add(h.ClockOffset)
	err = s.pool.QueryRow(ctx, `
		SELECT count(DISTINCT floor(extract(epoch FROM ($2::timestamptz - ts))))
		FROM event_log
		WHERE type = 'heartbeat' AND device_id = $1
		  AND ts > $2::timestamptz - make_interval(secs => $3) AND ts <= $2`,
		deviceID, deviceNow, HeartbeatWindow.Seconds()).Scan(&h.Slots)
	return h, err
}

// PresenceTimeline returns the room's last presence transition at or before
// start (nil if none) and every transition after start, ordered by event time,
// then device_id, then seq. Transitions dated ahead of the server clock are
// included: the latest presence event decides the room's current state.
func (s *Store) PresenceTimeline(ctx context.Context, roomID string, start time.Time) (*aggregation.Transition, []aggregation.Transition, error) {
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
		WHERE type = 'presence' AND room_id = $1 AND ts > $2
		ORDER BY ts, device_id, seq`, roomID, start)
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

// AlarmsSince returns every alarm the service received at or after since,
// ordered by event time. Filtering on receipt, not event time, means a poller
// that passes its last poll time still gets a fall that arrived late.
func (s *Store) AlarmsSince(ctx context.Context, since time.Time) ([]Alarm, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+alarmColumns+` FROM alarms WHERE received_at >= $1 ORDER BY ts, id`, since)
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
