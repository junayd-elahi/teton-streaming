package store

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/junayd-elahi/teton-streaming/migrations"
)

func connectStoreTestDatabase(ctx context.Context, raw string) (*pgx.Conn, error) {
	config, err := pgxpool.ParseConfig(raw)
	if err != nil {
		return nil, err
	}
	return pgx.ConnectConfig(ctx, config.ConnConfig)
}

func storeTestURL(raw, schema string) (string, error) {
	uri, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if uri.Scheme == "postgres" || uri.Scheme == "postgresql" {
		q := uri.Query()
		q.Set("search_path", schema)
		uri.RawQuery = q.Encode()
		return uri.String(), nil
	}
	return raw + " search_path=" + schema, nil
}

func isolatedStore(t testing.TB) *Store {
	t.Helper()
	raw := os.Getenv("DATABASE_URL")
	if raw == "" {
		t.Skip("DATABASE_URL required")
	}
	ctx := context.Background()
	conn, err := connectStoreTestDatabase(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("storage_test_%d", time.Now().UnixNano())
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		conn.Close(ctx)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := conn.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		if err != nil {
			t.Error(err)
		}
		conn.Close(ctx)
	})
	testURL, err := storeTestURL(raw, schema)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(ctx, testURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx, migrations.Files); err != nil {
		t.Fatal(err)
	}
	return s
}

func testEvent(seq int64, typ, payload string) Event {
	at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC).Add(time.Duration(seq) * time.Microsecond)
	hash := sha256.Sum256([]byte(payload))
	return Event{DeviceID: "dev_0100", Seq: seq, RoomID: "room_001", Type: typ, TS: at, Payload: []byte(payload), PayloadHash: hash[:], ReceivedAt: at}
}

func TestAppendMixedBatchClassifiesAndCreatesAlarmsAtomically(t *testing.T) {
	s := isolatedStore(t)
	ctx := context.Background()
	ordinary := testEvent(1, "heartbeat", `{"v":1}`)
	fall := testEvent(2, "fall_warn", `{"confidence":0.91}`)
	conflict := fall
	conflict.Payload = []byte(`{"confidence":0.92}`)
	hash := sha256.Sum256(conflict.Payload)
	conflict.PayloadHash = hash[:]
	batch := []Event{ordinary, fall, fall, conflict}
	for i, want := range [][]Outcome{{Inserted, Inserted, Duplicate, Conflict}, {Duplicate, Duplicate, Duplicate, Conflict}} {
		result, err := s.AppendBatch(ctx, batch)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(result.Outcomes, want) {
			t.Fatalf("pass %d outcomes %v, want %v", i, result.Outcomes, want)
		}
		if result.NewAlarms != 1-i {
			t.Fatalf("pass %d new alarms %d", i, result.NewAlarms)
		}
	}
	alarms, err := s.AlarmsAfter(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(alarms) != 1 || alarms[0].Seq != 2 {
		t.Fatalf("alarms %v", alarms)
	}
	if count, err := s.CountEvents(ctx); err != nil || count != 2 {
		t.Fatalf("events=%d error=%v", count, err)
	}
}

func TestAppendMixedBatchInvalidPayloadRollsBackAlarms(t *testing.T) {
	s := isolatedStore(t)
	ctx := context.Background()
	_, err := s.AppendBatch(ctx, []Event{testEvent(1, "fall_warn", `{"confidence":0.91}`), testEvent(2, "heartbeat", `{"v":1e999999}`)})
	if !IsUnstorable(err) {
		t.Fatalf("want unstorable error, got %v", err)
	}
	if count, err := s.CountEvents(ctx); err != nil || count != 0 {
		t.Fatalf("events=%d error=%v", count, err)
	}
	alarms, err := s.AlarmsAfter(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(alarms) != 0 {
		t.Fatalf("alarms survived failed batch: %v", alarms)
	}
}

func TestConcurrentOrdinaryAndFallIdentityKeepsSingleWinner(t *testing.T) {
	s := isolatedStore(t)
	ctx := context.Background()
	var expectedAlarms int
	for seq := int64(1); seq <= 30; seq++ {
		ordinary := testEvent(seq, "heartbeat", `{"v":1}`)
		fall := testEvent(seq, "fall_warn", `{"confidence":0.91}`)
		var results [2]BatchResult
		var errs [2]error
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i, event := range []Event{ordinary, fall} {
			wg.Go(func() { <-start; results[i], errs[i] = s.AppendBatch(ctx, []Event{event}) })
		}
		close(start)
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		got := []Outcome{results[0].Outcomes[0], results[1].Outcomes[0]}
		if !reflect.DeepEqual(got, []Outcome{Inserted, Conflict}) && !reflect.DeepEqual(got, []Outcome{Conflict, Inserted}) {
			t.Fatalf("outcomes %v", got)
		}
		if got[1] == Inserted {
			expectedAlarms++
		}
	}
	alarms, err := s.AlarmsAfter(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(alarms) != expectedAlarms {
		t.Fatalf("alarms=%d want %d", len(alarms), expectedAlarms)
	}
	if count, err := s.CountEvents(ctx); err != nil || count != 30 {
		t.Fatalf("events=%d error=%v", count, err)
	}
}

// A transaction can outlive its original service process or request. Emulate
// that orphan with a deferred trigger after A's alarm id has been allocated,
// then start B on a separate pool as a restarted writer would do.
func TestAlarmFencePreventsCursorPassingAnUncommittedAlarm(t *testing.T) {
	s := isolatedStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	keeper, err := s.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer keeper.Release()
	const gate int64 = 744011267
	if _, err := keeper.Exec(ctx, `CREATE FUNCTION storage_alarm_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF NEW.seq=1 THEN PERFORM pg_advisory_xact_lock(744011267::bigint); END IF;
 RETURN NEW; END $$;
 CREATE CONSTRAINT TRIGGER storage_alarm_gate AFTER INSERT ON alarms
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION storage_alarm_gate()`); err != nil {
		t.Fatal(err)
	}
	if _, err := keeper.Exec(ctx, "SELECT pg_advisory_lock($1::bigint)", gate); err != nil {
		t.Fatal(err)
	}
	locked := true
	defer func() {
		if locked {
			_, _ = keeper.Exec(context.Background(), "SELECT pg_advisory_unlock($1::bigint)", gate)
		}
	}()
	replacement, err := Open(ctx, s.pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	type completed struct {
		result BatchResult
		err    error
	}
	first := make(chan completed, 1)
	second := make(chan completed, 1)
	go func() {
		result, err := s.AppendBatch(ctx, []Event{testEvent(1, "fall_warn", `{"confidence":0.91}`)})
		first <- completed{result, err}
	}()
	waitForLock := func(query string, arg any) {
		t.Helper()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			var waiting bool
			if err := keeper.QueryRow(ctx, query, arg).Scan(&waiting); err != nil {
				t.Fatal(err)
			}
			if waiting {
				return
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				t.Fatal("writer did not reach expected database lock")
			}
		}
	}
	waitForLock(`SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=0 AND objid=$1 AND objsubid=1 AND NOT granted)`, gate)
	go func() {
		result, err := replacement.AppendBatch(ctx, []Event{testEvent(2, "fall_warn", `{"confidence":0.91}`)})
		second <- completed{result, err}
	}()
	var tableOID uint32
	if err := keeper.QueryRow(ctx, "SELECT 'alarms'::regclass::oid").Scan(&tableOID); err != nil {
		t.Fatal(err)
	}
	waitForLock(`SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=1413829711 AND objid=$1 AND objsubid=2 AND NOT granted)`, tableOID)
	select {
	case done := <-second:
		t.Fatalf("replacement completed before old commit: %+v", done)
	default:
	}
	var allocated int64
	if err := keeper.QueryRow(ctx, "SELECT last_value FROM alarms_id_seq").Scan(&allocated); err != nil {
		t.Fatal(err)
	}
	if allocated != 1 {
		t.Fatalf("replacement allocated alarm id before fence: sequence=%d", allocated)
	}
	before, err := replacement.AlarmsAfter(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 0 {
		t.Fatalf("cursor could pass old transaction: %v", before)
	}
	if _, err := keeper.Exec(ctx, "SELECT pg_advisory_unlock($1::bigint)", gate); err != nil {
		t.Fatal(err)
	}
	locked = false
	for _, done := range []<-chan completed{first, second} {
		select {
		case got := <-done:
			if got.err != nil {
				t.Fatal(got.err)
			}
			if !reflect.DeepEqual(got.result.Outcomes, []Outcome{Inserted}) || got.result.NewAlarms != 1 {
				t.Fatalf("result %+v", got.result)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	after, err := replacement.AlarmsAfter(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 2 || after[0].Seq != 1 || after[1].Seq != 2 || after[0].ID >= after[1].ID {
		t.Fatalf("alarm commit order %v", after)
	}
	resume, err := replacement.AlarmsAfter(ctx, after[0].ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(resume) != 1 || resume[0].Seq != 2 {
		t.Fatalf("resume %v", resume)
	}
}
