# Teton streaming backend

A streaming backend for room-sensor events: it ingests device messages, journals them durably in PostgreSQL, and serves device health, room occupancy and deduplicated fall alarms from them.

This is my solution to [Teton's real-time streaming backend challenge](https://github.com/Teton-ai/careers-challenge-streaming-backend). The problem statement, event schema, event generator and evaluator all live in their repository and are not copied here.

## Status

Work in progress.

| Area | State |
|---|---|
| Durable ingestion (`POST /events`) | Done |
| Idempotent retries and conflict detection | Done |
| Validation of all six event types | Done |
| Bounded queue with one batching writer | Done |
| Device health over 5 minutes (`GET /devices/{id}/health`) | Done |
| Room occupancy over 1m, 5m, 1h (`GET /rooms/{id}/occupancy`) | Done |
| Deduplicated fall alarms (`GET /alarms?since=`) | Done |
| Resumable long-poll alarm feed (`GET /alarms?after_id=&wait=`) | Done |
| Crash, overload and load measurements | Not started |

## How ingestion works

1. `POST /events` validates the envelope, the type-specific field and the timestamp. An event dated more than one hour ahead is rejected.
2. Valid events go into a bounded queue. One writer takes whatever is waiting, up to a batch limit, and commits it as one transaction.
3. The request is answered only after its batch commits, so a `200` means the event is on disk.
4. `(device_id, seq)` identifies an event. Sending the same event again returns `200` with `"duplicate": true` and changes nothing. Reusing the identity with different content returns `409`.
5. If the queue stays full, the request gets `503` with `Retry-After`. Retrying is always safe.

## How the reads work

Health and occupancy are computed at read time from the journal, by the device's own event time, so a late or replayed event corrects the past without any extra step.

- **Health:** the share of the last 300 one-second slots that contain a heartbeat.
- **Occupancy:** occupied time integrated from presence transitions and clipped to the window. Time before a room's first known state is reported as `unknown_seconds`, not counted as empty.

```bash
curl localhost:8080/devices/dev_0001/health
curl "localhost:8080/rooms/room_001/occupancy?window=5m"
```

## How alarms work

A sensor can report one fall several times. Reports with the same device, room, timestamp and confidence become one alarm, created in the same transaction as the event, so an acknowledged fall always has its alarm.

- `GET /alarms?since=0` returns all alarms ordered by event time.
- `GET /alarms?after_id=<cursor>&wait=10` is the live feed. It answers as soon as a new alarm commits, or with an empty list after the wait. Pass the returned `next_cursor` as `after_id` next time. The cursor survives disconnects and restarts, and a fall that arrives late is still delivered.

## Stack

Go (standard `net/http`, `pgx`) and PostgreSQL 16. One service, one database.

## Run it

Needs Go 1.24+ and Docker.

```bash
docker compose up -d db
cp .env.example .env && set -a && source .env && set +a
go run ./cmd/server
```

Send an event:

```bash
curl -d '{"device_id":"dev_0001","room_id":"room_001","type":"heartbeat","ts":"'"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"'","seq":1}' localhost:8080/events
```

## Test it

The integration tests need the database running:

```bash
go test -race -count=1 -p 1 ./...
```

To drive it with Teton's generator, clone their repository next to this one and run:

```bash
python3 event_generator/generate.py --mode adversarial --target http://localhost:8080 --devices 200 --duration 20
```

## Layout

| Path | Content |
|---|---|
| `cmd/server/` | Process entry point and configuration |
| `internal/api/` | HTTP handlers and validation |
| `internal/ingest/` | Bounded queue and batching writer |
| `internal/aggregation/` | Pure window calculations |
| `internal/store/` | PostgreSQL journal and migrations runner |
| `migrations/` | Versioned schema |
| `tests/integration/` | Restart, replay, concurrency, conflict, fixed-clock read and alarm feed tests |
| `NOTES.md` | Design decisions and what has been verified |
