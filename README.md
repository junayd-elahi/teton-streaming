# Teton streaming backend

A streaming backend for room-sensor events: it ingests device messages, journals them durably in PostgreSQL, and is being built up to serve device health, room occupancy and deduplicated fall alarms.

This is my solution to [Teton's real-time streaming backend challenge](https://github.com/Teton-ai/careers-challenge-streaming-backend). The problem statement, event schema, event generator and evaluator all live in their repository and are not copied here.

## Status

Work in progress.

| Area | State |
|---|---|
| Durable ingestion (`POST /events`) | Done |
| Idempotent retries and conflict detection | Done |
| Validation of all six event types | Done |
| Bounded queue with one batching writer | Done |
| Device health and room occupancy | Not started |
| Fall alarms and resumable alarm feed | Not started |
| Crash, overload and load measurements | Not started |

## How ingestion works

1. `POST /events` validates the envelope, the type-specific field and the timestamp. An event dated more than one hour ahead is rejected.
2. Valid events go into a bounded queue. One writer takes whatever is waiting, up to a batch limit, and commits it as one transaction.
3. The request is answered only after its batch commits, so a `200` means the event is on disk.
4. `(device_id, seq)` identifies an event. Sending the same event again returns `200` with `"duplicate": true` and changes nothing. Reusing the identity with different content returns `409`.
5. If the queue stays full, the request gets `503` with `Retry-After`. Retrying is always safe.

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
| `internal/store/` | PostgreSQL journal and migrations runner |
| `migrations/` | Versioned schema |
| `tests/integration/` | Restart, replay, concurrency and conflict tests |
| `NOTES.md` | Design decisions and what has been verified |
