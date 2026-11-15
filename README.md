# Teton streaming backend

A streaming backend for room-sensor events: it ingests device messages, journals them durably in PostgreSQL, and serves device health, room occupancy and deduplicated fall alarms from them.

This is my solution to [Teton's real-time streaming backend challenge](https://github.com/Teton-ai/careers-challenge-streaming-backend). The problem statement, event schema, event generator and evaluator all live in their repository and are not copied here.

## Status

Work in progress.

| Area | State |
|---|---|
| Durable ingestion (`POST /events`) | Done |
| Idempotent retries and conflict detection | Done |
| Validation of all six event types, hostile input rejected | Done |
| Bounded queue with one batching writer | Done |
| Device health over 5 minutes (`GET /devices/{id}/health`) | Done |
| Room occupancy over 1m, 5m, 1h (`GET /rooms/{id}/occupancy`) | Done |
| Deduplicated fall alarms (`GET /alarms?since=`) | Done |
| Resumable long-poll alarm feed (`GET /alarms?after_id=&wait=`) | Done |
| Fall warnings prioritised under load | Done |
| Clock-skew correction for device health | Done |
| Metrics (`GET /metrics`) and readiness (`GET /readyz`) | Done |
| Load measurements | In progress |

## How ingestion works

1. `POST /events` validates the envelope, the type-specific field and the timestamp. An event dated more than one hour ahead is rejected.
2. Valid events go into bounded queues, one per writer, chosen by device so a device's events stay in order. Each writer takes whatever is waiting, up to a batch limit, and commits it as one atomic statement.
3. The request is answered only after its batch commits, so a `200` means the event is on disk.
4. `(device_id, seq, ts)` identifies an event. Sending the same event again returns `200` with `"duplicate": true` and changes nothing. Reusing the identity with different content returns `409`.
5. If the queue stays full, the request gets `503` with `Retry-After`. Retrying is always safe.
6. Fall warnings have their own queue and their own writer, so a burst of heartbeats cannot delay or crowd out an alarm.

## How the reads work

Health and occupancy are computed at read time from the journal, by the device's own event time, so a late or replayed event corrects the past without any extra step.

- **Health:** the share of the last 300 one-second slots that contain a heartbeat, measured on the device's own clock so that clock drift does not make a healthy device look unhealthy.
- **Occupancy:** occupied time integrated from presence transitions and clipped to the window. Time before a room's first known state is reported as `unknown_seconds`, not counted as empty.

```bash
curl localhost:8080/devices/dev_0001/health
curl "localhost:8080/rooms/room_001/occupancy?window=5m"
```

## How alarms work

A sensor can report one fall several times. Reports with the same device, room, timestamp and confidence become one alarm, created in the same transaction as the event, so an acknowledged fall always has its alarm.

- `GET /alarms?since=0` returns all alarms ordered by event time. `since` filters by when the fall was received, so late falls are not missed.
- `GET /alarms?after_id=<cursor>&wait=10` is the live feed. It answers as soon as a new alarm commits, or with an empty list after the wait. Pass the returned `next_cursor` as `after_id` next time. The cursor survives disconnects and restarts, and a fall that arrives late is still delivered.

## Operating it

- `GET /healthz`: the process is up.
- `GET /readyz`: the database answers.
- `GET /metrics`: Prometheus text. Useful first looks are `teton_ingest_queue_depth`, `teton_events_received_total` by outcome, and `teton_alarm_visible_latency_seconds`.

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

To drive it with Teton's generator, clone their repository and run this from inside it. Use a fresh database per run, because the generator restarts its sequence numbers each time. Offline mode needs at least 95 seconds for devices to reconnect and replay.

```bash
python3 event_generator/generate.py --mode adversarial --target http://localhost:8080 --devices 200 --duration 100
```

## Measure it

`tools/load` offers events at a set rate from many connections, follows the alarm feed, and reports throughput, latency and whether any alarm was lost. This is the challenge's shape, a 5,000/s baseline with a 30 second burst at 50,000/s:

```bash
go run ./tools/load -senders 1000 -duration 60s -rate 5000 -burst-rate 50000 -burst-start 15s -burst-duration 30s
```

On Docker Desktop for Windows, run it inside Docker's network, because the Windows port proxy roughly halves throughput:

```bash
docker run --rm --network teton-streaming_default -v "${PWD}:/src" -w /src golang:1.24 go run ./tools/load -target http://server:8080 -senders 2000 -duration 60s -rate 5000 -burst-rate 50000 -burst-start 15s -burst-duration 30s
```

Leave out `-rate` to send as fast as the service answers. Results so far are in `NOTES.md`.

## Layout

| Path | Content |
|---|---|
| `cmd/server/` | Process entry point and configuration |
| `internal/api/` | HTTP handlers and validation |
| `internal/ingest/` | Bounded queue and batching writer |
| `internal/aggregation/` | Pure window calculations |
| `internal/metrics/` | Metric types and Prometheus text output |
| `internal/store/` | PostgreSQL journal and migrations runner |
| `migrations/` | Versioned schema |
| `tools/load/` | Load-measurement tool |
| `tests/integration/` | Restart, replay, concurrency, conflict, fixed-clock read and alarm feed tests |
| `NOTES.md` | Design decisions and what has been verified |
