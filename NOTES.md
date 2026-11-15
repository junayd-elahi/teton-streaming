# Implementation notes

Upstream commit: `ade2e2cba2d6157f9d1bec7e4821ea54026473d3`

## Phase 1: first durable path (done 15 Nov 2026)

- `POST /events` validates the envelope, journals to `event_log`, and returns 200 only after commit.
- Exact retry of `(device_id, seq)` with the same canonical payload: 200, `"duplicate": true`, no new row.
- Same identity with different content: 409, logged.
- `ts` more than 1 hour ahead of receipt: 400. Exactly 1 hour ahead: accepted.
- Canonical payload = the JSON object re-encoded with sorted keys, numbers kept as sent. Hash is sha256 of that.

Verified:

- Integration tests pass against PostgreSQL 16.
- Real binary: post, `kill -9`, restart, row still present, replay adds nothing, changed payload gives 409.

Docker: `docker compose up --build` verified on Windows with Docker Desktop (15 Nov 2026): event accepted, server restarted, replay reported as duplicate.

## Phase 2: journal and validation (done 15 Nov 2026)

- Each event type's own field is validated (boolean, 0 to 1 range, enum, number). Bad input is a 400.
- Requests go into a bounded queue. One writer takes whatever is waiting, up to `INGEST_BATCH_SIZE`, and commits it as one transaction. Batches grow by themselves under load; there is no flush timer.
- One writer means journal id order equals commit order.
- If the queue stays full for `INGEST_ENQUEUE_TIMEOUT_MS`, the request gets 503 with `Retry-After`. Storage failure is also 503. Both are safe to retry.
- Inside a batch, the first event for an identity wins; later copies are duplicates, later different content is a conflict.

Verified:

- `go test -race -count=5 ./...` passes (10 tests).
- 50 simultaneous copies of one event: one row, exactly one reported as the original.
- 20 simultaneous events with one identity and different content: one accepted, 19 conflicts.
- Reply dropped after commit, then retry: 200 duplicate, one row.
- Upstream generator, adversarial mode, 200 devices, 20 s: 6,273 sent, 0 failed, 6,273 distinct rows.

## Phase 3: device health and room occupancy (done 15 Nov 2026)

- Both are computed at read time straight from `event_log`, using two partial indexes. There are no derived tables for them, so a read always reflects every committed event and a late event corrects history without extra work.
- All calculations use the device's event time (`ts`). Events dated ahead of the server clock are stored and only start to count once the clock reaches them.
- Health: `availability_5m` is the number of distinct one-second slots holding a heartbeat in `(now - 300s, now]`, divided by 300. Slots are counted back from now, so there are exactly 300. Several heartbeats in one slot count once.
- Occupancy: occupied time is integrated from presence transitions, clipped to the window. The state at the window start comes from the last transition at or before it.
- Ordering inside a room is `ts`, then `device_id`, then `seq`. This makes ties reproducible; it does not claim to know which device was really first.
- No earlier history means unknown, not absent. Unknown time is reported in `unknown_seconds` and never counted as occupied, so `occupied_pct` is a lower bound when it is above zero.
- A device or room never seen returns zeros, not 404, so new devices work without registration.

Verified:

- Occupancy fixture: absent at 12:00:00, present at :10, absent at :40 gives 30/60 at 12:01:00. Adding a late present at :50 gives 40/60.
- Same events in order, reversed and interleaved give identical results.
- Two devices in one room, including an equal-timestamp tie, follow the documented rule.
- Health edges: a heartbeat exactly 300 s old is excluded, one at the current instant is included, two in one second count once, 300 consecutive seconds give 1.0.
- Teton's smoke evaluator, 50 devices: 1,464 sent, 0 failed, both read endpoints answer. Alarms are still missing.
- Both read queries use index-only scans.

## Run

```bash
docker compose up -d db
cp .env.example .env && set -a && source .env && set +a
go run ./cmd/server
go test -race -count=1 -p 1 ./...
```

## Deferred

Alarm deduplication and feed, readiness endpoint, metrics, load measurement.
