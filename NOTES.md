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

Not yet verified: `compose.yaml` and `Dockerfile` (written, never run).

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

## Run

```bash
docker compose up -d db
cp .env.example .env && set -a && source .env && set +a
go run ./cmd/server
go test -race -count=1 -p 1 ./...
```

## Deferred

Projections (health, occupancy, alarms), read endpoints, readiness endpoint, metrics, load measurement.
