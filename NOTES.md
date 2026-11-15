# Implementation notes

Upstream commit: `ade2e2cba2d6157f9d1bec7e4821ea54026473d3`

## Phase 1: first durable path (done 15 Nov 2026)

- `POST /events` validates the envelope, journals to `event_log`, and returns 200 only after commit.
- Exact retry of `(device_id, seq)` with the same canonical payload: 200, `"duplicate": true`, no new row.
- Same identity with different content: 409, logged.
- `ts` more than 1 hour ahead of receipt: 400. Exactly 1 hour ahead: accepted.
- Canonical payload = the JSON object re-encoded with sorted keys, numbers kept as sent. Hash is sha256 of that.

Verified:

- `go test -race ./...` passes against PostgreSQL 16 (4 integration tests).
- Real binary: post, `kill -9`, restart, row still present, replay adds nothing, changed payload gives 409.

Not yet verified: `compose.yaml` and `Dockerfile` (written, never run).

## Run

```bash
docker compose up -d db
export DATABASE_URL=postgres://teton:teton@localhost:5432/teton LISTEN_ADDR=:8080
go run ./cmd/server
go test -race -count=1 ./...
```

## Deferred to phase 2

Per-type field validation, bounded queue and batching writer, concurrent-retry and failure-after-commit tests.
