# Teton streaming backend

A Go HTTP service for room-sensor events. It journals events and fall alarms in PostgreSQL 16, serves device health and room occupancy, and exposes a resumable alarm feed.

This is my solution to [Teton's real-time streaming backend challenge](https://github.com/Teton-ai/careers-challenge-streaming-backend). The challenge repository contains the problem statement, event schema, generator, and evaluator.

This project was co-created with AI coding assistants: Claude wrote most of the code under my direction, and Codex was used for independent review and hardening. I set the direction, made the design decisions put to me, and ran the tests and load measurements on my own machine.

## Design

`POST /events` validates input and writes to bounded ingestion lanes. The service responds successfully only after commit. Exact retries are acknowledged as duplicates; reuse of an identity with different content returns `409`. If a queue stays full, the service returns `503` with `Retry-After`. Fall warnings have a dedicated lane, though database contention can still delay them.

Health and occupancy are calculated from the journal at read time using event timestamps, so late and replayed events update historical results. Alarm snapshots are timestamp ordered; the live feed follows durable commit order and resumes with a persistent cursor. See [NOTES.md](NOTES.md) for verified behavior, measurements, and limits, and [docs/retention-design.md](docs/retention-design.md) for the retention proposal.

## Run

Requirements: Go 1.26.11+ and Docker. Start PostgreSQL, create a local `.env`, then run the server:

```bash
docker compose -p teton-streaming up -d db
cp .env.example .env
set -a && source .env && set +a
go run ./cmd/server
```

The shell environment setup exports `DATABASE_URL` for the host process. Compose does not export container environment variables into your host shell. Keep the `.env` file local.

The Compose server is exposed on localhost for trusted local evaluation. External deployment needs an authenticated, encrypted access boundary and a deployment-specific access policy.

## Tests

Integration tests require PostgreSQL. They create and remove a random schema, reusing it across restarts within each test; the database account needs schema-creation permission. Other schemas are preserved. Use a dedicated test database/schema.

In a second Bash shell, export the local settings before running tests:

```bash
set -a && source .env && set +a
go test -race -count=1 -p 1 ./...
```

The optional [independent correctness verifier](tools/verification/README.md) checks numerical health and occupancy results against source-event truth, then verifies recovery after a hard kill. It supplements the public evaluator.

## Run the official generator

With the service running, clone the official harness alongside this checkout and run its smoke evaluator:

```bash
git clone https://github.com/Teton-ai/careers-challenge-streaming-backend.git ../teton-challenge
git -C ../teton-challenge checkout ade2e2cba2d6157f9d1bec7e4821ea54026473d3
python3 ../teton-challenge/eval/check.py --target http://localhost:8080 --devices 50 --duration 30 smoke
```

The evaluator also accepts `baseline`, `burst`, `offline`, and `adversarial`. Use at least 100 seconds for offline replay and 125 seconds to include both complete burst windows. Run each mode against a fresh dedicated test database: `since=0` includes all alarms, and reusing history contaminates the evaluator's distinct-fall count. The public evaluator checks a narrower set of properties than the private grader.

## Load measurements

A diagnostic run with a 5,000/s baseline and a 30-second 50,000/s burst:

```bash
go run ./tools/load -senders 1000 -duration 60s -rate 5000 -burst-rate 50000 -burst-start 15s -burst-duration 30s
```

On Docker Desktop for Windows, running the load process inside the Compose network avoids the Windows port proxy:

```bash
docker run --rm --network teton-streaming_default -v "${PWD}:/src" -w /src golang:1.26.11 go run ./tools/load -target http://server:8080 -senders 2000 -duration 60s -rate 5000 -burst-rate 50000 -burst-start 15s -burst-duration 30s -offer-buffer 16384
```

Results are environment-specific: the 9-reported-CPU Linux closed-loop run reached 58,477 events/s, and a full scenario accepted two complete 50,000/s cohorts. The owner-reported run on my 4-CPU Docker VM reached about 13,200/s in its burst ([record](docs/benchmarks/windows-docker-reported.json)). See [NOTES.md](NOTES.md) and [docs/benchmarks](docs/benchmarks) for conditions and accounting. These are local measurements, not a grader score or certification.

For the five-minute schedule, `tools/load` generates traffic but does not restart the service or independently inspect the database. Perform the hard kill/restart through the process manager and reconcile the journal separately. Do not use `docker compose down -v` against a database containing data you need.

## Endpoints

- `GET /healthz`: process is up.
- `GET /readyz`: database responds.
- `GET /metrics`: Prometheus text metrics.
- `GET /devices/{id}/health`
- `GET /rooms/{id}/occupancy?window=5m`
- `GET /alarms?since=0` or `GET /alarms?after_id=<cursor>&wait=10`

Run one service instance. Mixed-version rolling deployment is unsupported; stop the old instance before starting an upgraded build and migrations.
