# Teton streaming backend

A Go service that ingests room-sensor events, stores them durably in PostgreSQL 16, serves per-device health and per-room occupancy, and delivers fall alarms through a resumable feed.

It is my solution to [Teton's real-time streaming backend challenge](https://github.com/Teton-ai/careers-challenge-streaming-backend), which contains the problem statement, event schema, generator and evaluator. The write-up the challenge asks for is in [SUBMISSION.md](SUBMISSION.md).

This project was co-created with AI coding assistants: Claude wrote most of the code under my direction, and Codex was used for independent review and hardening. I set the direction, made the design decisions put to me, and ran the tests and load measurements on my own machine.

## Design

**Ingestion and backpressure.** `POST /events` validates each event and places it in a bounded queue. Four writer lanes batch ordinary events, up to 500 per transaction, and a device always maps to the same lane. Fall warnings have their own lane, so they never queue behind ordinary traffic. A lane waits up to 5 ms to fill a batch only when requests are already queued, so a client that sends one request at a time is answered without delay. The service replies successfully only after the transaction commits. If a queue stays full for 4 seconds the request receives `503` with `Retry-After`, and retrying is safe.

**Identity, retries and deduplication.** An event is identified by `(device_id, seq, ts)`. An identical retry is acknowledged as a duplicate; the same identity with different content returns `409`. A fall warning creates its alarm in the same transaction as its journal row. Falls are deduplicated by device, room, timestamp and confidence, which is how the challenge generator repeats a fall to simulate sensor jitter.

**Health and occupancy.** Both are computed at read time from the journal, by event time, so late and replayed events correct historical windows automatically. Device health counts distinct one-second heartbeat slots in the device's own last five minutes, with its clock offset estimated from the newest heartbeat received within 60 seconds of its timestamp. Occupancy intersects presence intervals with the 1m, 5m or 1h window; time before a room's first known state is reported as unknown, not empty. New devices need no registration.

**Alarms.** `GET /alarms?since=<ts>` returns a snapshot ordered by event time. `GET /alarms?after_id=<cursor>&wait=<s>` is a long-poll feed in commit order: a consumer stores `next_cursor` and resumes from it after a disconnect or a service restart. A database-level lock stops an interrupted fall commit from appearing behind a newer cursor. The live feed uses commit order because a fall replayed late by an offline device must still be delivered at once; strict event-time order would mean holding alarms back.

**Restart.** All state lives in PostgreSQL. Acknowledged events, alarms and feed cursors survive a hard kill, and Compose restarts both containers automatically.

**Observability.** `GET /metrics` exposes Prometheus metrics for events by outcome, queue depth, queue wait, batch size, commit time, request time, alarms created and alarm visibility latency. `GET /readyz` reports whether the database answers.

### Layout

| Path | Contents |
|---|---|
| `cmd/server` | Entry point and configuration |
| `internal/api` | HTTP handlers and event validation |
| `internal/ingest` | Bounded queues and batching writers |
| `internal/store` | PostgreSQL access, migrations runner and read queries |
| `internal/aggregation` | Occupancy interval calculation |
| `internal/metrics` | Dependency-free Prometheus registry |
| `migrations` | SQL schema |
| `tests/integration` | End-to-end tests against PostgreSQL |
| `tools/load` | Load generator with burst, offline and alarm-latency measurement |

## Run

Requirements: Docker, plus Go 1.26.11 or later to run the service outside a container.

Whole stack, with the service on `localhost:8080`:

```bash
docker compose -p teton-streaming up --build -d
curl http://localhost:8080/readyz
```

Service on the host, database in Docker:

```bash
docker compose -p teton-streaming up -d db
cp .env.example .env
set -a && source .env && set +a
go run ./cmd/server
```

Settings are environment variables; `.env.example` lists them. `GOGC=400` gives about 15% more throughput under concurrent load for about 155 MB of memory instead of 75 MB. Run one instance at a time and stop it before upgrading, because migrations run at start-up. Both ports are bound to localhost; there is no authentication or TLS.

## Test

```bash
set -a && source .env && set +a
go test -race -count=1 -p 1 ./...
```

Integration tests need PostgreSQL; each test creates and drops its own schema. The suite covers validation, retries, identity conflicts, concurrent submissions, offline replay, health and occupancy calculations, alarm deduplication, feed resume across a restart, interrupted commits and a 5,001st device.

Teton's evaluator, against a running service:

```bash
git clone https://github.com/Teton-ai/careers-challenge-streaming-backend.git ../teton-challenge
python3 ../teton-challenge/eval/check.py smoke --target http://localhost:8080 --devices 50
```

Other modes are `baseline`, `burst`, `offline` and `adversarial`. Use a fresh database for each run: `/alarms?since=0` returns every stored alarm, so earlier runs inflate the count. On Docker Desktop for Windows, run the evaluator on the Compose network instead:

```bash
docker run --rm --network teton-streaming_default -v <path-to-teton-challenge>:/c -w /c python:3.12 python eval/check.py smoke --target http://server:8080 --devices 50
```

The grading scenario (5,000/s baseline, two 30-second bursts at 50,000/s, 20% of devices offline for 60 seconds), run with the load tool:

```bash
go run ./tools/load -target http://localhost:8080 -senders 2000 -offer-buffer 16384 -duration 300s \
  -rate 5000 -burst-rate 50000 -burst-start 30s -burst-duration 30s -second-burst-start 210s \
  -offline-start 90s -offline-duration 60s -offline-percent 20 -retries 10
```

The load tool offers events on a fixed schedule. An event no sender is free to take is reported as not sent, so a slow service cannot quietly lower the offered rate. It also follows the alarm feed and reports how long each fall took to appear.

## Results

These are my own measurements, not Teton's private score. Rows marked "earlier build" were measured before the final batching and heartbeat changes and were not repeated.

| Check | Environment | Result |
|---|---|---|
| Full race test suite | Go 1.26.11, PostgreSQL 16 | 108 tests and 56 subtests pass |
| Teton evaluator, all five modes, 50 devices | Linux | No HTTP failures; alarms matched distinct falls in every mode |
| Teton evaluator, smoke | Docker Desktop on Windows | 1,479 of 1,479 events accepted; 23 alarms for 23 falls |
| Teton evaluator, adversarial, 5,000 devices, 240 s | 2-CPU Linux | 217,800 of 217,800 events accepted; 2,780 alarms for 2,780 falls |
| Same, service hard-killed at 100 s and restarted after 1 s | 2-CPU Linux | Only the 87 events sent during the outage failed (the generator does not retry); 2,855 alarms for 2,855 stored falls |
| Teton's generator, one request at a time, 5,000 devices | 2-CPU Linux | 886 events/s; each request waits for its own commit |
| Closed-loop capacity, 30 s | 9-CPU Linux, earlier build | 58,477 durable events/s; fall POST-to-feed p50 9.6 ms, p95 28.7 ms |
| Five-minute grading scenario with a hard kill at 3 minutes | 9-CPU Linux, earlier build | First run: all 4,200,000 events accepted. Two repeats sent 90% and 96% of the schedule; nothing refused or lost, all 60,000 offline events replayed, fall p95 under 30 ms |
| Same burst shape | 4-CPU Docker VM on Windows, earlier build | About 13,200 events/s during the burst; all 7,165 falls delivered, p95 67 ms |

In the run with the hard kill, which also had device clocks skewed by up to 30 seconds, 64 of 2,855 alarms (2.2%) reached the live feed after a later-timestamped alarm from the same room. The snapshot returned all of them in event-time order.

## How the work developed

1. **Core service.** Journal schema and migrations, `POST /events` with durable acknowledgement, a bounded queue with a batching writer, health and occupancy reads, alarms created inside the journal transaction, and the snapshot and long-poll feed.
2. **Independent review and hardening.** HTTP timeouts, a localhost-only database and a non-root container. Rejection of NaN, infinite and out-of-range values. One unstorable event no longer fails the rest of its batch. Alarms gained `event_id` to match the evaluator's shape. Event identity now includes the timestamp, so a device that restarts its sequence numbers is not mistaken for a retry.
3. **Throughput.** Each batch became a single SQL statement (about 9,800 to 14,900 events/s on a 2-CPU machine). Parallel writer lanes and a dedicated fall lane followed, then a short batch wait. Measuring on Windows exposed a load-tool fault: its alarm listener shared connections with the senders and reported its own queueing as alarm delay. It now has its own connection.
4. **Correctness.** Concurrent commits could let a feed consumer skip an interrupted fall commit; a database lock fixed this. A black-box check that computed expected results from the source events found that a delayed heartbeat could replace a newer clock estimate and lower availability. That is fixed and covered by a regression test.
5. **Sequential clients.** The batch wait made every lone request wait 5 ms. Waiting only when requests are already queued raised throughput under Teton's generator from 253 to 886 events/s.
6. **Final tuning.** Overloaded requests are held for up to 4 seconds before a `503`, inside the generator's 5-second timeout. A CPU profile showed about three quarters of service time in handling individual HTTP requests, and raising `GOGC` to 400 gave about 15% more throughput.

## Limits and next work

- **50,000 events/s bursts.** Reached in one full scenario on 9 CPUs but not in two repeats. Those runs fell short because replies slowed and the load tool ran out of free senders; no event was refused or lost. Most CPU goes on handling one HTTP request per event, which is how Teton's generator sends, so the remaining headroom depends on hardware.
- **Retention.** The journal grows without bound. Deleting old rows naively is unsafe: occupancy needs each room's last presence state before the window, health needs each device's latest heartbeat and clock sample, retries need stored identities and hashes, and consumers need resumable cursors. The plan is to checkpoint that state, then remove old history by time partitions or bounded deletion, verified by comparing reads before and after.
- **Alarm order.** The live feed follows commit order and the snapshot follows event time. Strict event-time order on the live feed needs an agreed bound on lateness.
- **Fall identity.** A repeat with a shifted timestamp becomes a separate alarm; merging those needs a fall identifier from the device.
- **Deployment.** Single instance only. Database failure and backup restore are untested.

## Endpoints

- `POST /events`
- `GET /devices/{device_id}/health`
- `GET /rooms/{room_id}/occupancy?window=1m|5m|1h`
- `GET /alarms?since=<ts>` (snapshot) or `GET /alarms?after_id=<cursor>&wait=<seconds>` (feed)
- `GET /healthz`, `GET /readyz`, `GET /metrics`
