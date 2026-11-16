# Design and verification notes

This file records the current design and the evidence behind the reported behavior. The challenge specification and generator are in [Teton's official repository](https://github.com/Teton-ai/careers-challenge-streaming-backend).

## Current design

- **Durable ingestion:** `POST /events` validates the envelope and type-specific values before enqueueing. Bounded ordinary lanes batch journal writes. A lane waits up to 5 ms to fill a batch only when requests are already queued, so a client sending one request at a time is answered without that wait. A dedicated fall lane protects warnings from ordinary queue saturation and never waits. The service replies successfully only after commit. Queue saturation returns `503` with `Retry-After`; storage failures return retryable `503`.
- **Identity and retries:** `(device_id, seq, ts)` identifies an event. An identical retry is a duplicate; different content under the same identity is a conflict. Alarm creation is transactional with the event. An advisory transaction fence prevents an interrupted alarm commit from being overtaken by a later cursor.
- **Reads:** Health and occupancy are computed from journal rows at read time and use event time. Late and replayed events therefore correct historical windows. Occupancy before the first known presence state is unknown, not empty.
- **Alarms:** Timestamp-ordered snapshots support history; the live feed follows commit order and resumes with a durable cursor. Strict event-time ordering for arbitrarily late events is not compatible with prompt live delivery.
- **Deployment:** The supported model is a single service instance with coordinated upgrades. The journal currently grows without bound; retention is design-only.

## Verification

The final source state has passed `go test -race -count=1 -p 1 ./...`, vet, and a pooled-DSN preservation check. The full test suite includes integration coverage for retries, identity conflicts, concurrent submissions, replay, read calculations, alarm feed resume, and interrupted commits. The generator and load runs below are separate from the test suite.

The public evaluator checks distinct-fall counts and samples read endpoints. Its exit status alone is not a correctness verdict; compare the printed ground-truth count with the returned alarms. It is narrower than the private grader.

### Public evaluator and failure probes

All five public evaluator modes passed on the final code using 50 devices and a fresh database per mode:

| Mode | Duration | HTTP accepted | Distinct falls / alarms |
|---|---:|---:|---:|
| smoke | 30 s | 1,460 | 15 / 15 |
| baseline | 60 s | 2,986 | 38 / 38 |
| burst | 180 s | 15,895 | 202 / 202 |
| offline | 120 s | 5,176 | 69 / 69 |
| adversarial | 240 s | 18,285 | 242 / 242 |

There were no HTTP failures. A separate 30-second API-kill/restart probe acknowledged all 5,400 scheduled events, replayed all 120 offline-buffered events, and delivered all 67 acknowledged falls. An intentionally throttled overload probe produced 533 retryable `503` attempts with `Retry-After`; all 32 events eventually committed exactly once, and a post-acknowledgement retry was recognised as a duplicate.

These are functional checks, not capacity proof. [The verification record](docs/verification/public-evaluator.json) includes source hashes, the official harness revision, counts, isolation and scope.

### Official generator at 5,000 devices

Teton's generator sends one request at a time on a new connection and waits for each reply, so against it the measure that matters is the time to answer a single request, not concurrent throughput. Baseline mode, 5,000 devices, 40 seconds, on a 2-CPU Linux machine shared by the generator, the service and PostgreSQL:

| Target | HTTP accepted | Events/s | Failed |
|---|---:|---:|---:|
| Teton's example stub (in memory, no durability) | 55,708 | 1,393 | 0 |
| This service, batches always waiting 5 ms to fill | 10,119 and 10,151 | 253 | 0 |
| This service, waiting only when requests are queued | 35,402 and 35,488 | 886 | 0 |

The unconditional wait was added while tuning for many concurrent senders and made every lone request wait for company that could not arrive. The conditional wait keeps full batches under concurrent load (about 54 events per batch at 1,000 senders, with throughput unchanged at about 14,500 events/s on the same machine) and removes the delay for sequential clients. With four connections, throughput rose from 577 to 1,745 events/s.

The remaining gap to the stub is the durable commit: each lone request waits for its own disk flush. That is the cost of acknowledging only after commit, and it is kept deliberately.

A 130-second adversarial run at 300 devices on the conditional-wait build accepted 60,537 events with 60,537 journal rows, and 805 distinct falls matched 805 alarms and 805 alarms returned by `GET /alarms?since=0`. A kill and restart under 3,000 events/s with retries acknowledged all 75,000 offered events, and the journal held exactly 75,000 rows.

The 50-device evaluator table above was recorded before this change; its counts are functional results and do not depend on it.

### Linux capacity and scenario runs

The server, PostgreSQL 16.2, and load process shared a Linux environment where Go reported 9 CPUs. Durability settings stayed enabled. This is not directly comparable with my 4-CPU Docker VM.

| Run | Verified result |
|---|---|
| 30-second closed-loop capacity | 1,755,375 accepted; 58,477/s; 23,162/23,162 acknowledged falls delivered; POST-to-feed p95 28.714 ms |
| Five-minute scenario, first 50K/s burst | 1,500,000 scheduled, sent, and accepted; all 19,826 acknowledged falls delivered |
| Five-minute scenario, second 50K/s burst | 1,500,000 scheduled, sent, and accepted; all 19,746 acknowledged falls delivered |
| Complete five-minute scenario | 4,200,000 events scheduled and sent; 4,199,967 fresh acceptances plus 33 duplicate acknowledgements; journal count 4,200,000; all 60,000 events buffered during offline periods replayed |

The five-minute run hard-killed the API at 180.442 seconds; readiness returned at 180.772 seconds. There were no final HTTP or transport errors, unsent events, or unflushed offline events. All 61 live read checks passed shape/range checks, not comparison against an independent aggregation oracle. Counts persisted after the API stopped. The database was not killed, so this does not test database failure or backup restoration.

The load schedule used an explicit 16,384-slot offer buffer (327.68 ms at 50,000/s) because the default bounded scheduler dropped work during pauses even against an immediate-response endpoint. Earlier paced diagnostics did not send their full schedules and were not target passes. The final full scenario did send and acknowledge its entire schedule. Do not treat unsent scheduled work as service refusals.

Raw summaries: [closed-loop capacity](docs/benchmarks/closed-loop-30s.json) and [full scenario](docs/benchmarks/full-scenario.json). They include phase accounting and latency measurements.

### Four-CPU Docker VM result

The corrected-listener 60-second run on my 4-CPU Docker VM scheduled 1,650,000 events ([owner-reported record](docs/benchmarks/windows-docker-reported.json)). It accepted 547,327 (9,121/s over the whole run, about 13,200/s during the burst); 1,102,673 scheduled events were never sent because no load sender was available. There were no HTTP or transport errors, and all 7,165 acknowledged falls reached the feed. Request p95 was 126 ms; first-send-to-feed p95 was 67 ms. This run did not reach the 50,000/s burst target.

Older Windows port-proxy and shared-listener latency results are superseded by the corrected-listener run above. The two environments use different hardware, runtime topology, and workloads; compare them only with those conditions attached.

## Limits and next work

Retention should use time-based partitions and a cleanup policy that preserves sufficient history for window calculations. Define and test the retained-state boundary before deletion. Other explicit limits are the single-instance cursor/wakeup model, clock-skew estimates based on recent received heartbeats, and event-time versus live commit-order alarm semantics. No private evaluator score or certification is reported.

## Local commands

The `.env` setup exports `DATABASE_URL` for the host process; Compose does not export it to the host shell. Integration tests need a dedicated database/schema, and leave other schemas intact.

```bash
docker compose -p teton-streaming up -d db
cp .env.example .env
set -a && source .env && set +a
go run ./cmd/server
```

Run tests from a second shell with the same `.env` exports:

```bash
set -a && source .env && set +a
go test -race -count=1 -p 1 ./...
```
