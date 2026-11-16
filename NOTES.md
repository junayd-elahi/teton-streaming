# Design and verification notes

This file records the current design and the evidence behind the reported behavior. The challenge specification and generator are in [Teton's official repository](https://github.com/Teton-ai/careers-challenge-streaming-backend).

## Current design

- **Durable ingestion:** `POST /events` validates the envelope and type-specific values before enqueueing. Bounded ordinary lanes batch journal writes. A lane waits up to 5 ms to fill a batch only when requests are already queued, so a client sending one request at a time is answered without that wait. A dedicated fall lane protects warnings from ordinary queue saturation and never waits. The service replies successfully only after commit. A request waits up to `INGEST_ENQUEUE_TIMEOUT_MS` (4 seconds as configured) for queue space; after that, saturation returns `503` with `Retry-After`; storage failures return retryable `503`.
- **Identity and retries:** `(device_id, seq, ts)` identifies an event. An identical retry is a duplicate; different content under the same identity is a conflict. Alarm creation is transactional with the event. An advisory transaction fence prevents an interrupted alarm commit from being overtaken by a later cursor.
- **Reads:** Health and occupancy are computed from journal rows at read time and use event time. Late and replayed events therefore correct historical windows. Occupancy before the first known presence state is unknown, not empty.
- **Alarms:** Timestamp-ordered snapshots support history; the live feed follows commit order and resumes with a durable cursor. Strict event-time ordering for arbitrarily late events is not compatible with prompt live delivery.
- **Deployment:** The supported model is a single service instance with coordinated upgrades. The journal currently grows without bound; retention is design-only.

## Verification

The final source state has passed `go test -race -count=1 -p 1 ./...`, vet, and a pooled-DSN preservation check. The full test suite includes integration coverage for retries, identity conflicts, concurrent submissions, replay, read calculations, alarm feed resume, and interrupted commits. The generator and load runs below are separate from the test suite.

The public evaluator checks distinct-fall counts and samples read endpoints. Its exit status alone is not a correctness verdict; compare the printed ground-truth count with the returned alarms. It is narrower than the private grader.

### Public evaluator and failure probes

All five public evaluator modes passed at 50 devices with a fresh database per mode. This table was recorded on commit `11b1fa4`:

| Mode | Duration | HTTP accepted | Distinct falls / alarms |
|---|---:|---:|---:|
| smoke | 30 s | 1,460 | 15 / 15 |
| baseline | 60 s | 2,986 | 38 / 38 |
| burst | 180 s | 15,895 | 202 / 202 |
| offline | 120 s | 5,176 | 69 / 69 |
| adversarial | 240 s | 18,285 | 242 / 242 |

There were no HTTP failures. A separate 30-second API-kill/restart probe acknowledged all 5,400 scheduled events, replayed all 120 offline-buffered events, and delivered all 67 acknowledged falls. An intentionally throttled overload probe produced 533 retryable `503` attempts with `Retry-After`; all 32 events eventually committed exactly once, and a post-acknowledgement retry was recognised as a duplicate.

These are functional checks, not capacity proof. [The verification record](docs/verification/public-evaluator.json) includes source hashes, the official harness revision, counts, isolation and scope.

### Independent source-event verification

A separate HTTP verifier calculates occupancy by intersecting source presence intervals with each window and health by counting original heartbeat second slots. It does not call the service aggregation code or derive expectations from stored rows. Request-time bounds account for real clock progression. It checks all three occupancy windows, predecessor/unknown state, historical replay, heartbeat cutoff boundaries, +/-30-second clocks, retries, 5,001 actual device identities, alarm identities and cursor resume after SIGKILL.

This check found a recent-replay defect missed by earlier tests: a 59-second-old heartbeat arriving after a current heartbeat replaced the clock estimate, excluding newer heartbeat slots. The query now chooses the newest eligible event-time sample; a matching index avoids a lifetime-history sort. The frozen-clock regression failed before the fix and passed afterward. All independent checks then passed: 66 occupancy reads, 28 health reads, four alarm snapshots, six feed/restart/journal checks and four retry assertions, alongside 5,580 ingestion acknowledgements for 5,576 distinct source events. These are not 5,688 distinct aggregation scenarios.

The full race suite passes 108 top-level tests and 56 subtests. [The compact before/after evidence](docs/verification/aggregation-oracle.json) records scope, source hashes and interpretation limits; [the reusable verifier](tools/verification/README.md) documents how to reproduce the low-rate check. Short delivery delay is still indistinguishable from clock skew, and a backwards device-clock jump can retain the prior estimate until it catches up.

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

All five evaluator modes were run again at 50 devices from a fresh clone after this change (commit `52a844b`), each on a fresh database, with no HTTP failures:

| Mode | Duration | HTTP accepted | Distinct falls / alarms |
|---|---:|---:|---:|
| smoke | 30 s | 1,471 | 25 / 25 |
| baseline | 60 s | 3,005 | 53 / 53 |
| offline | 110 s | 5,400 | 87 / 87 |
| burst | 130 s | 32,999 | 432 / 432 |
| adversarial | 135 s | 26,404 | 339 / 339 |

Per second, burst mode now delivers about 2.9 times and adversarial mode about 2.6 times as many events as in the earlier table (254 against 88, and 196 against 76), because the generator is no longer held back by the wait. The durations differ between the two tables.

A grader-shaped run from the same clone used the official generator in adversarial mode with 5,000 devices for 150 seconds, with the service hard-killed at 60 seconds and restarted one second later. The generator sent 111,382 events successfully and 87 failed, all of them refused connections during the outage. The journal held 111,382 rows, and 1,426 alarms matched 1,426 distinct journaled falls, the generator's own count of 1,426 distinct falls, and the 1,426 alarms returned by `GET /alarms?since=0`.

A device that has never sent a heartbeat returns `last_heartbeat_ts: null` with availability 0. A heartbeat dated up to 60 seconds ahead of the server clock is accepted as that device's newest clock sample: it moves `clock_offset_seconds` and `last_heartbeat_ts` forward until real time catches up, while availability changes little.

### Latest instrumented capacity checks

Two fresh five-minute runs of the health fix used a 5,000/s baseline, two 30-second 50,000/s bursts, 20% of devices offline for 60 seconds, and API SIGKILL/restart after three minutes. Both failed the full-offer gate: the first sent and acknowledged 3,767,249 of 4,200,000 scheduled events; an unchanged quiet repeat sent and acknowledged 4,049,045. The generator never sent 432,751 and 150,955 events respectively. These are not demonstrated server refusals, but neither run proves the full scheduled target was handled.

Every acknowledged fall was delivered exactly once (49,758 and 53,508 respectively), all 60,000 offline events replayed in each run, and acknowledged journal counts persisted after the API stopped. Each run passed 183 occupancy and 183 health reads against independent input truth on controlled devices in the same loaded database. This does not validate every load-generator event against an aggregation oracle. [Both attempt records](docs/verification/rubric-load-attempts.json) retain the failed gate, phase counts, latency and source hashes. The cause of the shortfall is analysed below; the burst-capacity target remains unproven at full scale.

The older successful measurements below are historical evidence under their recorded conditions. They do not override these newer unmet gates.

### Shortfall cause and final configuration

The load tool counts a scheduled event as not sent only when every sender is still waiting for a reply and its offer buffer is full. With 2,000 senders, 50,000 events/s needs a mean reply time of 40 ms or less. In the first burst of the first failed run the senders completed about 35,600 events/s, a mean of about 56 ms per request, and the median wait for a free sender was 420 ms; the same build then sent the whole second burst. In the successful scenario the first burst was acknowledged at a 17 ms median. The unsent events therefore come from slower replies on a machine shared by the load tool, the service and PostgreSQL. They are not a scheduling fault and not refusals. The records cannot separate service time from the load tool competing for the same CPUs.

A CPU profile of the service under closed-loop load (2-CPU Linux, 1,000 senders, everything on one machine) put about three quarters of its time in reading, parsing and answering individual HTTP requests and about 14% in memory allocation; PostgreSQL used a third as much CPU as the service. One change followed: Go's garbage-collection target is raised with `GOGC=400` in `compose.yaml` and `.env.example`.

| GOGC | Accepted events/s, three 20-second runs | Service memory |
|---|---|---:|
| 100 (Go default) | 16,548, 16,739 and 17,880 | about 75 MB |
| 400 | 19,319, 19,539 and 20,127 | about 155 MB |

That is about 15% more on the same machine. Forcing PostgreSQL checkpoints inside a burst (`max_wal_size` of 256 MB against 8 GB) made no measurable difference there, so the database keeps its default settings. The 50,000/s schedule was not rerun on a larger machine with this setting, so that target remains unproven.

Final checks used the source at commit `b1b9fdd` with this setting, on 2-CPU Linux with Go 1.26.11 and PostgreSQL 16.15, durability settings on:

- `go test -race -count=1 -p 1 ./...` passed 108 top-level tests and 56 subtests.
- Teton's evaluator in adversarial mode at 5,000 devices for 240 seconds: 217,800 events generated and accepted with no HTTP failures, and 2,780 alarms for 2,780 distinct falls.
- The same run with the service hard-killed at 100 seconds and restarted one second later: 217,730 of 217,817 events accepted and 87 failed during the outage, which the generator does not retry. The journal held 217,731 rows, one of them committed before its reply was lost. 2,855 alarms matched 2,855 distinct journaled falls; the generator counted 2,860, and the other five never reached the service.
- Feed order in that run, with device clocks skewed by up to 30 seconds and offline replay: 64 of 2,855 alarms (2.2%) reached the live feed after an alarm from the same room with a later timestamp, 50 of them within clock skew of their arrival and 14 replayed late. `GET /alarms?since=0` returned all 2,855 in timestamp order.

### Linux capacity and scenario runs

The capacity and latency figures in this section, the section above and the Docker VM section were measured before the conditional batch wait and the heartbeat calibration fix, and were not repeated on the final commit. On the 2-CPU machine used for the official-generator runs, concurrent throughput was the same before and after the conditional wait.

The server, PostgreSQL 16.2, and load process shared a Linux environment where Go reported 9 CPUs. Durability settings stayed enabled. This is not directly comparable with my 4-CPU Docker VM.

| Run | Verified result |
|---|---|
| 30-second closed-loop capacity | 1,755,375 accepted; 58,477/s; 23,162/23,162 acknowledged falls delivered; POST-to-feed p50 9.624 ms, p95 28.714 ms |
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

Retention should use time-based partitions or bounded deletion, chosen after measuring their cost, with a cleanup policy that preserves sufficient history for window calculations. Define and test the retained-state boundary before deletion. Fall deduplication matches reports with the same device, room, timestamp and confidence, which is how the supplied generator repeats a fall; a repeat with a shifted timestamp would become a separate alarm, and merging those needs a physical-fall identifier from the device. Other explicit limits are the single-instance cursor/wakeup model, clock-skew estimates based on the newest eligible event-time heartbeat, and event-time versus live commit-order alarm semantics. No private evaluator score or certification is reported.

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
