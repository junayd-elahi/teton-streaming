# Submission, Real-time Streaming Backend

**Your name:** Junayd Elahi

**Email:** Included privately with the submission.

**Link to your fork or solution:** https://github.com/junayd-elahi/teton-streaming

---

## Stack and storage

Go (`net/http`, `pgx`) provides a direct single-process API. PostgreSQL 16 stores the event journal and alarms; success follows transaction commit. HTTP/JSON matches the challenge generator.

## Ordering and late events

Identity combines device, sequence, and event timestamp. Journal-based reads use event time, so replay corrects history. Falls deduplicate by device, room, timestamp and confidence, matching supplied jitter copies. Timestamp-shifted repeats need a physical-fall identifier. Live alarms follow durable commit order; snapshots use event time. Strict live event-time order with arbitrary replay would require delayed delivery.

## Backpressure

Bounded ordinary lanes and a separate fall lane provide backpressure. Sustained saturation returns retryable `503` with `Retry-After`; identity checks make retries safe. Database contention can still delay falls.

## Restart correctness

Acknowledgements follow durable commit. Journal rows, alarms, and feed cursors survive restart. A transaction advisory fence prevents an interrupted fall commit from appearing after a newer cursor advances.

## How to run it locally

```bash
docker compose -p teton-streaming up -d db
cp .env.example .env
set -a && source .env && set +a
go run ./cmd/server
```

From Teton’s repository, run `python3 event_generator/generate.py --mode adversarial --target http://localhost:8080 --devices 200 --duration 125`. Use a dedicated test database.

## Reported metrics

- Sustained ingest rate: 58,477 accepted events/s in a 30-second closed-loop Linux run reporting 9 CPUs; the five-minute scenario accepted both scheduled 50,000/s cohorts (1.5 million each). My 4-CPU Docker VM reached about 13,200/s in its burst.
- Alarm feed latency p50 / p95: Closed-loop POST-to-feed p50/p95 9.624/28.714 ms, with 23,162/23,162 falls delivered. Full-scenario burst schedule-to-feed p95: 100.119 ms and 25.902 ms.
- Behavior under hard kill + restart: all 4.2 million scenario events, including offline replay, were acknowledged; journal counts persisted after API stop. The database was not killed.
- Aggregation correctness on replayed events: fixed-clock tests cover late-event corrections; 61 live checks validated shape/ranges, not an independent aggregation oracle.

## With another week

I would implement time-based journal partitions and cleanup. First I would define and test the history needed to calculate occupancy and health, preserving room/device checkpoints, retry hashes and feed-cursor guarantees.
