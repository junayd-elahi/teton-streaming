# Submission, Real-time Streaming Backend

**Your name:** Junayd Elahi

**Email:** Included privately with the submission.

**Link to your fork or solution:** https://github.com/junayd-elahi/teton-streaming

---

## Stack and storage

Go (`net/http`, `pgx`) provides a direct single-process API. PostgreSQL 16 stores the event journal and alarms transactionally, so an alarm commits with its event without a broker; success follows commit. HTTP/JSON matches the challenge generator.

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

- Sustained ingest rate: 58,477 accepted events/s in a 30-second closed-loop run (Linux, 9 CPUs). One five-minute scenario accepted both 50,000/s bursts; two repeats sent 90% and 96% of the schedule, so that target is not reliably met. My 4-CPU Docker VM reached about 13,200/s. Teton's sequential generator reaches 886 events/s at 5,000 devices.
- Alarm feed latency p50 / p95: closed-loop POST-to-feed 9.6 / 28.7 ms; 23,162 of 23,162 falls delivered.
- Behavior under hard kill + restart: every acknowledged event and fall survived; under Teton's generator at 5,000 devices, 1,426 alarms matched 1,426 distinct falls. The database was not killed.
- Aggregation correctness on replayed events: an independent source-event verifier passed 66 occupancy and 28 health reads, including replay and clock skew.

## With another week

I would implement journal retention, by time-based partitions or bounded deletion. First I would define and test the history needed to calculate occupancy and health, preserving room/device checkpoints, retry hashes and feed-cursor guarantees.
