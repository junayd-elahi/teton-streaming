# Submission, Real-time Streaming Backend

**Your name:** Junayd Elahi

**Email:** Included privately with the submission.

**Link to your fork or solution:** https://github.com/junayd-elahi/teton-streaming

---

## Stack and storage

Go's `net/http` and `pgx` give one binary that handles many open requests cheaply. PostgreSQL 16 stores the journal and alarms transactionally, so an alarm commits with its event without a broker; success follows commit. HTTP/JSON matches the challenge generator and is easy to inspect.

## Ordering and late events

Identity combines device, sequence, and event timestamp. Journal-based reads use event time, so replay corrects history. Falls deduplicate by device, room, timestamp and confidence, matching supplied jitter copies. Live alarms follow durable commit order; snapshots use event time.

## Backpressure

Bounded ordinary lanes and a separate fall lane provide backpressure. A 10x burst is queued and delayed, not dropped. Saturation lasting four seconds returns `503` with `Retry-After`; identity checks make retries safe. Database contention can still delay falls.

## Restart correctness

Acknowledgements follow commit. Journal rows, alarms, and feed cursors survive restart. A database-level fence stops an interrupted fall commit surfacing behind a newer cursor.

## How to run it locally

```bash
docker compose -p teton-streaming up -d db
cp .env.example .env
set -a && source .env && set +a
go run ./cmd/server
```

From Teton's repository, run `python3 event_generator/generate.py --mode adversarial --target http://localhost:8080 --devices 200 --duration 125`. Use a fresh database.

## Reported metrics

- Sustained ingest rate: 58,477 events/s in a 30-second closed-loop run (Linux, 9 CPUs). One five-minute scenario accepted both 50,000/s bursts; two repeats sent 90% and 96% of the schedule, so that target is not reliably met. My 4-CPU Docker VM reached about 13,200/s. Teton's sequential generator reaches 886 events/s at 5,000 devices. Capacity figures predate the final batching change.
- Alarm feed latency p50 / p95: POST-to-feed 9.6 / 28.7 ms in that closed-loop run; 23,162 of 23,162 falls delivered.
- Behavior under hard kill + restart: every acknowledged event and fall survived; under Teton's generator at 5,000 devices, 1,426 alarms matched 1,426 distinct falls. The database was not killed.
- Aggregation correctness on replayed events: at low rate, an independent source-event verifier passed 66 occupancy and 28 health reads.

## With another week

I would implement journal retention, by time-based partitions or bounded deletion. First I would define and test the history that reads need: room and device checkpoints, retry hashes and feed cursors.
