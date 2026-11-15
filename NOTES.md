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

- Each event type's own field is validated (boolean, 0 to 1 range, enum, number). Bad input is a 400. See the hardening section for inputs that used to slip through.
- Requests go into a bounded queue. One writer takes whatever is waiting, up to `INGEST_BATCH_SIZE`, and commits it as one transaction. Batches grow by themselves under load; there is no flush timer.
- One writer means journal id order equals commit order.
- If the queue stays full for `INGEST_ENQUEUE_TIMEOUT_MS`, the request gets 503 with `Retry-After`. Storage failure is also 503. Both are safe to retry.
- Inside a batch, the first event for an identity wins; later copies are duplicates, later different content is a conflict.

Verified:

- `go test -race -count=5 ./...` passes.
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
- Teton's smoke evaluator, 50 devices: 1,464 sent, 0 failed, both read endpoints answer.
- Both read queries use the partial indexes (checked with EXPLAIN on a small table).

## Phase 4: fall alarms and resumable feed (done 15 Nov 2026)

- One logical fall = one row in `alarms`, keyed by device, room, event time and confidence. Jitter copies carry new sequence numbers but the same four values, so they collapse. All copies stay in the journal.
- The alarm is created in the same transaction as its event, by the single journal writer. There is no separate worker or checkpoint: an acknowledged fall always has its alarm, and a crash can never leave one without the other.
- `alarms.id` is the feed cursor. Because one writer commits batches one at a time, ids become visible in increasing order, so a consumer at cursor N cannot miss a later alarm.
- `GET /alarms?since=<ts>` is a snapshot ordered by event time. `since` is unix seconds or a timestamp.
- `GET /alarms?after_id=<cursor>&wait=<seconds>` is the feed, in commit order, long-polling up to 30 s. The writer wakes waiting consumers when a batch commits a new alarm.
- Consumers may see an alarm twice if they disconnect before saving the cursor; `alarm_id` makes that harmless.

Known limits, chosen on purpose:

- The brief asks for the feed "in order per room". The feed is in commit order, which equals event order per room except when a fall arrives late. A late fall is delivered as soon as it commits, with its original `ts`, instead of being held back or dropped. Sub-second delivery, hour-late arrivals and strict event order cannot all hold at once.
- Two real falls from one device with the same millisecond timestamp and the same confidence would merge. Falls a few seconds apart with different timestamps stay separate; merging those would need a time-window heuristic the brief does not define.

Verified:

- Three jitter copies plus an exact retry: 3 journaled events, 1 alarm. 30 simultaneous copies: 1 alarm.
- Falls differing in time, device or confidence: separate alarms.
- Cursor saved, service restarted, new falls sent including one with an older event time and a jitter copy: resume returns exactly the two new alarms, in commit order.
- Long poll answers within a second of a fall being sent; an empty wait returns after the wait.
- Teton's generator, adversarial mode, 300 devices, service hard-killed and restarted mid-run: 22,606 acknowledged, 22,606 journaled, 289 distinct falls among them, 289 alarms. The generator does not retry, so the 272 requests it sent while the service was down are not in the journal.

## Hardening after an independent test and review (15 Nov 2026)

The service was attacked with hostile input and reviewed against the brief. Defects found and fixed:

- **Poison events.** Values PostgreSQL refuses (NUL characters, numbers beyond float range or with huge literals, ids too long for an index) passed validation, failed the batch insert, and turned every valid event in that batch into a 503. Measured before the fix: 60 poison events caused 400 valid events to be refused. Now they are rejected as 400 before queueing. As a backstop, when PostgreSQL refuses a batch over its data, the writer commits each event alone, so only the bad one fails. After the fix: 4,740 of 4,740 valid events accepted alongside 60 poison events.
- **Alarm reads broken by one fall.** A confidence of `1e-400` parsed to zero, passed the range check, and then failed conversion on every alarm read, including after restart. Underflowing numbers are now rejected, and confidence is stored as a float so a read cannot fail on it.
- **Query parameters.** `NaN`, infinities and out-of-range values for `since` and `wait` are rejected.
- **Request body.** Data after the JSON object is rejected.
- **Server.** Read, write and idle timeouts are set. The database port is bound to localhost and the container runs as a non-root user.
- **Alarm shape.** `event_id` added, as documented by Teton's evaluator.

Rough throughput measured by the independent tester on a 2 vCPU machine, with server, database and load tool sharing it: about 14,000 events/s at 1,000 concurrent senders, alarm delivery p95 130 ms, no refusals. That machine cannot say whether 50,000/s is reachable.

## Phase 5: priority, clock skew, overload and observability (done 15 Nov 2026)

- **Fall priority.** Fall warnings have their own queue. Every batch takes all waiting falls first and then fills up with other events, so a fall waits for at most the batch already being committed. A full normal queue cannot cause a fall to be refused.
- **Stalled database.** A batch that takes longer than `INGEST_COMMIT_TIMEOUT_MS` fails with a retryable 503 and the writer moves on, instead of blocking every sender behind it.
- **Clock skew in health.** The schema allows device clocks to be 30 s or more off. Health is now measured on the device's own clock: the offset is the latest heartbeat's timestamp minus the time it was received, and the 300 s window ends at server time plus that offset. A healthy device scores 1.0 whether its clock is right, behind or ahead. A device that stops keeps its last offset, so its availability falls as real time passes. The response includes `clock_offset_seconds`.
- **Clock skew in occupancy.** `in_room` follows the latest presence event even when it is dated ahead of the server clock ("latest presence event wins"). Such an event adds no occupied time until the server clock reaches its timestamp. Occupancy windows are not corrected per device; with 30 s of skew a transition can be placed up to 30 s off.
- **Identity.** An event is identified by `(device_id, seq, ts)`. A device or generator that restarts its sequence numbers produces new events, not conflicts. A true retry carries the same timestamp, so retries are still recognised. Same identity with different content is still a 409.
- **Alarm snapshot.** `since` filters by when the service received the fall, so a poller passing its last poll time also gets falls that arrived late. Results stay ordered by event time.
- **Observability.** `GET /metrics` serves Prometheus text: events by outcome and by type, queue depth for both queues, batch size, commit time, queue wait, request time, alarms created, time from receiving a fall to its alarm being committed, batches split over refused data, waiting feed consumers, reads by endpoint and read errors. `GET /readyz` checks that the database answers; `GET /healthz` only says the process is up.

Verified:

- 12 heartbeats queued, then 2 falls, batches of 5: both falls are in the first batch.
- Normal queue full: a heartbeat is refused as overloaded, a fall sent at the same moment is accepted.
- Stalled database: each submit fails after about the commit timeout and the writer keeps serving.
- A device sending once a second for 5 minutes scores 1.0 with its clock exact, 30 s behind and 30 s ahead, with the offset reported as 0, -30 and +30.
- A device 30 s behind that stopped 2 minutes ago scores 0.6.
- Same device and sequence number with a new timestamp: accepted as a new event.
- A fall that happened before the poller's last poll but arrived after it is returned.

Chosen limits:

- A single heartbeat with a wildly wrong timestamp (up to the one hour allowed) moves that device's estimated clock until a later heartbeat replaces it.
- Single instance only: the alarm wake-up signal and the commit ordering live in the process.
- The journal grows without bound. Reads need about an hour of it; time-based partitions with a drop job are the intended fix.

## Run

```bash
docker compose up -d db
cp .env.example .env && set -a && source .env && set +a
go run ./cmd/server
go test -race -count=1 -p 1 ./...
```

## Deferred

Load measurement, journal retention.
