# Teton streaming backend

A Go and PostgreSQL service for room sensors. It accepts events over HTTP, tracks device health and room occupancy, and publishes fall alarms that consumers can resume reading after a disconnect. The [upstream challenge](https://github.com/Teton-ai/careers-challenge-streaming-backend) contains the event schema and generator.

## Run locally

From the repository root, start PostgreSQL 16 and the API with Docker Compose.

```sh
docker compose -p teton-streaming up --build -d
curl http://localhost:8080/readyz
```

The API listens on `localhost:8080`. Run one service instance at a time. It applies database migrations on startup and keeps its journal in the Compose database volume. Raw events and alarms are retained indefinitely.

## Ingestion and reads

| Endpoint | Behaviour |
| --- | --- |
| `POST /events` | Validates one sensor event and acknowledges it after PostgreSQL commits. An exact retry returns success with `duplicate: true`. Reusing the same device, sequence and timestamp with different content returns `409`. |
| `GET /devices/{id}/health` | Returns the latest heartbeat and five-minute availability. A recent credible heartbeat calibrates the device clock. |
| `GET /rooms/{id}/occupancy?window=5m` | Returns current presence and occupancy over `1m`, `5m` or `1h`. Replayed events correct the window using their original timestamps. Time before the first known state is reported as unknown. |
| `GET /alarms?since=0` | Returns alarms in event-time order. Supply a receipt timestamp to limit the snapshot. |
| `GET /alarms?after_id=0&wait=10` | Long-polls for committed alarms. Save `next_cursor` after processing a page and pass it as `after_id` when reconnecting. |
| `GET /healthz`, `/readyz`, `/metrics` | Expose process health, database readiness and Prometheus metrics. |

Ordinary events use bounded queues and batched writes. Falls have a separate queue so a backlog of heartbeats cannot fill it. With the default settings, a request that waits four seconds for queue space receives `503` with `Retry-After`.

A fall and its alarm commit together. Repeated reports with the same device, room, timestamp and confidence produce one alarm. The live feed follows commit order so a consumer can resume safely. A fall replayed by an offline sensor can therefore appear after a newer fall from the same room.

## Test

The Go tests require Go 1.26.11 or later and PostgreSQL. Each integration test creates and removes its own database schema. From the repository root in Bash, start the database and load the local settings.

```bash
docker compose -p teton-streaming up -d db
cp -n .env.example .env
set -a && source .env && set +a
go test -race -count=1 -p 1 ./...
```

To run the upstream smoke evaluator, start the full stack and use a fresh test database. The evaluator compares its generated falls with every alarm in the database.

```sh
git clone https://github.com/Teton-ai/careers-challenge-streaming-backend.git ../teton-challenge
python3 ../teton-challenge/eval/check.py smoke --target http://localhost:8080 --devices 50
```

For concurrent burst measurements, use `go run ./tools/load -help`. The tool reports scheduled requests, requests actually sent, durable acknowledgements and alarm latency separately. The service reached two 50,000-event/s bursts in one earlier run. Two repeats sent 90% and 96% of the schedule, so repeatable capacity at that rate remains unverified.

## Code

Request validation and HTTP handlers live in [internal/api](internal/api). The [ingestion writers](internal/ingest) batch events for the [PostgreSQL store](internal/store). [Occupancy calculations](internal/aggregation), [schema migrations](migrations) and [integration tests](tests/integration) are kept alongside the service.

Developed by Junayd Elahi with Claude and Codex.
