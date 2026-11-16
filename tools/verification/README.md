# Independent correctness verifier

This optional verifier builds the Go server and checks it through HTTP against
an independently calculated source-event history. It creates a random database,
kills only its own API process, restarts it, and drops only that random database.
It never changes tables in the database named by the supplied URL.

Requirements: Linux or WSL for SIGKILL, Python 3.10+, Go, PostgreSQL and the
optional Python `psycopg2` driver. The database account must be able to create
and drop databases. The Go service has no Python dependency.

With the local Compose database running, from the repository root:

```bash
python3 -m venv /tmp/teton-verifier-env
source /tmp/teton-verifier-env/bin/activate
python3 -m pip install psycopg2-binary
set -a && source .env && set +a
python3 tools/verification/blackbox_oracle.py --out /tmp/teton-verification
```

Exit zero means all tested expectations passed; a mismatch or execution error
exits nonzero. Output includes `report.json`, `http.jsonl`, `event_truth.json`,
source hashes and a binary hash. Without `--out`, a new temporary directory is
used. `--repo` selects a different checkout.

## What is checked

- Occupancy duration for 1m, 5m and 1h windows: predecessor state, unknown time,
  shuffled transitions, repeated states and historical replay corrections.
- Latest heartbeat, distinct one-second coverage, cutoff boundaries, exact
  retries, missing devices, old replay and clocks skewed by +/-30 seconds.
- A recent delayed heartbeat must not replace a newer live calibration sample.
- Five thousand actual device identities followed by another device, without
  restarting or changing configuration.
- The official generator's fall jitter: sequence changes while timestamp and
  confidence stay equal. Separate falls remain separate.
- Health, occupancy, journal rows, alarm identities and cursor resume after an
  observed SIGKILL and restart.

Expected values come from the original HTTP input events, not the application's
calculation functions or stored rows. Reads retain client request start/end
times; comparisons cover the range of correct answers within those intervals.
The final journal row count is a separate durability cross-check.

## Interpretation and limits

The challenge does not define equal-time presence ties, the exact one-second
availability bin convention, unknown occupancy presentation or future presence's
current boolean. The report states those assumptions separately. Health checks
bound clock offsets using fresh heartbeat timestamps and HTTP receipt intervals.

There is no physical-fall identifier in the schema. The verifier follows the
public generator's identity; it does not certify merging repeats whose timestamp
or confidence changes. The resumable feed is checked in commit order, preserving
late alarms. Unconditional event-time order with arbitrarily late arrivals and
immediate delivery cannot both be guaranteed.

This is a low-rate correctness suite. It does not award the private grader's
score or prove aggregation correctness for every event during a 50,000/s burst.
The separate capacity evidence records its own workload and read-check scope.
