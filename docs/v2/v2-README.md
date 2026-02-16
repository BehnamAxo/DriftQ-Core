# DriftQ v2 Foundations (inside DriftQ-Core)

This folder documents the **v2 workflow runtime foundations** that currently live inside DriftQ-Core.

**Reality check:** the broker API (`/v1/*`) is the stable surface today. The v2 runtime is exposed via **debug endpoints** (`/debug/*`) and `driftqctl runs ...` so it's easy to iterate and prove the system before "locking" a public API.


## What you get (today)

- **Durable runs**: a run has durable state + an **append-only event log** (inspectable execution history)
- **Deterministic DAG scheduling**: node dependencies, fan-out/fan-in, retries, attempts
- **Replay**
  - **time_travel** replay: reuse recorded outputs/artifacts
  - **live** replay: re-execute from a chosen step
- **Durable delay** primitive: timers that survive restarts (resume loop)
- **Artifacts + replay cache**: store large step outputs and reuse them on replay
- **Budget/throttle controls**: max attempts, tokens, dollars, wallclock timeout
- **Cancel propagation**: cancel runs and propagate through the DAG
- **Rollback primitive (minimal but real)**: "active index" pointer with **promote** + **rollback**
- **Debug tooling**: inspect run state, timelines, and attempt diffs


## Core concepts

### Run
A **run** is one execution of a workflow.
- `run_id` — stable identifier
- `workflow_id` — logical workflow name
- Nodes / steps execute with **attempt numbers** (`attempt=1`, `attempt=2`, ...)

### Node (step)
A node is a step in a DAG:
- `id` — node identifier in the spec (e.g. `"A"`, `"embed_chunks"`)
- `topic` — handler name (registered in the server's handler registry)
- `deps` — prerequisite node IDs

### Event log
Runs produce events such as:
- `node_started`
- `node_finished` (with proof/telemetry fields)
- `node_failed`
- `run_started`, `run_finished`, `run_canceled`

The event log is what makes replay + debugging sane.


## Quickstart

Run DriftQ-Core (pinned tag recommended):
```bash
docker run --rm -p 8080:8080 -v driftq_data:/data ghcr.io/driftq-org/driftq-core:1.2.0
```

Then use the CLI:
```bash
go build -o driftqctl ./cmd/driftqctl
./driftqctl runs list --base-url http://127.0.0.1:8080
```


## Starting a run from a spec

### POST `/debug/run-spec`

This starts a run from an inline **JSON spec** and input.

Request body:
```json
{
  "run_id": "demo-1",
  "spec": {
    "id": "wf_demo",
    "nodes": [
      {"id": "A", "topic": "sleep_500ms"},
      {"id": "B", "topic": "noop", "deps": ["A"]}
    ]
  },
  "input": {"hello": "world"}
}
```

**Spec format:**

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Workflow identifier |
| `nodes` | array | List of node specs |

**Node spec format:**

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Node identifier |
| `topic` | string | Handler name |
| `deps` | array | Prerequisite node IDs (optional) |
| `retry` | object | Retry policy (optional) |

The server ships with a small set of **demo handlers** registered:
- `noop` — does nothing
- `sleep_500ms` — sleeps for 500ms
- `fail_once` — fails on first attempt, succeeds on retry
- `delay_once_2s` — delays 2s on first attempt via durable timer

(These are for proving durability/replay/timers quickly.)

**Response:**
```json
{"ok": true, "run_id": "demo-1", "trace_id": "abc123"}
```


## Inspecting a run

### GET `/debug/run?run_id=<RUN_ID>`

Returns a compact summary of the run with per-node status rows.

### GET `/debug/run-state?run_id=<RUN_ID>`

Returns the full durable run state plus the event log (`events`), node executions (`nodes`), and timers (`timers`).

### GET `/debug/runs?limit=50`

Lists all runs (newest first).

**CLI wrappers:**
```bash
# List runs
./driftqctl runs list --base-url http://127.0.0.1:8080

# Get run status (uses /debug/run)
./driftqctl runs status --base-url http://127.0.0.1:8080 --run-id <RUN_ID>

# Get full state (uses /debug/run-state)
./driftqctl runs state --base-url http://127.0.0.1:8080 --run-id <RUN_ID>
```

### Timeline (proof fields)
```bash
./driftqctl runs timeline --base-url http://127.0.0.1:8080 --run-id <RUN_ID>
```

Timeline includes proof/telemetry fields when present:
- `used_cached_output`, `cached_attempt`
- `queued_at`, `started_at`, `ended_at`
- `queue_ms`, `worker_ms`


## Replay

### POST `/debug/run-replay`

Request body:
```json
{
  "run_id": "demo-1",
  "from_step": "B",
  "mode": "time_travel"
}
```

**Modes:**

| Mode | Description |
|------|-------------|
| `time_travel` | Reuse recorded outputs/artifacts when possible (fast, deterministic) |
| `live` | Re-execute from `from_step` (good for "try again" after fixing code) |

**Response:**
```json
{"ok": true, "run_id": "demo-1", "from_step": "B", "mode": "time_travel", "trace_id": "abc123"}
```

**CLI wrapper:**
```bash
./driftqctl runs replay --base-url http://127.0.0.1:8080 --run-id demo-1 --from-step B --mode time_travel
```

> **Note:** The CLI accepts `--mode time-travel` (with hyphen) as an alias for `time_travel`.


## Cancel

### POST `/debug/run-cancel`

Request body:
```json
{
  "run_id": "demo-1",
  "reason": "user requested stop"
}
```

**CLI wrapper:**
```bash
./driftqctl runs cancel --base-url http://127.0.0.1:8080 --run-id demo-1 --reason "stopping"
```


## Attempt diffs

When a step retries (attempt 1 → attempt 2), you often want: "what changed?"

**CLI:**
```bash
./driftqctl runs diff --base-url http://127.0.0.1:8080 --run-id <RUN_ID> --node-id <NODE_ID> --from 1 --to 2
```

This compares attempt metadata (status, error, duration) and output payload.


## Artifacts

Artifacts are how you store large outputs outside the event log (and later reuse them on replay).

### GET `/debug/run-artifacts?run_id=<RUN_ID>`

Lists all artifacts for a run.

**Response:**
```json
{
  "ok": true,
  "run_id": "demo-1",
  "count": 2,
  "artifacts": [...]
}
```

### GET `/debug/artifact-meta?artifact_id=<ID>`

Get metadata for a specific artifact.

### GET `/debug/artifact-get?artifact_id=<ID>`

Download the artifact blob.

**CLI wrapper:**
```bash
./driftqctl runs artifacts --base-url http://127.0.0.1:8080 --run-id <RUN_ID>
```


## Demo workflow

### POST `/debug/run-demo`

Starts the built-in demo workflow. Optionally pass input:

Query param: `?x=5`

Or JSON body:
```json
{"x": 5}
```

**CLI wrapper:**
```bash
./driftqctl runs demo --base-url http://127.0.0.1:8080
```


## Index pointer promote / rollback

This is a minimal rollback primitive for "active index" style workflows (e.g., build a new index version, validate, then flip traffic).

### GET `/debug/index/active`

Returns the current active index version pointer.

**Response:**
```json
{"ok": true, "active_version": "v1.2.3"}
```

### POST `/debug/index/promote`

Promotes a run's output to be the active index version.

Query params or JSON body:
```json
{
  "run_id": "build-run-123",
  "version": "v1.2.3"
}
```

### POST `/debug/index/rollback`

Rolls back to a specific version.

Query param or JSON body:
```json
{
  "version": "v1.2.2"
}
```

The pointer itself is persisted via a WAL-backed KV entry, so it survives restarts.


## Debug topic endpoints

These endpoints provide broker debugging capabilities:

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/debug/topics` | List topics |
| GET | `/debug/topics/peek?topic=T&limit=N` | Peek at messages |
| GET | `/debug/topics/lag?group=G&topic=T` | Consumer lag info |
| POST | `/debug/topics-create` | Create topic (helper) |
| GET | `/debug/metrics` | Engine metrics |


## API Reference

### Run Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/debug/run-spec` | Start a run from JSON spec |
| GET | `/debug/runs?limit=N` | List all runs |
| GET | `/debug/run?run_id=ID` | Get run summary |
| GET | `/debug/run-state?run_id=ID` | Get full run state with events |
| POST | `/debug/run-replay` | Replay a run |
| POST | `/debug/run-cancel` | Cancel a run |
| POST | `/debug/run-demo` | Start demo workflow |

### Artifacts

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/debug/run-artifacts?run_id=ID` | List artifacts for a run |
| GET | `/debug/artifact-meta?artifact_id=ID` | Get artifact metadata |
| GET | `/debug/artifact-get?artifact_id=ID` | Download artifact |

### Index Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/debug/index/active` | Get active index version |
| POST | `/debug/index/promote` | Promote index version |
| POST | `/debug/index/rollback` | Rollback index version |

### Debug/Metrics

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/debug/metrics` | Engine metrics |
| GET | `/debug/topics` | List topics |
| GET | `/debug/topics/peek` | Peek topic messages |
| GET | `/debug/topics/lag` | Consumer lag info |


## Stability & safety notes ⚠️

- `/debug/*` is **not a stable public API** yet. It's intentionally fast to evolve.
- Pin Docker tags in production (use `1.2.0`, not `latest`).
- WAL is forward-compatible only: don't downgrade binaries after writing newer ops.


## What's next (directionally)

These foundations are the base for:
- workflow primitives (timers/delay, cancel propagation, join/barrier)
- budgets & throttles at scale
- replayable runs at scale
- RAG ingestion / index builder demos (in separate repos) that pin DriftQ-Core images
