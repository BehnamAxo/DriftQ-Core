# DriftQ v2 Foundations (inside DriftQ-Core)

This folder documents the **v2 workflow runtime foundations** that currently live inside DriftQ-Core.

**Reality check:** the broker API (`/v1/*`) is the stable surface today. The v2 runtime is exposed via **debug endpoints** (`/debug/*`) and `driftqctl runs ...` so it’s easy to iterate and prove the system before "locking" a public API.


## What you get (today)

- **Durable runs**: a run has durable state + an **append-only event log** (inspectable execution history)
- **Deterministic DAG scheduling**: node dependencies, fan-out/fan-in, retries, attempts
- **Replay**
  - **time-travel** replay: reuse recorded outputs/artifacts
  - **live** replay: re-execute from a chosen step
- **Durable delay** primitive: timers that survive restarts (resume loop)
- **Artifacts + replay cache**: store large step outputs and reuse them on replay
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
- `step_id` — node identifier in the spec (e.g. `"A"`, `"embed_chunks"`)
- `topic` — handler name (registered in the server’s handler registry)
- `depends_on` — prerequisite step_ids

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
docker run --rm -p 8080:8080 -v driftq_data:/data ghcr.io/driftq-org/driftq-core:v1.2.0
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
  "spec": "{"workflow_id":"wf_demo","nodes":[{"id":"A","topic":"sleep_500ms"},{"id":"B","topic":"noop","depends_on":["A"]}]}",
  "input": {"hello":"world"}
}
```

Notes:
- `spec` is a **string** containing JSON for:
  - `workflow_id` (string)
  - `nodes` (array of `{id, topic, depends_on?}`)

The server ships with a small set of **demo handlers** registered (examples):
- `noop`
- `sleep_500ms`
- `fail_once`
- `delay_once_2s`

(These are for proving durability/replay/timers quickly.)


## Inspecting a run

### GET `/debug/run-state?run_id=<RUN_ID>`
Returns the durable run state plus the event log (`events`).

CLI wrapper:
```bash
./driftqctl runs status --base-url http://127.0.0.1:8080 --run-id <RUN_ID>
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

Modes:
- `time_travel` — reuse recorded outputs/artifacts when possible (fast, deterministic)
- `live` — re-execute from `from_step` (good for "try again" after fixing code)

CLI wrapper:
```bash
./driftqctl runs replay --base-url http://127.0.0.1:8080 --run-id demo-1 --from-step B --mode time_travel
```

## Attempt diffs

When a step retries (attempt 1 → attempt 2), you often want: "what changed?"

CLI:
```bash
./driftqctl runs diff --base-url http://127.0.0.1:8080 --run-id <RUN_ID> --step <STEP_ID> --from 1 --to 2
```

This compares attempt metadata (status, error, duration) and output payload.


## Artifacts

Artifacts are how you store large outputs outside the event log (and later reuse them on replay).

Debug endpoints:
- `GET /debug/artifact-meta?artifact_id=...`
- `GET /debug/artifact-get?artifact_id=...`


## Index pointer promote / rollback

This is a minimal rollback primitive for "active index" style workflows (e.g., build a new index version, validate, then flip traffic).

Endpoints:
- `GET /debug/index/active`
- `GET /debug/index/versions`
- `POST /debug/index/promote`
- `POST /debug/index/rollback`

The pointer itself is persisted via a WAL-backed KV entry, so it survives restarts.


## Stability & safety notes ⚠️

- `/debug/*` is **not a stable public API** yet. It’s intentionally fast to evolve.
- Pin Docker tags in production.
- WAL is forward-compatible only: don’t downgrade binaries after writing newer ops.


## What’s next (directionally)

These foundations are the base for:
- workflow primitives (timers/delay, cancel propagation, join/barrier)
- budgets & throttles
- replayable runs at scale
- RAG ingestion / index builder demos (in separate repos) that pin DriftQ-Core images
