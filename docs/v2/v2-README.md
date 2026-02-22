# DriftQ v2 Foundations (inside DriftQ-Core) 🧱

This folder documents the **v2 workflow runtime foundations** that currently live inside DriftQ-Core.

**Reality check:** the broker API (`/v1/*`) is the stable surface today. The v2 runtime is exposed via **debug endpoints** (`/debug/*`) and `driftqctl runs ...` so it’s easy to iterate and prove the system before a locked public API.

---

## What you get (today)

- **Durable runs**: a run has durable state + an **append-only event log**
- **Deterministic DAG scheduling**: deps, fan-out/fan-in, retries, attempts
  - Duplicate + empty node IDs rejected at parse time
  - Cycle detection (DFS) before execution begins
- **Replay**
  - **time-travel** replay: reuse recorded outputs/artifacts
  - **live** replay: re-execute from a chosen step
- **Durable delay** primitive: timers that survive restarts
- **Artifacts + replay cache**: store big outputs and reuse on replay
- **Budget/throttle controls**: max attempts, wallclock timeout
- **Cancel propagation**
- **Rollback primitive (minimal but real)**: “active index” pointer (promote/rollback)

If you’re here for “Temporal-like durability + replay”, this is the track.

---

## Quickstart (local)

### Option A: run with Docker (fastest)
Pinned tag:
```bash
docker run --rm -p 8080:8080 -v driftq_data:/data ghcr.io/driftq-org/driftq-core:1.2.0
```

This runs `driftqd` with:
- broker WAL at `/data/driftq.wal`
- v2 engine store defaults to `memory` unless you enable file mode

### Enable durable engine storage + artifacts (recommended for v2 demos)
```bash
docker run --rm -p 8080:8080 -v driftq_data:/data ghcr.io/driftq-org/driftq-core:1.2.0   -engine-store file   -engine-wal /data/engine.wal   -artifacts-dir /data/artifacts
```

Health:
```bash
curl http://127.0.0.1:8080/v1/healthz
```

**Windows PowerShell notes**
- use `curl.exe` (PowerShell aliases `curl`)
- streaming endpoints: `curl.exe --no-buffer`

### Option B: run from source (dev)
Go version: **Go 1.25** (see `go.mod`).

```bash
# in-memory engine, file-based broker WAL (default)
go run ./cmd/driftqd

# durable engine + artifacts stored under ./data
mkdir -p ./data
go run ./cmd/driftqd   --wal ./data/driftq.wal   --engine-store file   --engine-wal ./data/engine.wal   --artifacts-dir ./data/artifacts
```

---

## driftqctl (CLI)

Build:
```bash
go build -o driftqctl ./cmd/driftqctl
```

Windows:
```powershell
go build -o driftqctl.exe ./cmd/driftqctl
```

Sanity check:
```bash
./driftqctl --base-url http://127.0.0.1:8080 runs list --limit 20
```

---

## Fastest demo: run the built‑in demo workflow

Start a demo run:
```bash
./driftqctl --base-url http://127.0.0.1:8080 runs demo
```

Then:
```bash
./driftqctl --base-url http://127.0.0.1:8080 runs list --limit 20
./driftqctl --base-url http://127.0.0.1:8080 runs status --run-id <RUN_ID>
./driftqctl --base-url http://127.0.0.1:8080 runs timeline --run-id <RUN_ID>
./driftqctl --base-url http://127.0.0.1:8080 runs artifacts --run-id <RUN_ID>
```

Replay:
```bash
# live replay (re-execute steps) via driftqctl
./driftqctl --base-url http://127.0.0.1:8080 runs replay --run-id <RUN_ID> --from-step <STEP_ID> --mode live

# time-travel replay (reuse recorded outputs/artifacts) via the debug endpoint
curl -i -X POST "http://127.0.0.1:8080/debug/run-replay" -H "Content-Type: application/json" -d '{"run_id":"<RUN_ID>","from_step":"<STEP_ID>","mode":"time_travel"}'
```


Cancel:
```bash
./driftqctl --base-url http://127.0.0.1:8080 runs cancel --run-id <RUN_ID> --reason "stopping"
```

---

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

Example:
```bash
curl -i -X POST "http://127.0.0.1:8080/debug/run-spec"   -H "Content-Type: application/json"   -d '{"run_id":"demo-1","spec":{"id":"wf_demo","nodes":[{"id":"A","topic":"sleep_500ms"},{"id":"B","topic":"noop","deps":["A"]}]},"input":{"hello":"world"}}'
```

**Spec format**

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Workflow identifier |
| `nodes` | array | List of node specs |

**Node spec format**

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Node identifier (must be non-empty + unique) |
| `topic` | string | Handler name |
| `deps` | array | Prerequisite node IDs (optional) |
| `retry` | object | Retry policy (optional) |

**Validation rules**
- Node `id` must not be empty
- Node `id` values must be unique
- DAG must be acyclic
- All IDs referenced in `deps` must exist

The server ships with a small set of **demo handlers**:
- `noop`
- `sleep_500ms`
- `fail_once` (fails first attempt, succeeds on retry)
- `delay_once_2s` (durable timer delay on first attempt)

Response:
```json
{"ok": true, "run_id": "demo-1", "trace_id": "abc123"}
```

---

## Inspecting a run

### GET `/debug/run?run_id=<RUN_ID>`
Returns a compact summary of the run with per-node status rows.

### GET `/debug/run-state?run_id=<RUN_ID>`
Returns the full durable run state plus:
- `events` (event log)
- `nodes` (node executions)
- `timers` (durable timers)

CLI equivalents:
```bash
./driftqctl --base-url http://127.0.0.1:8080 runs status --run-id <RUN_ID>
./driftqctl --base-url http://127.0.0.1:8080 runs state --run-id <RUN_ID>
./driftqctl --base-url http://127.0.0.1:8080 runs events --run-id <RUN_ID>
./driftqctl --base-url http://127.0.0.1:8080 runs step --run-id <RUN_ID> --node-id <NODE_ID>
```

---

## Artifacts

### GET `/debug/run-artifacts?run_id=<RUN_ID>`
Lists artifacts for a run (requires artifacts store to be configured).

If you started DriftQ with:
- `-artifacts-dir /data/artifacts` (Docker) or
- `--artifacts-dir ./data/artifacts` (from source)

…then you can list artifacts:
```bash
./driftqctl --base-url http://127.0.0.1:8080 runs artifacts --run-id <RUN_ID>
```

You can also query:
- `/debug/artifact-meta?artifact_id=<ARTIFACT_ID>`
- `/debug/artifact-get?artifact_id=<ARTIFACT_ID>`

---

## Metrics

- `/metrics` → Prometheus metrics (broker + some v2 counters)
- `/debug/metrics` → JSON snapshot of internal runner metrics (dev/debug)

---

## Troubleshooting

- **Nothing shows up in the demo:** make sure the server is running and `/v1/healthz` returns `{"status":"ok"}`.
- **Artifacts endpoints say “not configured”:** you started with `--artifacts-dir ""` or left it unset. Re-run with a directory.
- **PowerShell streaming:** use `curl.exe --no-buffer`.
