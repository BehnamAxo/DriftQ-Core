# DriftQ v2 Foundations (inside DriftQ-Core)

This document covers the current v2 workflow runtime that ships inside DriftQ-Core.

Reality check:
- Stable public surface today is still broker v1 (`/v1/*`).
- v2 is currently exposed through debug endpoints (`/debug/*`) and `driftqctl runs ...`.

## What exists today

- Run model with persisted state + append-only event log when `--engine-store file` is enabled.
- Deterministic DAG execution with dependency ordering and cycle validation.
- Replay controls: `live` re-executes from a chosen step, `time_travel` reuses recorded outputs/artifacts when possible.
- Durable delay/timer flow (`Delay(...)` in handlers, timer fire + resume loop in server).
- Artifact storage + retrieval endpoints.
- Run cancel support.
- Minimal index pointer promote/rollback controls.

Important behavior notes:
- Automatic "retry on failure" is not a generic built-in node policy today.
- `retry.max_attempts` is parsed in spec and stored, but current execution does not auto-retry failed nodes from that field.
- Demo handler `fail_once` succeeds on a later attempt when you replay.

## Quickstart

### Option A: Docker

Pinned image:

```bash
docker run --rm -p 8080:8080 -v driftq_data:/data ghcr.io/driftq-org/driftq-core:1.2.0
```

This starts:
- broker WAL at `/data/driftq.wal`
- engine store in memory by default
- artifact store at default path (`driftq.artifacts`) inside the container filesystem

Durable engine + persistent artifacts on the mounted volume:

```bash
docker run --rm -p 8080:8080 -v driftq_data:/data ghcr.io/driftq-org/driftq-core:1.2.0 \
  -engine-store file \
  -engine-wal /data/engine.wal \
  -artifacts-dir /data/artifacts
```

Health check:

```bash
curl http://127.0.0.1:8080/v1/healthz
```

### Option B: From source

`go.mod` currently targets Go `1.25`.

Default run:

```bash
go run ./cmd/driftqd
```

Durable engine + durable artifacts:

```bash
mkdir -p ./data
go run ./cmd/driftqd \
  --wal ./data/driftq.wal \
  --engine-store file \
  --engine-wal ./data/engine.wal \
  --artifacts-dir ./data/artifacts
```

Windows PowerShell notes:
- Use `curl.exe` instead of `curl`.
- For streaming output, use `curl.exe --no-buffer`.

## CLI quickstart (`driftqctl`)

Build:

```bash
go build -o driftqctl ./cmd/driftqctl
```

Windows:

```powershell
go build -o driftqctl.exe ./cmd/driftqctl
```

Sanity:

```bash
./driftqctl --base-url http://127.0.0.1:8080 runs list --limit 20
```

## End-to-end v2 flow (fast)

1. Start demo run:

```bash
./driftqctl --base-url http://127.0.0.1:8080 runs demo
```

Expected output includes:
- `run_id=<...>`
- optional `trace_id=<...>`

2. Inspect:

```bash
./driftqctl --base-url http://127.0.0.1:8080 runs status --run-id <RUN_ID>
./driftqctl --base-url http://127.0.0.1:8080 runs events --run-id <RUN_ID>
./driftqctl --base-url http://127.0.0.1:8080 runs timeline --run-id <RUN_ID>
./driftqctl --base-url http://127.0.0.1:8080 runs artifacts --run-id <RUN_ID>
```

3. Replay from a step:

Live replay:

```bash
./driftqctl --base-url http://127.0.0.1:8080 runs replay --run-id <RUN_ID> --from-step <STEP_ID> --mode live
```

Time-travel replay:

```bash
./driftqctl --base-url http://127.0.0.1:8080 runs replay --run-id <RUN_ID> --from-step <STEP_ID> --mode time-travel
```

Equivalent raw API call:

```bash
curl -i -X POST "http://127.0.0.1:8080/debug/run-replay" \
  -H "Content-Type: application/json" \
  -d '{"run_id":"<RUN_ID>","from_step":"<STEP_ID>","mode":"time_travel"}'
```

4. Cancel a run:

```bash
./driftqctl --base-url http://127.0.0.1:8080 runs cancel --run-id <RUN_ID> --reason "stopping"
```

## Run from a JSON spec

Endpoint:
- `POST /debug/run-spec`

Request body shape:

```json
{
  "run_id": "demo-1",
  "spec": {
    "id": "wf_demo",
    "nodes": [
      {"id":"A","topic":"sleep_500ms"},
      {"id":"B","topic":"noop","deps":["A"]}
    ]
  },
  "input": {"hello":"world"}
}
```

Example:

```bash
curl -i -X POST "http://127.0.0.1:8080/debug/run-spec" \
  -H "Content-Type: application/json" \
  -d '{"run_id":"demo-1","spec":{"id":"wf_demo","nodes":[{"id":"A","topic":"sleep_500ms"},{"id":"B","topic":"noop","deps":["A"]}]},"input":{"hello":"world"}}'
```

Response:

```json
{"ok":true,"run_id":"demo-1","trace_id":"..."}
```

Validation enforced by current runtime:
- `spec.id` required.
- `spec.nodes` must be non-empty.
- each node `id` must be non-empty and unique.
- `deps` references must point to existing node IDs.
- graph must be acyclic.
- each node must have `topic`.
- each `topic` must exist in handler registry.

Built-in handlers registered by `driftqd`:
- `noop`
- `sleep_500ms`
- `fail_once`
- `delay_once_2s`

## Inspecting runs

Summary:
- `GET /debug/run?run_id=<RUN_ID>`

Full state:
- `GET /debug/run-state?run_id=<RUN_ID>`

`/debug/run-state` includes:
- `run`
- `nodes`
- `events`
- `timers`

CLI equivalents:

```bash
./driftqctl --base-url http://127.0.0.1:8080 runs list --limit 20
./driftqctl --base-url http://127.0.0.1:8080 runs status --run-id <RUN_ID>
./driftqctl --base-url http://127.0.0.1:8080 runs state --run-id <RUN_ID>
./driftqctl --base-url http://127.0.0.1:8080 runs events --run-id <RUN_ID>
./driftqctl --base-url http://127.0.0.1:8080 runs step --run-id <RUN_ID> --node-id <NODE_ID>
./driftqctl --base-url http://127.0.0.1:8080 runs diff --run-id <RUN_ID> --node-id <NODE_ID>
```

## Artifacts

List artifacts by run:
- `GET /debug/run-artifacts?run_id=<RUN_ID>&limit=<N>`

CLI:

```bash
./driftqctl --base-url http://127.0.0.1:8080 runs artifacts --run-id <RUN_ID> --limit 50
```

Artifact metadata:
- `GET /debug/artifact-meta?artifact_id=<ARTIFACT_ID>`

Artifact bytes:
- `GET /debug/artifact-get?artifact_id=<ARTIFACT_ID>`

CLI helpers:

```bash
./driftqctl --base-url http://127.0.0.1:8080 runs artifact-meta --artifact-id <ARTIFACT_ID>
./driftqctl --base-url http://127.0.0.1:8080 runs artifact-get --artifact-id <ARTIFACT_ID> --out ./artifact.bin
```

If artifact store is not configured, artifact endpoints return `400` with text like `artifact store not configured`.

## Delay/timer behavior

When a handler returns a delay (`engine.Delay(...)`):
- node moves to waiting state
- timer is written into the run store (durable across restart when `--engine-store file` is used)
- run moves to `waiting`
- on timer fire, run is resumed by server loop

Timer fire + resume loop runs continuously in `driftqd`.

## Replay semantics

`POST /debug/run-replay` body:

```json
{
  "run_id": "<RUN_ID>",
  "from_step": "<STEP_ID>",
  "mode": "time_travel"
}
```

Modes:
- `time_travel`: reuse recorded successful outputs/artifacts where valid.
- `live`: force execution from selected step onward.

API allows `from_step` to be empty for full-run behavior.
Current CLI requires `--from-step`.

## Active index pointer (promote/rollback)

Get active version:
- `GET /debug/index/active`
- CLI: `driftqctl runs active-index`

Promote a succeeded run:
- `POST /debug/index/promote`
- body: `{"run_id":"<RUN_ID>","version":"<OPTIONAL_VERSION>"}`
- CLI: `driftqctl runs promote --run-id <RUN_ID> [--version <V>]`

Rollback pointer:
- `POST /debug/index/rollback`
- body: `{"version":"<VERSION>"}`
- CLI: `driftqctl runs rollback --to <VERSION>`

Promote requires run status `succeeded`.

## Metrics

- `GET /metrics` is Prometheus broker metrics (plus broker-side rejection/DLQ counters).
- `GET /debug/metrics` is JSON snapshot of internal engine runner metrics.

## Troubleshooting

- Demo run fails with `artifact store not configured`: start server with non-empty `--artifacts-dir` or Docker `-artifacts-dir`.
- Replay returns `run not found`: verify run ID exists with `runs list`.
- Spec run fails with `no handler registered for topic`: your node `topic` is not registered in server handler registry.
- PowerShell streaming behavior: use `curl.exe --no-buffer`.
