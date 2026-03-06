# DriftQ-Core 🚀

**DriftQ-Core** is a durable message broker (v1) that also contains the **DriftQ v2 foundations**: a replayable workflow runtime with a run/event log (persistent when `-engine-store file` is enabled), deterministic DAG scheduling, and debugging primitives.

- **v1 (stable):** broker API under `/v1/*` (produce/consume/ack/nack, topics, leases).
- **v2 foundations (evolving):** workflow runtime exposed via `/debug/*` and `driftqctl runs ...` (replay, timelines, diffs, rollback primitives).

If you only want the broker, you can ignore v2. If you want "Temporal-like" durability + replay, v2 is where this is going. 🙂

<a id="toc"></a>

## Table of contents

- [Why DriftQ-Core?](#why-driftq-core)
- [Highlights](#highlights-)
- [Quickstart (Docker)](#quickstart-docker)
  - [Do this next: broker "Hello World"](#broker-hello-world)
  - [Optional: v2 demo in 60 seconds](#v2-demo-60s)
  - [Common gotchas](#common-gotchas)
- [driftqctl (CLI)](#driftqctl-cli-)
- [API Surface](#api-surface)
- [Development](#development)
- [Compatibility note (WAL) ⚠️](#compatibility-note-wal-️)
- [Roadmap](#roadmap)
- [Repo layout](#repo-layout)
- [Starter templates & demos](#starter-templates--demos)
- [Docs](#docs)
- [License](#license)


<a id="why-driftq-core"></a>

## Why DriftQ-Core?

- **One binary, no external deps** (no Kafka/Temporal/DB cluster required)
- **Right-sized reliability**: leases, retries, DLQ, idempotency
- **Replay + debugging primitives** via v2 foundations

<details>
  <summary><strong>Read the full explanation</strong></summary>

### The problem

Building reliable backend systems today means choosing between two painful options:

**Option A: Managed infrastructure overkill.** You need a message queue, so you spin up Kafka (plus ZooKeeper or KRaft), or RabbitMQ (plus Erlang cluster management), or pay for SQS/Pub-Sub. You need durable workflows, so you add Temporal (plus a Cassandra or PostgreSQL cluster). Suddenly your "simple pipeline" requires 4+ services, a Kubernetes cluster, and a platform team to keep it all running. Most of the time, your actual workload is a few hundred messages per second — nowhere near justifying this complexity.

**Option B: Roll your own.** You wire up Redis lists, cron jobs, and Postgres-as-a-queue hacks. It works until it doesn't: messages get lost during deploys, retry logic is scattered across 15 files, debugging a failed pipeline means grepping through logs from three different services, and "replay that failed job from step 3" is a fantasy.

Neither option is great when you're a small team shipping fast, or when you're building AI agent pipelines where the real complexity is in the logic — not the plumbing.

### What DriftQ-Core does differently

**Single binary. Zero external dependencies.** DriftQ-Core is one Go binary that gives you both a Kafka-style message broker and a Temporal-style workflow runtime. No ZooKeeper. No etcd. No Redis. No separate database. You run one process, it writes to one WAL file, and you're done. `docker run` and you have durable messaging + workflow orchestration in seconds.

**Built for the workloads most teams actually have.** Not every project needs to process a million messages per second. Most teams need reliable delivery for a few hundred to a few thousand messages per second, with proper retries, dead-letter queues, and the ability to see what went wrong when something breaks. DriftQ-Core is built for exactly that sweet spot — where you need real durability guarantees without the operational tax of distributed infrastructure.

**Designed for AI and agent workflows from day one.** The v2 runtime isn't a generic workflow engine that happens to work for AI — it was built with AI pipelines in mind. Budget controls track tokens and dollars across a run so a runaway agent can't burn through your OpenAI bill. Concurrency throttles prevent "500 parallel embedding calls" accidents. Replay lets you re-run a pipeline from step 3 without re-calling the expensive LLM steps that already succeeded. Artifacts store large intermediate outputs (embeddings, generated documents) without bloating your event log.

**Debuggable by default.** Every run produces an append-only event log. Every step records its input, output, timing, and attempt number. You can inspect a failed run, diff two attempts of the same step, time-travel replay to reproduce issues, and see exactly where your budget was spent — all through the CLI or HTTP API. No more "what happened to that job?" mysteries.

### Who is this for?

- **Small teams building AI/LLM pipelines** who need durable execution without managing Temporal + Kafka + PostgreSQL
- **Backend developers** who want a lightweight message broker with proper retry semantics, DLQ routing, and consumer groups — without running a Kafka cluster
- **Solo developers and startups** who need production-grade messaging and workflow orchestration that runs on a single $5/month VPS
- **Anyone tired of gluing together 5 services** to get reliable message processing with retry and observability

### What DriftQ-Core is NOT

- It's not a distributed system (yet). It runs as a single process with file-based durability. If you need multi-node replication and horizontal scaling today, use Kafka + Temporal.
- It's not a general-purpose database. The WAL is append-only and optimized for message/event storage, not arbitrary queries.
- It's not trying to replace Kafka at 10 million messages per second. It's built for the 99% of workloads that don't need that scale.

</details>

<a id="highlights-"></a>

## Highlights

### v1 — Broker (stable)
- Topics / partitions (Kafka-style offsets)
- `produce`, streaming `consume` (NDJSON), `ack` / `nack`
- Consumer groups with round-robin dispatch
- Consumer leases (`lease_ms`) with automatic redelivery
- Idempotency keys (at-least-once with dedupe)
- Retry policies with exponential backoff
- Dead Letter Queue (DLQ) routing (with DLQ-of-DLQ prevention)
- Backpressure via configurable partition buffer limits
- Configurable broker limits (max partition bytes, max partition messages, max in-flight)
- WAL-backed durability
- Prometheus metrics

### v2 foundations — Replayable workflow runtime (evolving)
- **Run contract** + **append-only run/event log** (inspectable execution history)
- **Deterministic DAG engine** (step dependencies, fan-out/fan-in, retries)
  - Validates against duplicate/empty node IDs at spec parse time
- **Replay controls**
  - **time-travel replay**: reuse recorded outputs/artifacts (don't re-run expensive steps)
  - **live replay**: re-execute from a chosen step
- **Durable delay primitive** (timers + resume loop after restart)
- **Artifact store** + **replay cache** (store big outputs, reuse on replay)
- **Budget/throttle controls** (max attempts, tokens, dollars, wallclock timeout)
- **Debug endpoints** for inspection/control (run state, timelines, diffs, replay)
- **Minimal rollback primitive** via an "active index" pointer (promote/rollback)
- **Handler panic recovery** (panicking handlers do not crash the server)

### v3.1 foundation — Multi-Agent Messaging Layer
- Agent topic conventions:
  - `agent.{id}.inbox`
  - `agent.{id}.outbox`
  - `team.{id}.broadcast`
- Basic capability/role routing via in-memory registry (round-robin selection)
- Structured agent message contract (`sender`, `receiver|team|capability|role`, `intent`, `payload`)
- Startup config and topic bootstrap in `driftqd` via:
  - `-multiagent-config`
  - `-bootstrap-multiagent-topics`
- Runnable examples and smoke script:
  - `examples/multiagent/v3.1/*`
  - `scripts/multiagent_v31_smoke.sh`


<a id="quickstart-docker"></a>

## Quickstart (Docker)

**Recommended (pinned tag):**
```bash
docker run --rm -p 8080:8080 -v driftq_data:/data ghcr.io/driftq-org/driftq-core:1.2.0
```

**Development / tracks `main`:**
```bash
docker run --rm -p 8080:8080 -v driftq_data:/data ghcr.io/driftq-org/driftq-core:latest
```

Then hit:
```bash
curl http://127.0.0.1:8080/v1/healthz
```

Windows PowerShell:
```powershell
curl.exe http://127.0.0.1:8080/v1/healthz
```


> Tip: In production, pin the image tag (reproducible deploys). `latest` is for dev.

**Using docker-compose:**
```bash
docker-compose up -d
```

> **Important:** this repo’s `docker-compose.yml` references the image `driftq-core:local`. Build it once first:
> ```bash
> docker build -t driftq-core:local .
> ```
> Then run either `docker compose up -d` or `docker-compose up -d`.


**Docker with custom flags:**
```bash
docker run --rm -p 8080:8080 -v driftq_data:/data ghcr.io/driftq-org/driftq-core:1.2.0 \
  -addr :8080 \
  -wal /data/driftq.wal \
  -engine-store file \
  -engine-wal /data/engine.wal \
  -artifacts-dir /data/artifacts \
  -max-partition-bytes 8388608 \
  -max-inflight 4 \
  -log-level info \
  -log-format json
```

<a id="broker-hello-world"></a>

## Do this next: end-to-end broker "Hello World" (topics → produce → consume → ack)

This is the missing "what now?" after you see `{"status":"ok"}` from `/v1/healthz`.

> TL;DR: create a topic → produce → stream-consume → ack using the same `{topic, group, owner}`.

### 1) Create a topic

macOS/Linux:
```bash
curl -i -X POST "http://127.0.0.1:8080/v1/topics?name=demo&partitions=1"
```

Windows PowerShell (**use `curl.exe`** — PowerShell aliases `curl` to `Invoke-WebRequest`):
```powershell
curl.exe -i -X POST "http://127.0.0.1:8080/v1/topics?name=demo&partitions=1"
```

List topics:
```bash
curl http://127.0.0.1:8080/v1/topics
```

### 2) Produce a message

Basic:
```bash
curl -i -X POST "http://127.0.0.1:8080/v1/produce?topic=demo&value=hello"
```

Produce with retry policy (so you can observe retries/DLQ behavior later):
```bash
curl -i -X POST "http://127.0.0.1:8080/v1/produce?topic=demo&value=hello-retry&retry_max_attempts=3&retry_backoff_ms=500"
```

### 3) Consume as a streaming client (NDJSON)

Open a **second terminal**.

macOS/Linux (**important: `-N` disables output buffering**):
```bash
curl -N "http://127.0.0.1:8080/v1/consume?topic=demo&group=g1&owner=c1&lease_ms=5000"
```

Windows PowerShell (**important: `--no-buffer`**):
```powershell
curl.exe --no-buffer "http://127.0.0.1:8080/v1/consume?topic=demo&group=g1&owner=c1&lease_ms=5000"
```

You’ll see one JSON object per line, like:
```json
{"partition":0,"offset":0,"attempts":1,"key":"","value":"hello","last_error":""}
```

### 4) Ack the message you processed

Use the `partition` + `offset` from the consume line (example below uses 0/0):

```bash
curl -i -X POST "http://127.0.0.1:8080/v1/ack?topic=demo&group=g1&owner=c1&partition=0&offset=0"
```

**Important:** ack/nack must come from the same `owner` that consumed the message. If you use the wrong `owner`, you’ll get `409 Conflict`.

### 5) (Optional) Prove redelivery + retries + DLQ

- Consume a message **but do not ack it**.
- Wait for the lease to expire (e.g. `lease_ms=5000` → wait ~5 seconds).
- You should see it re-delivered with `attempts` incrementing.

If the message has a retry policy and exceeds `retry_max_attempts`, it routes to:
- `dlq.<original_topic>` (example: `dlq.demo`)

Create the DLQ topic and consume it:
```bash
curl -i -X POST "http://127.0.0.1:8080/v1/topics?name=dlq.demo&partitions=1"
curl -N "http://127.0.0.1:8080/v1/consume?topic=dlq.demo&group=dlq&owner=dlq1&lease_ms=5000"
```

### 6) Metrics (Prometheus)

```bash
curl http://127.0.0.1:8080/metrics
```

Look for metrics like:
- `inflight_messages{group,topic,partition}`
- `consumer_lag{group,topic,partition}`
- `dlq_messages_total{topic,reason}`
- `produce_rejected_total{reason}`


<a id="v2-demo-60s"></a>

## Optional: v2 demo in 60 seconds (replayable workflow foundations)

If you’re curious about the "v2 foundations", this is the fastest way to see them.

1) Build the CLI:
```bash
go build -o driftqctl ./cmd/driftqctl
```

Windows:
```powershell
go build -o driftqctl.exe ./cmd/driftqctl
```

2) Run the built-in demo workflow:
```bash
./driftqctl --base-url http://127.0.0.1:8080 runs demo
```

3) Inspect:
```bash
./driftqctl --base-url http://127.0.0.1:8080 runs list --limit 20
./driftqctl --base-url http://127.0.0.1:8080 runs timeline --run-id <RUN_ID>
./driftqctl --base-url http://127.0.0.1:8080 runs artifacts --run-id <RUN_ID>
```

> Note: the `/debug/run-replay` endpoint expects `"mode": "time_travel"` or `"live"` in JSON. `driftqctl runs replay --mode time-travel` maps to `"time_travel"` for you.


<a id="common-gotchas"></a>

## Common gotchas (first-time users)

- **PowerShell `curl` isn’t curl.** Use `curl.exe ...` (or use `Invoke-WebRequest` explicitly).
- **`/v1/consume` is a stream.** It stays open and prints one JSON line per message.
- **`topic`, `group`, and `owner` are required for `/v1/consume`.** (Owner matters because ack/nack are ownership-scoped.)
- **docker-compose requires a local image first.** This repo’s `docker-compose.yml` references `driftq-core:local`, so do:
  ```bash
  docker build -t driftq-core:local .
  docker compose up -d
  ```
  (If you prefer legacy syntax, `docker-compose up -d` still works.)
- **Reset / clean slate:**
  - Docker volume cleanup:
    ```bash
    docker volume rm driftq_data
    ```
  - From source: `go run ./cmd/driftqd --reset-wal`


<a id="driftqctl-cli-"></a>

## driftqctl (CLI)

Build from source:
```bash
go build -o driftqctl ./cmd/driftqctl
./driftqctl --help
```

**v1 broker examples:**
```bash
# List topics
./driftqctl --base-url http://127.0.0.1:8080 topics list

# Create a topic
./driftqctl --base-url http://127.0.0.1:8080 topics create --name my-topic --partitions 4

# Peek at messages
./driftqctl --base-url http://127.0.0.1:8080 topics peek --topic my-topic
```

**v2 foundations examples:**
```bash
# List runs
./driftqctl --base-url http://127.0.0.1:8080 runs list

# Get run status
./driftqctl --base-url http://127.0.0.1:8080 runs status --run-id <RUN_ID>

# View timeline
./driftqctl --base-url http://127.0.0.1:8080 runs timeline --run-id <RUN_ID>

# Time-travel replay (reuse recorded outputs)
./driftqctl --base-url http://127.0.0.1:8080 runs replay --run-id <RUN_ID> --from-step <STEP_ID> --mode time-travel

# Live replay (re-execute steps)
./driftqctl --base-url http://127.0.0.1:8080 runs replay --run-id <RUN_ID> --from-step <STEP_ID> --mode live

# Cancel a run
./driftqctl --base-url http://127.0.0.1:8080 runs cancel --run-id <RUN_ID> --reason "stopping"

# View artifacts
./driftqctl --base-url http://127.0.0.1:8080 runs artifacts --run-id <RUN_ID>

# Start a demo run
./driftqctl --base-url http://127.0.0.1:8080 runs demo
```

<a id="api-surface"></a>

## API Surface

### v1 (stable)

All stable broker endpoints are under `/v1/*`:

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/v1/healthz` | Health check |
| GET | `/v1/version` | Version info |
| GET | `/v1/topics` | List topics |
| POST | `/v1/topics?name=T&partitions=N` | Create topic |
| POST | `/v1/produce` | Produce message (JSON body or query params) |
| GET | `/v1/consume?topic=T&group=G` | Streaming consume (NDJSON) |
| POST | `/v1/ack` | Acknowledge message |
| POST | `/v1/nack` | Negative acknowledge (trigger retry) |
| GET | `/metrics` | Prometheus metrics |

Full reference: `docs/v1/v1-README.md`

> **Note:** `/v1/consume` requires **topic + group + owner** (example: `/v1/consume?topic=T&group=G&owner=O`). Ack/nack are scoped to that `owner`.


### v2 foundations (debug / evolving)

These endpoints are under `/debug/*` and are meant for development, demos, and iteration:

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/debug/run-spec` | Start a run from JSON spec |
| GET | `/debug/runs` | List all runs |
| GET | `/debug/run?run_id=ID` | Get run details |
| GET | `/debug/run-state?run_id=ID` | Get run state |
| POST | `/debug/run-replay` | Time-travel or live replay |
| POST | `/debug/run-cancel` | Cancel a run |
| GET | `/debug/run-artifacts?run_id=ID` | List run artifacts |
| GET | `/debug/artifact-meta?artifact_id=A` | Artifact metadata |
| GET | `/debug/artifact-get?artifact_id=A` | Download artifact |
| POST | `/debug/run-demo` | Start demo workflow |
| GET | `/debug/index/active` | Get active index pointer |
| POST | `/debug/index/promote` | Promote index pointer |
| POST | `/debug/index/rollback` | Rollback index pointer |
| GET | `/debug/topics` | List topics (debug) |
| GET | `/debug/topics/peek` | Peek topic messages |
| GET | `/debug/topics/lag` | Consumer lag info |
| GET | `/debug/metrics` | Engine metrics |

Full reference: `docs/v2/v2-README.md`


<a id="development"></a>

## Development

### Run from source

```bash
go run ./cmd/driftqd
```

### Dashboard UI (`/ui/`)

The dashboard is a React app served by the same `driftqd` HTTP server.

Build UI assets before running from source:

```bash
npm ci --prefix ui
npm run build --prefix ui
```

Then start DriftQ and open:

```text
http://localhost:8080/ui/
```

Notes:
- API/debug routes stay the same (`/v1/*`, `/debug/*`).
- Docker image builds the UI automatically in a Node build stage.
- `ui/dist` is generated locally/CI and is not committed to git.

**Server flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--addr` | `:8080` | HTTP listen address |
| `--wal` | `driftq.wal` | Path to broker WAL file |
| `--reset-wal` | `false` | Reset WAL by moving existing file aside (creates a `.bak.<ts>` file) |
| `--wal-sync-interval` | `0` | Broker WAL fsync interval (`0` = fsync every append; higher = lower latency with larger crash window) |
| `--wal-buffer-bytes` | `262144` | Broker WAL write buffer size in bytes |
| `--access-log` | `true` | Enable per-request HTTP access logging |
| `--engine-store` | `memory` | Engine store: `memory` or `file` |
| `--engine-wal` | `driftq.engine.wal` | Path to engine WAL (when `--engine-store=file`) |
| `--artifacts-dir` | `driftq.artifacts` | Artifact store directory (empty = in-memory) |
| `--log-level` | `info` | Log level: `debug`, `info`, `warn`, `error` |
| `--log-format` | `text` | Log format: `text` or `json` |
| `--max-partition-bytes` | `0` | Max bytes buffered per partition (`0` = broker default: 4 MB) |
| `--max-partition-msgs` | `0` | Max messages buffered per partition (`0` = broker default: 1024) |
| `--max-inflight` | `0` | Max in-flight messages per (topic, group, partition) (`0` = broker default: 32) |

**Broker defaults** (when flags are `0` or omitted):

| Limit | Default | Description |
|-------|---------|-------------|
| Max partition bytes | 4 MB | Per-partition byte buffer limit (backpressure) |
| Max partition messages | 1024 | Per-partition message count limit (backpressure) |
| Max in-flight | 32 | Max unacknowledged messages per (topic, group, partition) |

**Examples:**

```bash
# Default (in-memory engine, file-based broker WAL)
go run ./cmd/driftqd

# Custom address and log level
go run ./cmd/driftqd --addr :9090 --log-level debug

# JSON structured logging (for production / log aggregation)
go run ./cmd/driftqd --log-format json

# Durable engine with file-based storage
go run ./cmd/driftqd --engine-store file --engine-wal ./data/engine.wal --artifacts-dir ./data/artifacts

# Tune WAL latency/throughput behavior and disable access logs
go run ./cmd/driftqd --wal-sync-interval 20ms --wal-buffer-bytes 1048576 --access-log=false

# Tune broker backpressure limits
go run ./cmd/driftqd --max-partition-bytes 8388608 --max-partition-msgs 500 --max-inflight 4

# Reset WAL on startup (safe: moves old WAL to .bak)
go run ./cmd/driftqd --reset-wal
```

### Run tests

**Unit tests:**
```bash
npm ci --prefix ui
npm run build --prefix ui
go test ./... -count=1
```

**Integration tests** (requires the `integration` build tag):
```bash
# Basic integration test run
go test -tags=integration ./... -count=1

# Multiple iterations to catch flaky behavior
go test -tags=integration ./... -count=5

# Race detection + shuffled test ordering (recommended for CI)
go test -tags=integration ./... -race -count=1 -shuffle=on

# Stress test: race + shuffle + multiple iterations
go test -tags=integration ./... -race -count=5 -shuffle=on
```

**Using gotestsum** (nicer output, recommended):
```bash
go run gotest.tools/gotestsum@latest --format pkgname -- -count=1 -tags=integration ./...
```

### Load testing

A load test script is included at `scripts/loadtest.sh`. It uses [hey](https://github.com/rakyll/hey) to measure produce latency/throughput, burst handling, and sustained load:

Start the server first (any OS):

```bash
go run ./cmd/driftqd -addr 127.0.0.1:8080
```

Linux/macOS:

```bash
# Run load tests (defaults: 100 req/s for 60s)
BASE_URL=http://127.0.0.1:8080 ./scripts/loadtest.sh

# Custom rate and duration
BASE_URL=http://127.0.0.1:8080 RATE=500 DURATION=120 ./scripts/loadtest.sh

# Against a remote server
BASE_URL=http://remote:8080 ./scripts/loadtest.sh
```

Burst tuning:

```bash
# Default burst: 200 concurrent, 5000 requests
BASE_URL=http://127.0.0.1:8080 ./scripts/loadtest.sh

# Customize burst
BASE_URL=http://127.0.0.1:8080 BURST_C=300 BURST_N=8000 ./scripts/loadtest.sh

# Optional extreme burst (500 concurrent)
BASE_URL=http://127.0.0.1:8080 EXTREME_BURST=1 ./scripts/loadtest.sh
```

Windows (PowerShell + Git Bash):

```powershell
# Resolve Git Bash path (works for Program Files and Program Files (x86))
$gitBash = Join-Path $env:ProgramFiles "Git\\bin\\bash.exe"
if (-not (Test-Path $gitBash) -and ${env:ProgramFiles(x86)}) {
  $gitBash = Join-Path ${env:ProgramFiles(x86)} "Git\\bin\\bash.exe"
}
if (-not (Test-Path $gitBash)) {
  throw "Git Bash not found. Install Git for Windows or update this path."
}

# Run load tests (defaults: 100 req/s for 60s)
& $gitBash -lc "BASE_URL=http://127.0.0.1:8080 ./scripts/loadtest.sh"

# Custom rate and duration
& $gitBash -lc "BASE_URL=http://127.0.0.1:8080 RATE=500 DURATION=120 ./scripts/loadtest.sh"

# Customize burst / optional extreme mode
& $gitBash -lc "BASE_URL=http://127.0.0.1:8080 BURST_C=300 BURST_N=8000 ./scripts/loadtest.sh"
& $gitBash -lc "BASE_URL=http://127.0.0.1:8080 EXTREME_BURST=1 ./scripts/loadtest.sh"
```

Windows (WSL shell):

```bash
# From repo root inside WSL
BASE_URL=http://127.0.0.1:8080 ./scripts/loadtest.sh
```

The load test covers:
- Produce endpoint latency under a fixed request count (throughput-ish smoke test)
- Burst test (defaults to **200 concurrent / 5000 requests**, cross-platform)
  - Optional extreme burst (**500 concurrent**) by setting `EXTREME_BURST=1`
- Sustained load (50 req/s for 30s)
- Health check throughput
- Concurrent workflow demo execution + completion polling

### v3.1 multi-agent smoke test

Use this to validate direct, capability, and broadcast routing end-to-end with `/v1/*` APIs.

Start server with the example config:

```bash
go run ./cmd/driftqd -addr 127.0.0.1:8080 -reset-wal \
  -multiagent-config ./examples/multiagent/v3.1/multiagent.json \
  -bootstrap-multiagent-topics
```

Linux/macOS:

```bash
BASE_URL=http://127.0.0.1:8080 ./scripts/multiagent_v31_smoke.sh
```

Windows (PowerShell + Git Bash):

```powershell
$gitBash = Join-Path $env:ProgramFiles "Git\\bin\\bash.exe"
if (-not (Test-Path $gitBash) -and ${env:ProgramFiles(x86)}) {
  $gitBash = Join-Path ${env:ProgramFiles(x86)} "Git\\bin\\bash.exe"
}

# If python3 is not on PATH but Python launcher (py) is available:
& $gitBash -lc "python3(){ py -3 \"\$@\"; }; export -f python3; cd /c/Workspace/DriftQ-Core && BASE_URL=http://127.0.0.1:8080 ./scripts/multiagent_v31_smoke.sh"
```

### Build binaries

```bash
go build -o driftqd ./cmd/driftqd
go build -o driftqctl ./cmd/driftqctl
```

**Build with version info:**
```bash
go build -ldflags "-X main.buildVersion=1.2.0 -X main.buildCommit=$(git rev-parse --short HEAD)" -o driftqd ./cmd/driftqd
```

<a id="compatibility-note-wal-️"></a>

## Compatibility note (WAL) ⚠️

WAL is **forward-compatible only**: once you write WAL entries with newer ops, you can't safely downgrade to an older binary that doesn't understand them.


<a id="roadmap"></a>

## Roadmap

- Message Queue MVP (Completed ✅)
- Replayable Workflow Runtime (Completed ✅)
- Multi-Agent Runtime & Real-Time AI
- DriftQ Cloud


<a id="repo-layout"></a>

## Repo layout

```
DriftQ-Core/
|-- cmd/
|   |-- driftqd/          # Server binary
|   `-- driftqctl/        # CLI client
|-- internal/
|   |-- broker/           # v1 broker core (dispatch, redelivery, idempotency, DLQ)
|   |-- engine/           # v2 workflow runtime (runner, DAG, replay, artifacts, timers)
|   |-- multiagent/       # v3.1 foundational multi-agent message contract + validation
|   |-- storage/          # WAL implementation
|   `-- httpapi/          # HTTP types and helpers
|-- ui/                   # React dashboard source + embedded static build
|   |-- src/
|   |-- dist/             # Generated by `npm run build` and embedded by Go
|   `-- embed.go
|-- scripts/
|   `-- loadtest.sh       # Load testing script (uses hey)
|-- docs/
|   |-- v1/               # Broker documentation
|   |-- v2/               # Workflow runtime documentation
|   `-- v3/               # Multi-agent runtime docs (foundational)
|-- docker-compose.yml    # Local development
|-- Dockerfile            # Multi-stage distroless build
`-- Makefile
```

<a id="starter-templates--demos"></a>

## Starter templates & demos

For copy/paste starter repos and runnable demos, see **DriftQ-Starters**:

- GitHub: [driftq-org/DriftQ-Starters](https://github.com/driftq-org/DriftQ-Starters)


<a id="docs"></a>

## Docs

- **v1 broker docs:** `docs/v1/v1-README.md`
- **v2 foundations docs:** `docs/v2/v2-README.md`
- **v3.1 messaging foundation docs:** `docs/v3/v3.1-README.md`


<a id="license"></a>

## License

See `LICENSE`.
