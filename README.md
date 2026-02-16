# DriftQ-Core 🚀

**DriftQ-Core** is a durable message broker (v1) that also contains the **DriftQ v2 foundations**: a replayable workflow runtime with a persistent run/event log, deterministic DAG scheduling, and debugging primitives.

- **v1 (stable):** broker API under `/v1/*` (produce/consume/ack/nack, topics, leases).
- **v2 foundations (evolving):** workflow runtime exposed via `/debug/*` and `driftqctl runs ...` (replay, timelines, diffs, rollback primitives).

If you only want the broker, you can ignore v2. If you want "Temporal-like" durability + replay, v2 is where this is going. 🙂

---

## Highlights ✨

### v1 — Broker (stable)
- Topics / partitions (Kafka-style offsets)
- `produce`, streaming `consume` (NDJSON), `ack` / `nack`
- Consumer groups with round-robin dispatch
- Consumer leases (`lease_ms`) with automatic redelivery
- Idempotency keys (at-least-once with dedupe)
- Retry policies with exponential backoff
- Dead Letter Queue (DLQ) routing
- Backpressure via partition buffer limits
- WAL-backed durability
- Prometheus metrics

### v2 foundations — Replayable workflow runtime (evolving)
- **Run contract** + **append-only run/event log** (inspectable execution history)
- **Deterministic DAG engine** (step dependencies, fan-out/fan-in, retries)
- **Replay controls**
  - **time-travel replay**: reuse recorded outputs/artifacts (don't re-run expensive steps)
  - **live replay**: re-execute from a chosen step
- **Durable delay primitive** (timers + resume loop after restart)
- **Artifact store** + **replay cache** (store big outputs, reuse on replay)
- **Budget/throttle controls** (max attempts, tokens, dollars, wallclock timeout)
- **Debug endpoints** for inspection/control (run state, timelines, diffs, replay)
- **Minimal rollback primitive** via an "active index" pointer (promote/rollback)

---

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

> Tip: In production, pin the image tag (reproducible deploys). `latest` is for dev.

**Using docker-compose:**
```bash
docker-compose up -d
```

---

## driftqctl (CLI) 🧰

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

---

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

Full reference: `docs/v1/README.md`

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
| GET | `/debug/artifact-meta?run_id=ID&node_id=N` | Artifact metadata |
| GET | `/debug/artifact-get?run_id=ID&node_id=N` | Download artifact |
| POST | `/debug/run-demo` | Start demo workflow |
| GET | `/debug/index/active` | Get active index pointer |
| POST | `/debug/index/promote` | Promote index pointer |
| POST | `/debug/index/rollback` | Rollback index pointer |
| GET | `/debug/topics` | List topics (debug) |
| GET | `/debug/topics/peek` | Peek topic messages |
| GET | `/debug/topics/lag` | Consumer lag info |
| GET | `/debug/metrics` | Engine metrics |

Full reference: `docs/v2/README.md`

---

## Development

### Run from source

```bash
go run ./cmd/driftqd
```

**Server flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--addr` | `:8080` | HTTP listen address |
| `--wal` | `driftq.wal` | Path to broker WAL file |
| `--reset-wal` | `false` | Reset WAL (creates .bak file) |
| `--engine-store` | `memory` | Engine store: `memory` or `file` |
| `--engine-wal` | `driftq.engine.wal` | Path to engine WAL (when `--engine-store=file`) |
| `--artifacts-dir` | `driftq.artifacts` | Artifact store directory (empty = in-memory) |
| `--log-level` | `info` | Log level: `debug`, `info`, `warn`, `error` |
| `--log-format` | `text` | Log format: `text` or `json` |

**Examples:**

```bash
# Default (in-memory engine, file-based broker WAL)
go run ./cmd/driftqd

# Custom address and log level
go run ./cmd/driftqd --addr :9090 --log-level debug

# Durable engine with file-based storage
go run ./cmd/driftqd --engine-store file --engine-wal ./data/engine.wal --artifacts-dir ./data/artifacts

# Reset WAL on startup
go run ./cmd/driftqd --reset-wal
```

### Run tests

```bash
go test ./... -count=1
```

### Build binaries

```bash
go build -o driftqd ./cmd/driftqd
go build -o driftqctl ./cmd/driftqctl
```

---

## Compatibility note (WAL) ⚠️

WAL is **forward-compatible only**: once you write WAL entries with newer ops, you can't safely downgrade to an older binary that doesn't understand them.

---

## Roadmap

- Message Queue MVP (Completed ✅)
- Replayable Workflow Runtime (Completed ✅)
- Multi-Agent Runtime & Real-Time AI
- DriftQ Cloud

---

## Repo layout

```
DriftQ-Core/
├── cmd/
│   ├── driftqd/          # Server binary
│   └── driftqctl/        # CLI client
├── internal/
│   ├── broker/           # v1 broker core (dispatch, redelivery, idempotency, DLQ)
│   ├── engine/           # v2 workflow runtime (runner, DAG, replay, artifacts, timers)
│   ├── storage/          # WAL implementation
│   └── httpapi/          # HTTP types and helpers
├── docs/
│   ├── v1/               # Broker documentation
│   └── v2/               # Workflow runtime documentation
├── docker-compose.yml    # Local development
└── Dockerfile
```

---

## Starter templates & demos

For copy/paste starter repos and runnable demos, see **DriftQ-Starters**:

- GitHub: [driftq-org/DriftQ-Starters](https://github.com/driftq-org/DriftQ-Starters)

---

## Docs

- **v1 broker docs:** `docs/v1/README.md`
- **v2 foundations docs:** `docs/v2/README.md`

---

## License

See `LICENSE`.
