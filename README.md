# DriftQ-Core 🚀

**DriftQ-Core** is a durable message broker (v1) that also contains the **DriftQ v2 foundations**: a replayable workflow runtime with a persistent run/event log, deterministic DAG scheduling, and debugging primitives.

- **v1 (stable):** broker API under `/v1/*` (produce/consume/ack/nack, topics, leases).
- **v2 foundations (evolving):** workflow runtime exposed via `/debug/*` and `driftqctl runs ...` (replay, timelines, diffs, rollback primitives).

If you only want the broker, you can ignore v2. If you want "Temporal-like" durability + replay, v2 is where this is going. 🙂

---

## Highlights ✨

### v1 — Broker (stable)
- Topics / partitions
- `produce`, streaming `consume` (NDJSON), `ack` / `nack`
- Consumer leases (`lease_ms`)
- Idempotency keys (at-least-once with dedupe)
- WAL-backed durability

### v2 foundations — Replayable workflow runtime (evolving)
- **Run contract** + **append-only run/event log** (inspectable execution history)
- **Deterministic DAG engine** (step dependencies, fan-out/fan-in, retries)
- **Replay controls**
  - **time-travel replay**: reuse recorded outputs/artifacts (don’t re-run expensive steps)
  - **live replay**: re-execute from a chosen step
- **Durable delay primitive** (timers + resume loop after restart)
- **Artifact store** + **replay cache** (store big outputs, reuse on replay)
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

---

## driftqctl (CLI) 🧰

From this repo:
```bash
go build -o driftqctl ./cmd/driftqctl
./driftqctl --help
```

Example (v1 broker):
```bash
./driftqctl topics list --base-url http://127.0.0.1:8080
```

Example (v2 foundations):
```bash
./driftqctl runs list --base-url http://127.0.0.1:8080
./driftqctl runs timeline --base-url http://127.0.0.1:8080 --run-id <RUN_ID>
./driftqctl runs replay --base-url http://127.0.0.1:8080 --run-id <RUN_ID> --from-step <STEP_ID> --mode time_travel
```

## Docs

- **v1 broker docs:** `docs/v1/README.md`
- **v2 foundations docs:** `docs/v2/README.md` (new)


## Starter templates & demos

For copy/paste starter repos and runnable demos, see **DriftQ-Starters**:

- GitHub: [driftq-org/DriftQ-Starters](https://github.com/driftq-org/DriftQ-Starters)


## API Surface

### v1 (stable)
All stable broker endpoints are under `/v1/*`.

Start here: `docs/v1/README.md`

### v2 foundations (debug / evolving)
These endpoints are currently under `/debug/*` and are meant for development, demos, and iteration (they will evolve).

Start here: `docs/v2/README.md`

---

## Development

Run from source:
```bash
go run ./cmd/driftqd -data-dir ./driftq_data
```

Run tests:
```bash
go test ./... -count=1
```

## Compatibility note (WAL) ⚠️

WAL is **forward-compatible only**: once you write WAL entries with newer ops, you can’t safely downgrade to an older binary that doesn’t understand them.


## Roadmap
- Message Queue MVP (Completed✅)
- Replayable Workflow Runtime (Completed✅)
- Multi-Agent Runtime & Real-Time AI
- DriftQ Cloud

## Repo layout (high level)

- `cmd/driftqd` — server
- `cmd/driftqctl` — CLI client
- `internal/broker` — v1 broker core
- `internal/engine` — v2 foundations (run store, DAG runner, replay, artifacts, timers, debug endpoints)
- `docs/` — documentation (v1 + v2)


## License

See `LICENSE`.
