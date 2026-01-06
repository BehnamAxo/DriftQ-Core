# DriftQ-Core

DriftQ-Core is the **v1 MVP** of DriftQ: a small, WAL-backed message broker built in Go.

It’s intentionally minimal: the goal is a clean foundation for higher-level features (routing/policies, schedulers, etc.), with a simple HTTP API you can hit with `curl`.

## What you get (MVP)

- Topics + partitions
- Producer API with basic envelope fields (tenant/run/step/idempotency key, retry policy)
- Consumer groups with **owner + lease** semantics
- `/ack` and `/nack`
- Retry + **DLQ** (`dlq.<topic>`)
- Prometheus `/metrics` (basic observability)

## Run with Docker (recommended)

This is the easiest way to run DriftQ-Core without installing Go.

### Build the image locally

**mac/linux**
```bash
docker build -t driftq-core:local \
  --build-arg VERSION=dev \
  --build-arg COMMIT="$(git rev-parse --short HEAD)" \
  .
```

**windows powershell**
```powershell
docker build -t driftq-core:local `
  --build-arg VERSION=dev `
  --build-arg COMMIT=$(git rev-parse --short HEAD) `
  .
```

### Run with Docker Compose (WAL persists)

```bash
docker compose up
```

- DriftQ listens on `http://localhost:8080`
- WAL is stored in a named Docker volume mounted at `/data` inside the container.

Stop it:
```bash
docker compose down
```

⚠️ If you want to **wipe data / reset WAL**, remove the volume:
```bash
docker compose down -v
```

## Run locally (Go)

```bash
go run ./cmd/driftqd -log-format=text -log-level=info -reset-wal
```

By default DriftQ listens on `:8080`. You can change it:

```bash
go run ./cmd/driftqd -addr :9090 -log-format=text -log-level=info
```

## Quickstart (HTTP)

Create a topic:

```bash
curl -i -X POST "http://localhost:8080/v1/topics?name=t&partitions=1"
```

Produce a message:

```bash
curl -i -X POST "http://localhost:8080/v1/produce?topic=t&value=hello"
```

Consume (streams **NDJSON**, one JSON object per line):

```bash
# mac/linux
curl -N "http://localhost:8080/v1/consume?topic=t&group=g&owner=o&lease_ms=5000"

# windows powershell
curl.exe --no-buffer "http://localhost:8080/v1/consume?topic=t&group=g&owner=o&lease_ms=5000"
```

Ack:

```bash
curl -i -X POST "http://localhost:8080/v1/ack?topic=t&group=g&owner=o&partition=0&offset=0"
```

Nack:

```bash
curl -i -X POST "http://localhost:8080/v1/nack?topic=t&group=g&owner=o&partition=0&offset=0&error=failed"
```

## Observability

### Metrics endpoint

```bash
curl -s "http://localhost:8080/metrics" | head
```

### Metrics currently exported

- `inflight_messages{topic,group,partition}` (gauge)
- `consumer_lag{topic,group,partition}` (gauge)
- `dlq_messages_total{topic,reason}` (counter)
- `produce_rejected_total{reason}` (counter)

Example:

```bash
curl -s "http://localhost:8080/metrics" | findstr inflight_messages
curl -s "http://localhost:8080/metrics" | findstr consumer_lag
```

## Docs

- HTTP API reference: `docs/v1/README.md`
- Architecture notes: `docs/architecture.md`

## Tests

```bash
go test ./...
```
