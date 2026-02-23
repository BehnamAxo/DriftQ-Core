# DriftQ v1 HTTP API (stable broker)

Base URL: `http://<host>:8080`

All stable broker endpoints are under `/v1/*` except `/metrics`, which is unversioned.

## Quickstart (local)

### Run the broker (Docker)
Pinned image tag:

```bash
docker run --rm -p 8080:8080 -v driftq_data:/data ghcr.io/driftq-org/driftq-core:1.2.0
```

Health check:

```bash
curl http://127.0.0.1:8080/v1/healthz
```

Expected:

```json
{"status":"ok"}
```

Windows PowerShell notes:
- Use `curl.exe` (PowerShell aliases `curl` to `Invoke-WebRequest`).
- For streaming consume, use `curl.exe --no-buffer`.

### End-to-end "Hello World" (create -> produce -> consume -> ack)

1. Create topic:

```bash
curl -i -X POST "http://127.0.0.1:8080/v1/topics?name=demo&partitions=1"
```

Expected status: `201 Created`

2. Produce one message:

```bash
curl -i -X POST "http://127.0.0.1:8080/v1/produce?topic=demo&value=hello"
```

Expected body:

```json
{"status":"produced","topic":"demo"}
```

3. Consume in a second terminal (streaming NDJSON):

macOS/Linux:

```bash
curl -N "http://127.0.0.1:8080/v1/consume?topic=demo&group=g1&owner=c1&lease_ms=5000"
```

Windows PowerShell:

```powershell
curl.exe --no-buffer "http://127.0.0.1:8080/v1/consume?topic=demo&group=g1&owner=c1&lease_ms=5000"
```

Sample line:

```json
{"partition":0,"offset":0,"attempts":1,"key":"","value":"hello","last_error":"","routing":{"label":"test-label","meta":{"source":"router"}}}
```

4. Ack using the same `owner` that consumed:

```bash
curl -i -X POST "http://127.0.0.1:8080/v1/ack?topic=demo&group=g1&owner=c1&partition=0&offset=0"
```

Expected status: `204 No Content`

## Retry, redelivery, and DLQ walkthrough

1. Produce with retry policy:

```bash
curl -i -X POST "http://127.0.0.1:8080/v1/produce?topic=demo&value=needs-retry&retry_max_attempts=3&retry_backoff_ms=500"
```

2. Consume it:

```bash
curl -N "http://127.0.0.1:8080/v1/consume?topic=demo&group=g1&owner=c1&lease_ms=2000"
```

3. Nack it from the same owner:

```bash
curl -i -X POST "http://127.0.0.1:8080/v1/nack?topic=demo&group=g1&owner=c1&partition=0&offset=<OFFSET>&reason=processing_failed"
```

4. Observe subsequent deliveries:
- `attempts` increases on each redelivery.
- `last_error` carries the latest failure context.

5. When max attempts is reached:
- Message is routed to `dlq.<topic>` (for `demo`, that is `dlq.demo`).
- The broker auto-creates the DLQ topic if it does not already exist.

6. Consume DLQ:

```bash
curl -N "http://127.0.0.1:8080/v1/consume?topic=dlq.demo&group=dlq&owner=dlq1&lease_ms=5000"
```

## Core behavior notes

- `topic`, `group`, and `owner` are required for `/v1/consume`.
- Ack/Nack are ownership-scoped. Wrong `owner` returns `409 Conflict` with error code `FAILED_PRECONDITION`.
- `/v1/consume` is an open stream. It does not return a finite JSON array.
- `value` is required and cannot be empty for `/v1/produce`.
- Consumer offsets are tracked per `(topic, group, partition)`, so groups consume independently.
- JSON request parsing is strict (`unknown field` is rejected with `400 INVALID_ARGUMENT`).

## Endpoint reference

### GET `/v1/healthz`

Response:

```json
{"status":"ok"}
```

### GET `/v1/version`

Response shape:

```json
{"version":"dev","commit":"unknown","wal_enabled":true}
```

### GET `/v1/topics`

Response:

```json
{"topics":["demo","orders","payments"]}
```

### POST `/v1/topics`

Create topic. Supports query params or JSON body.

Query params:
- `name` (required)
- `partitions` (optional, default `1`, must be `> 0`)

JSON body:

```json
{
  "name": "my-topic",
  "partitions": 4
}
```

Response:

```json
{"status":"created","name":"my-topic","partitions":4}
```

Status codes:
- `201 Created`
- `400 INVALID_ARGUMENT`

### POST `/v1/produce`

Produce a message. Supports JSON body or query params.

Query params:
- `topic` (required)
- `value` (required, non-empty)
- `key` (optional)

Optional query envelope fields:
- `run_id`
- `step_id`
- `parent_step_id`
- `tenant_id` (alias: `tenant`)
- `idempotency_key` (alias: `idem_key`)
- `target_topic`
- `partition_override`
- `deadline_rfc3339`
- `deadline_ms`

Optional query retry policy fields:
- `retry_max_attempts`
- `retry_backoff_ms`
- `retry_max_backoff_ms`

If `retry_backoff_ms` or `retry_max_backoff_ms` is set, `retry_max_attempts` must be `> 0`.

JSON body example:

```json
{
  "topic": "orders",
  "key": "order-123",
  "value": "created",
  "envelope": {
    "run_id": "run-123",
    "step_id": "step-1",
    "tenant_id": "tenant-a",
    "idempotency_key": "order-123-created",
    "target_topic": "orders.enriched",
    "deadline": "2026-02-23T12:00:00Z",
    "partition_override": 0,
    "retry_policy": {
      "max_attempts": 3,
      "backoff_ms": 500,
      "max_backoff_ms": 5000
    }
  }
}
```

Success response:

```json
{"status":"produced","topic":"orders"}
```

Overload response (`429 Too Many Requests`) includes `Retry-After` header:

```json
{
  "error": "RESOURCE_EXHAUSTED",
  "message": "producer overload: partition_buffer_full",
  "reason": "partition_buffer_full",
  "retry_after_ms": 1000
}
```

Possible `reason` values:
- `partition_buffer_full`
- `partition_buffer_bytes_full`

### GET `/v1/consume` (streaming NDJSON)

Streams one JSON object per line until client disconnects.

Query params:
- `topic` (required)
- `group` (required)
- `owner` (required)
- `lease_ms` (optional, default `2000`)

Example:

```bash
curl -N "http://127.0.0.1:8080/v1/consume?topic=orders&group=workers&owner=w1&lease_ms=5000"
```

Sample output lines:

```json
{"partition":0,"offset":0,"attempts":1,"key":"order-123","value":"created","last_error":""}
{"partition":0,"offset":1,"attempts":2,"key":"order-456","value":"created","last_error":"processing_failed"}
```

Notes:
- `attempts` starts at `1`.
- `routing` and `envelope` are optional fields and appear when present.
- There is no partition filter on `/v1/consume`.

### POST `/v1/ack`

Ack a leased message (must use the same `owner` that consumed it).

Required fields (query or JSON body):
- `topic`
- `group`
- `owner`
- `partition`
- `offset`

Query example:

```bash
curl -i -X POST "http://127.0.0.1:8080/v1/ack?topic=orders&group=workers&owner=w1&partition=0&offset=10"
```

Responses:
- `204 No Content` on success
- `400 INVALID_ARGUMENT`
- `409 FAILED_PRECONDITION` when owner does not match lease owner

### POST `/v1/nack`

Nack a leased message (must use the same `owner` that consumed it).

Required fields (query or JSON body):
- `topic`
- `group`
- `owner`
- `partition`
- `offset`

Optional:
- `reason` (defaults to `"nack"` when omitted/empty)

Query example:

```bash
curl -i -X POST "http://127.0.0.1:8080/v1/nack?topic=orders&group=workers&owner=w1&partition=0&offset=10&reason=timeout"
```

Responses:
- `204 No Content` on success
- `400 INVALID_ARGUMENT`
- `409 FAILED_PRECONDITION` when owner does not match lease owner

## Metrics

Prometheus scrape endpoint:

```bash
curl http://127.0.0.1:8080/metrics
```

Key metrics include:
- `inflight_messages{topic,group,partition}`
- `consumer_lag{topic,group,partition}`
- `produce_rejected_total{reason}`
- `dlq_messages_total{topic,reason}`
