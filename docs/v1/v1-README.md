# DriftQ v1 HTTP API (stable broker) ✅

Base URL: `http://<host>:8080`

All API endpoints below are under `/v1/*` **except** `/metrics` (Prometheus) which is unversioned.

---

## Quickstart (local)

### Run the broker (Docker)
Pinned tag:
```bash
docker run --rm -p 8080:8080 -v driftq_data:/data ghcr.io/driftq-org/driftq-core:1.2.0
```

Health check:
```bash
curl http://127.0.0.1:8080/v1/healthz
```

**Windows PowerShell notes**
- Use `curl.exe` (PowerShell aliases `curl`).
- For streaming consume, use `curl.exe --no-buffer`.

### End‑to‑end “Hello World”
1) Create a topic:
```bash
curl -i -X POST "http://127.0.0.1:8080/v1/topics?name=demo&partitions=1"
```

2) Produce:
```bash
curl -i -X POST "http://127.0.0.1:8080/v1/produce?topic=demo&value=hello"
```

3) Consume (streaming NDJSON):
```bash
curl -N "http://127.0.0.1:8080/v1/consume?topic=demo&group=g1&owner=c1&lease_ms=5000"
```

4) Ack (use the `partition` + `offset` you saw in the consume output):
```bash
curl -i -X POST "http://127.0.0.1:8080/v1/ack?topic=demo&group=g1&owner=c1&partition=0&offset=0"
```

---

## Core concepts (read this once)

### Consumer groups + leases + owners
- A **group** is your consumer group identifier.
- An **owner** is your consumer instance identifier (think “consumer id”).
- `/v1/consume` grants a **lease** per delivered message.
- **Ack/Nack must come from the same owner** that holds the lease; otherwise you’ll get `409 Conflict`.

### Redelivery + retries + DLQ
- If you never `ack`, the message is redelivered when the lease expires.
- If a message has a retry policy and exceeds `max_attempts`, it is routed to `dlq.<topic>` (example: `dlq.demo`).
  - You must create that topic yourself if you want to consume it (it’s just another topic).

---

## Endpoints

### Health
**GET** `/v1/healthz`

Returns `200 OK` with JSON:
```json
{"status":"ok"}
```

---

### Version
**GET** `/v1/version`

Returns JSON:
```json
{"version":"dev","commit":"unknown","wal_enabled":true}
```

---

### Topics

#### List Topics
**GET** `/v1/topics`

Returns JSON:
```json
{"topics":["my-topic","another-topic"]}
```

#### Create Topic
**POST** `/v1/topics`

Accepts either query params or JSON body.

Query params:

| Param | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Topic name |
| `partitions` | No | Number of partitions (default: 1) |

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

Example:
```bash
curl -i -X POST "http://localhost:8080/v1/topics?name=t&partitions=1"
```

---

### Produce
**POST** `/v1/produce`

Accepts either JSON body or query params (useful for quick manual testing).

#### JSON body (recommended)
```json
{
  "topic": "my-topic",
  "key": "optional-key",
  "value": "message-content",
  "envelope": {
    "run_id": "run-123",
    "step_id": "step-456",
    "parent_step_id": "step-000",
    "tenant_id": "tenant-abc",
    "idempotency_key": "unique-key",
    "target_topic": "next-topic",
    "deadline": "2024-01-15T10:30:00Z",
    "partition_override": 2,
    "retry_policy": {
      "max_attempts": 3,
      "backoff_ms": 1000,
      "max_backoff_ms": 30000
    }
  }
}
```

#### Query params

| Param | Required | Description |
|-------|----------|-------------|
| `topic` | Yes | Target topic |
| `key` | No | Message key (for partitioning) |
| `value` | Yes | Message content |

Envelope params (all optional):

| Param | Alias | Description |
|-------|-------|-------------|
| `tenant_id` | `tenant` | Tenant identifier |
| `run_id` | | Workflow run ID |
| `step_id` | | Step identifier |
| `parent_step_id` | | Parent step ID |
| `idempotency_key` | `idem_key` | Deduplication key |
| `deadline_rfc3339` | | Deadline (RFC3339) |
| `deadline_ms` | | Deadline (Unix ms) |
| `target_topic` | | Target topic for routing |
| `partition_override` | | Force specific partition (int) |

Retry policy params (all optional):

| Param | Description |
|-------|-------------|
| `retry_max_attempts` | Max retry attempts (required if using backoff params) |
| `retry_backoff_ms` | Initial backoff in milliseconds |
| `retry_max_backoff_ms` | Maximum backoff in milliseconds |

Response:
```json
{"status":"ok","topic":"my-topic"}
```

Example:
```bash
curl -i -X POST "http://localhost:8080/v1/produce?topic=t&value=hello&retry_max_attempts=2"
```

Error responses:
- `400 Bad Request` — Invalid arguments
- `429 Too Many Requests` — Broker overloaded (includes `Retry-After` header)

```json
{
  "error": "RESOURCE_EXHAUSTED",
  "message": "producer overloaded",
  "reason": "overloaded",
  "retry_after_ms": 1000
}
```

---

### Consume (streaming)
**GET** `/v1/consume`

Consumes messages for a consumer group. This endpoint streams **NDJSON** (one JSON object per line) until the client disconnects.

Query params:

| Param | Required | Description |
|-------|----------|-------------|
| `topic` | Yes | Topic to consume from |
| `group` | Yes | Consumer group name |
| `owner` | Yes | Consumer instance identifier |
| `lease_ms` | No | Lease duration in milliseconds (default: 2000) |
| `partition` | No | Optional partition (int); if omitted, consumes all partitions |

JSON body:
```json
{
  "topic": "my-topic",
  "group": "my-group",
  "owner": "consumer-1",
  "lease_ms": 5000
}
```

Example:
```bash
curl -N "http://localhost:8080/v1/consume?topic=t&group=g&owner=o&lease_ms=5000"
```

Output format (NDJSON):
```json
{"partition":0,"offset":0,"attempts":1,"key":"k","value":"v","last_error":"","routing":{"label":"test","meta":{}},"envelope":{"run_id":"r1"}}
{"partition":0,"offset":1,"attempts":1,"key":"k2","value":"v2","last_error":""}
```

Important notes:
- If you never `ack`, messages will be redelivered after the lease expires.
- If a message has a retry policy and exceeds `max_attempts`, it is routed to `dlq.<topic>`.

---

### Ack
**POST** `/v1/ack`

Marks a message as successfully processed (advances the committed offset if possible).

Query params:

| Param | Required | Description |
|-------|----------|-------------|
| `topic` | Yes | Topic name |
| `group` | Yes | Consumer group |
| `owner` | Yes | Consumer instance identifier |
| `partition` | Yes | Partition number |
| `offset` | Yes | Message offset |

JSON body:
```json
{
  "topic": "my-topic",
  "group": "my-group",
  "owner": "consumer-1",
  "partition": 0,
  "offset": 5
}
```

Example:
```bash
curl -i -X POST "http://localhost:8080/v1/ack?topic=t&group=g&owner=o&partition=0&offset=0"
```

Responses:
- `204 No Content` — Success
- `400 Bad Request` — Invalid arguments
- `409 Conflict` — Caller is not the current owner

---

### Nack
**POST** `/v1/nack`

Marks a message as failed (may schedule retry depending on policy).

Query params:

| Param | Required | Description |
|-------|----------|-------------|
| `topic` | Yes | Topic name |
| `group` | Yes | Consumer group |
| `owner` | Yes | Consumer instance identifier |
| `partition` | Yes | Partition number |
| `offset` | Yes | Message offset |
| `reason` | No | Error reason (stored for debugging/metrics) |

JSON body:
```json
{
  "topic": "my-topic",
  "group": "my-group",
  "owner": "consumer-1",
  "partition": 0,
  "offset": 5,
  "reason": "processing failed: timeout"
}
```

Example:
```bash
curl -i -X POST "http://localhost:8080/v1/nack?topic=t&group=g&owner=o&partition=0&offset=0&reason=failed"
```

Responses:
- `204 No Content` — Success
- `400 Bad Request` — Invalid arguments
- `409 Conflict` — Caller is not the current owner
