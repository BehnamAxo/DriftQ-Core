# DriftQ v1 HTTP API (MVP)

Base URL: `http://<host>:8080`

All API endpoints below are under `/v1/*` **except** `/metrics` (Prometheus) which is unversioned.


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

**Query params:**

| Param | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Topic name |
| `partitions` | No | Number of partitions (default: 1) |

**JSON body:**

```json
{
  "name": "my-topic",
  "partitions": 4
}
```

**Response:**

```json
{"status":"created","name":"my-topic","partitions":4}
```

**Example:**

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

**Envelope params (all optional):**

| Param | Alias | Description |
|-------|-------|-------------|
| `tenant_id` | `tenant` | Tenant identifier |
| `run_id` | | Workflow run ID |
| `step_id` | | Step identifier |
| `parent_step_id` | | Parent step ID |
| `idempotency_key` | `idem_key` | Deduplication key |
| `deadline_rfc3339` | | Deadline (RFC3339 format, e.g. `2024-01-15T10:30:00Z`) |
| `deadline_ms` | | Deadline (Unix milliseconds) |
| `target_topic` | | Target topic for routing |
| `partition_override` | | Force specific partition (int) |

**Retry policy params (all optional):**

| Param | Description |
|-------|-------------|
| `retry_max_attempts` | Max retry attempts (required if using backoff params) |
| `retry_backoff_ms` | Initial backoff in milliseconds |
| `retry_max_backoff_ms` | Maximum backoff in milliseconds |

**Response:**

```json
{"status":"ok","topic":"my-topic"}
```

**Example:**

```bash
curl -i -X POST "http://localhost:8080/v1/produce?topic=t&value=hello&retry_max_attempts=2"
```

**Error responses:**

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

Consumes messages for a consumer group. This endpoint **streams NDJSON**
(one JSON object per line) until the client disconnects.

Accepts either query params or JSON body.

#### Query params

| Param | Required | Description |
|-------|----------|-------------|
| `topic` | Yes | Topic to consume from |
| `group` | Yes | Consumer group name |
| `owner` | Yes | Consumer instance identifier |
| `lease_ms` | No | Lease duration in milliseconds (default: 2000) |

#### JSON body

```json
{
  "topic": "my-topic",
  "group": "my-group",
  "owner": "consumer-1",
  "lease_ms": 5000
}
```

**Example:**

```bash
curl -N "http://localhost:8080/v1/consume?topic=t&group=g&owner=o&lease_ms=5000"
```

**Output format (NDJSON):**

Each line is a `ConsumeItem` JSON object:

```json
{"partition":0,"offset":0,"attempts":1,"key":"k","value":"v","last_error":"","routing":{"label":"test","meta":{}},"envelope":{"run_id":"r1"}}
{"partition":0,"offset":1,"attempts":1,"key":"k2","value":"v2","last_error":""}
```

**Important notes:**

- If you never `ack`, messages will be redelivered after the lease expires.
- If a message has a retry policy and exceeds `max_attempts`, it is routed to `dlq.<topic>`.

---

### Ack

**POST** `/v1/ack`

Marks a message as successfully processed (advances the committed offset if possible).

Accepts either query params or JSON body.

#### Query params

| Param | Required | Description |
|-------|----------|-------------|
| `topic` | Yes | Topic name |
| `group` | Yes | Consumer group |
| `owner` | Yes | Consumer instance identifier |
| `partition` | Yes | Partition number |
| `offset` | Yes | Message offset |

#### JSON body

```json
{
  "topic": "my-topic",
  "group": "my-group",
  "owner": "consumer-1",
  "partition": 0,
  "offset": 5
}
```

**Example:**

```bash
curl -i -X POST "http://localhost:8080/v1/ack?topic=t&group=g&owner=o&partition=0&offset=0"
```

**Responses:**

- `204 No Content` — Success
- `400 Bad Request` — Invalid arguments
- `409 Conflict` — Caller is not the current owner

---

### Nack

**POST** `/v1/nack`

Marks a message as failed (may schedule retry depending on policy).

Accepts either query params or JSON body.

#### Query params

| Param | Required | Description |
|-------|----------|-------------|
| `topic` | Yes | Topic name |
| `group` | Yes | Consumer group |
| `owner` | Yes | Consumer instance identifier |
| `partition` | Yes | Partition number |
| `offset` | Yes | Message offset |
| `reason` | No | Error reason (stored for debugging/metrics) |

#### JSON body

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

**Example:**

```bash
curl -i -X POST "http://localhost:8080/v1/nack?topic=t&group=g&owner=o&partition=0&offset=0&reason=failed"
```

**Responses:**

- `204 No Content` — Success
- `400 Bad Request` — Invalid arguments
- `409 Conflict` — Caller is not the current owner

---

### Metrics (Prometheus)

**GET** `/metrics`

Exports Prometheus metrics:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `inflight_messages` | gauge | topic, group, partition | Messages currently being processed |
| `consumer_lag` | gauge | topic, group, partition | Number of unprocessed messages |
| `dlq_messages_total` | counter | topic, reason | Messages routed to DLQ |
| `produce_rejected_total` | counter | reason | Rejected produce requests |

**Example:**

```bash
curl -s "http://localhost:8080/metrics" | grep consumer_lag
```


## Error format

All errors are JSON with this structure:

```json
{"error":"ERROR_CODE","message":"Human readable description"}
```

Common error codes:

| Code | Description |
|------|-------------|
| `INVALID_ARGUMENT` | Bad request parameters |
| `FAILED_PRECONDITION` | Operation cannot be performed (e.g., not owner) |
| `RESOURCE_EXHAUSTED` | Rate limited / overloaded |
| `INTERNAL` | Server error |

---

## Method handling

Calling an endpoint with the wrong HTTP method returns `405 Method Not Allowed`
and includes an `Allow` header indicating the correct method.

---

## Idempotency

To ensure exactly-once delivery semantics, include an `idempotency_key` in the envelope:

```bash
curl -X POST "http://localhost:8080/v1/produce?topic=t&value=hello&idem_key=unique-123"
```

If a message with the same idempotency key has already been processed, the broker will deduplicate it.


## Dead Letter Queue (DLQ)

Messages that exceed `max_attempts` in their retry policy are automatically routed to `dlq.<topic>`.

For example, if a message on topic `orders` fails 3 times with `max_attempts=3`, it will be moved to `dlq.orders`.

DLQ messages preserve the original message content and include metadata about the failure:
- Original topic, partition, and offset
- Number of attempts
- Last error message
- Timestamp when routed to DLQ
