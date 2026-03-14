# DriftQ-Core Architecture

This document describes the internal architecture of DriftQ-Core, a durable message broker (v1) with workflow runtime foundations (v2), v3 runtime guardrails, and OpenTelemetry-native observability.

## Table of Contents

1. [Overview](#overview)
2. [High-Level Architecture](#high-level-architecture)
3. [v1 Broker Architecture](#v1-broker-architecture)
4. [v2 Workflow Engine Architecture](#v2-workflow-engine-architecture)
5. [Storage Layer](#storage-layer)
6. [HTTP API Layer](#http-api-layer)
7. [Data Flow](#data-flow)
8. [Durability Model](#durability-model)
9. [Concurrency Model](#concurrency-model)
10. [Package Structure](#package-structure)
11. [Extension Points](#extension-points)


## Overview

DriftQ-Core is a single Go binary that provides two main subsystems:

| Subsystem | Status | Purpose |
|-----------|--------|---------|
| **v1 Broker** | Stable | Kafka-like message broker with topics, partitions, consumer groups |
| **v2 Engine / v3 Runtime** | Evolving | Temporal-like workflow runtime with DAG scheduling, replay, artifacts, guardrails, governance, HITL, and OTLP-friendly telemetry |

Both subsystems share core infrastructure (HTTP server, WAL storage, metrics, tracing) but operate independently. You can use v1 alone as a simple message queue, or combine both for durable workflow orchestration.


## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              driftqd (server)                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────────────────────┐   ┌──────────────────────────────┐    │
│  │        HTTP Layer            │   │        CLI (driftqctl)       │    │
│  │  /v1/* (broker)              │   │  topics list|create|peek     │    │
│  │  /debug/* (engine)           │   │  runs list|status|replay...  │    │
│  │  /metrics (prometheus)       │   │                              │    │
│  │  OTLP trace/metric export    │   │                              │    │
│  └──────────────┬───────────────┘   └──────────────────────────────┘    │
│                 │                                                       │
│  ┌──────────────▼───────────────┐   ┌──────────────────────────────┐    │
│  │       v1 Broker              │   │       v2 Engine              │    │
│  │  ┌─────────────────────┐     │   │  ┌─────────────────────┐     │    │
│  │  │   InMemoryBroker    │     │   │  │      Runner         │     │    │
│  │  │  - Topics/Partitions│     │   │  │  - DAG Scheduler    │     │    │
│  │  │  - Consumer Groups  │     │   │  │  - Handler Registry │     │    │
│  │  │  - Lease Management │     │   │  │  - Replay Cache     │     │    │
│  │  │  - Retry/DLQ        │     │   │  │  - Budget/Throttle  │     │    │
│  │  │  - Idempotency      │     │   │  │  - Timers           │     │    │
│  │  └─────────┬───────────┘     │   │  └─────────┬───────────┘     │    │
│  │            │                 │   │            │                 │    │
│  │  ┌─────────▼───────────┐     │   │  ┌─────────▼───────────┐     │    │
│  │  │   FileWAL           │     │   │  │   Store Interface   │     │    │
│  │  │   (broker.wal)      │     │   │  │  - MemoryStore      │     │    │
│  │  └─────────────────────┘     │   │  │  - FileStore        │     │    │
│  │                              │   │  └─────────────────────┘     │    │
│  └──────────────────────────────┘   │                              │    │
│                                     │  ┌─────────────────────┐     │    │
│                                     │  │   ArtifactStore     │     │    │
│                                     │  │  - MemoryStore      │     │    │
│                                     │  │  - LocalStore       │     │    │
│                                     │  └─────────────────────┘     │    │
│                                     └──────────────────────────────┘    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## v1 Broker Architecture

The v1 broker provides Kafka-like messaging semantics with built-in durability.

### Core Components

#### InMemoryBroker

The central broker implementation (`internal/broker/broker.go`):

```go
type InMemoryBroker struct {
    mu              sync.RWMutex
    topics          map[string]*TopicState
    consumerOffsets map[string]map[string]map[int]int64
    consumerChans   map[string]map[string][]consumerStream
    rrCursor        map[string]map[string]int
    inFlight        map[string]map[string]map[int]map[int64]*inflightEntry
    nextIndex       map[string]map[string]map[int]int
    lastDelivery    map[string]map[string]map[int]deliverySnapshot
    wal             storage.WAL
    router          Router
    idem            *IdempotencyStore
    retryState      map[string]map[string]map[int]map[int64]*retryStateEntry
    metrics         MetricsSink
    lag             *LagTracker
    pendingOffsets  map[offsetFlushKey]int64
}
```

#### Message Flow

```
Producer                          Broker                           Consumer
   │                                │                                   │
   │─── Produce(topic, msg) ───────▶│                                  │
   │                                │── WAL.Append() ─────────────────▶│
   │                                │── Router.Route() (optional) ────▶│
   │                                │── Partition Assignment ─────────▶│
   │                                │── Dispatch to Consumer ─────────▶│
   │                                │                                   │
   │                                │◀── ConsumeWithLease() ───────────│
   │                                │─── Message + Lease ─────────────▶│
   │                                │                                   │
   │                                │◀── Ack(partition, offset) ───────│
   │                                │── Commit Offset ────────────────▶│
   │                                │                                   │
   │                                │◀── Nack(partition, offset) ──────│
   │                                │── Schedule Retry ───────────────▶│
```

#### Key Features

| Feature | Implementation |
|---------|----------------|
| **Topics/Partitions** | `TopicState` with slice-per-partition |
| **Consumer Groups** | Round-robin dispatch across group members |
| **Lease-based Delivery** | `inflightEntry` with `SentAt`, `Owner`, `NextDeliverAt` |
| **Idempotency** | `IdempotencyStore` keyed by `(tenant, topic, group, key)` |
| **Retry Policy** | Exponential backoff via `RetryPolicy` |
| **Dead Letter Queue** | Auto-route to `dlq.<topic>` after max attempts |
| **Backpressure** | `maxPartitionMsgs` and `maxPartitionBytes` limits |
| **Routing** | Pluggable `Router` interface for message labeling/routing |

#### Redelivery Loop

A background goroutine (`internal/broker/redelivery.go`) handles:

1. Scanning in-flight messages with expired leases
2. Checking retry policy and attempt count
3. Either redelivering to consumers or routing to DLQ
4. Updating retry state in WAL


## v2 Workflow Engine Architecture

The v2 engine provides durable, replayable workflow execution with DAG scheduling.

### Core Components

#### Runner

The workflow execution engine (`internal/engine/runner.go`):

```go
type Runner struct {
    store           Store
    metrics         *EngineMetrics
    obs             *runtimeTelemetry
    logger          *slog.Logger
    graphs          map[string]WorkflowGraph
    registry        *HandlerRegistry
    tenantRegistries map[string]*HandlerRegistry
    artifacts       ArtifactStore
    artifactInlineLimit int
    cancels         map[string]context.CancelFunc
    policyBundle    *AuthorizationPolicyBundle
    riskPolicy      *RiskPolicy
    defaultRunBudget BudgetPolicy
    tenantBudgets   map[string]BudgetPolicy
    tenantRunCaps   map[string]int
    rateLimiter     RateLimiter
    topicCaps       map[string]int
    tenantTopicCaps map[string]map[string]int
    inflightCaps    map[string]int
    maxParallel     int
}
```

### Runtime Observability

`driftqd` initializes OpenTelemetry providers and HTTP trace propagation before the broker and engine are created. The engine then emits spans for:

- workflow runs
- node execution
- authorization checks
- risk evaluation
- governance / tenant checks
- human wait and resolution
- replay operations

The engine also emits matching OTel metrics for run outcomes, node outcomes, authz/risk/governance decisions, human tasks, replay activity, tool execution, artifact operations, and broker/topic activity. OTLP export complements the existing Prometheus `/metrics` endpoint and the structured logs that already include `trace_id`.

The current v3 observability scope also includes:

- nested tool spans inside node execution
- artifact put/get/delete spans and metrics
- broker/topic spans around the v1 API operations
- an OTel broker metrics sink that mirrors broker counters and hot-path timings into OTLP

#### Store Interface

```go
type Store interface {
    CreateRun(r Run) error
    UpdateRun(r Run) error
    GetRun(runID string) (Run, bool)

    UpsertNodeExecution(n NodeExecution) error
    GetNodeExecution(runID, nodeID string, attempt int) (NodeExecution, bool)
    ListNodeExecutions(runID string) []NodeExecution

    AppendEvent(e RunEvent) (RunEvent, error)
    ListEvents(runID string) []RunEvent

    UpsertTimer(t Timer) error
    GetTimer(runID, nodeID string, attempt int) (Timer, bool)
    ListTimers(runID string) []Timer
    ListDueTimers(now time.Time) []Timer

    ListRuns() []string
    PutKV(key, value string) error
    GetKV(key string) (string, bool)
}
```

#### DAG Execution

```
Workflow Spec                      DAG Scheduler                    Handlers
     │                                  │                               │
     │── Parse JSON Spec ──────────────▶│                               │
     │                                  │── Topological Sort ──────────▶│
     │                                  │── Find Ready Nodes ──────────▶│
     │                                  │                               │
     │                                  │── Execute Node A ────────────▶│
     │                                  │◀── Output A ──────────────────│
     │                                  │── AppendEvent(node_finished) ▶│
     │                                  │                               │
     │                                  │── Node B depends on A ───────▶│
     │                                  │── Build Input from A output ─▶│
     │                                  │── Execute Node B ────────────▶│
     │                                  │◀── Output B ──────────────────│
     │                                  │                               │
     │                                  │── All nodes done ────────────▶│
     │                                  │── run_finished event ────────▶│
```

#### Key Types

```go
// Run represents a workflow execution
type Run struct {
    RunID          string
    WorkflowID     string
    Status         RunStatus
    StartedAt      *time.Time
    EndedAt        *time.Time
    Spec           json.RawMessage
    InitialInput   json.RawMessage
    TenantID       string
    TerminalReason string
    TerminalMeta   json.RawMessage
    RunBudget      BudgetPolicy
    BudgetUsage    BudgetUsage
}

// NodeExecution tracks a single step attempt
type NodeExecution struct {
    RunID      string
    WorkflowID string
    NodeID     string
    Attempt    int
    Status     NodeStatus
    StartedAt  *time.Time
    EndedAt    *time.Time
    Input      json.RawMessage
    Output     json.RawMessage
    Error      string
}

// RunEvent is an append-only log entry
type RunEvent struct {
    RunID      string
    Seq        int64
    Type       RunEventType
    At         time.Time
    WorkflowID string
    NodeID     string
    Attempt    int
    Payload    json.RawMessage
}

// Timer for durable delays
type Timer struct {
    RunID      string
    WorkflowID string
    NodeID     string
    Attempt    int
    Status     TimerStatus
    FireAt     time.Time
    CreatedAt  time.Time
    FiredAt    *time.Time
    Reason     string
}
```

#### v3 runtime guardrails layered on top of the engine

The same runner now performs a layered preflight before execution:

1. authorization check
2. runtime risk evaluation
3. tenant governance / quota checks
4. optional human approval pause
5. workflow execution

Additional runtime concepts now carried by the engine:

- `Principal` for caller identity, roles, capabilities, and tenant scope
- authorization policy bundles for workflow/tool decisions
- risk policy + workflow risk reports
- tenant-scoped registries and tenant access checks
- audit records for policy/governance/HITL decisions
- human tasks for approval/review-edit steps

This lets the runner:

- deny unauthorized runs before creation
- sandbox or block risky runs
- stage manual approval before side effects
- isolate runs and artifacts by tenant
- resume waiting runs after timers or human responses

#### Replay Modes

| Mode | Behavior |
|------|----------|
| `time_travel` | Reuse recorded outputs from successful attempts |
| `live` | Force re-execution of steps |

Replay implementation (`internal/engine/replay.go`):

1. Load run state and spec from store
2. Build replay cache from previous node executions
3. Invalidate downstream nodes from `from_step`
4. Re-execute DAG with cache (time_travel) or without (live)


## Storage Layer

### Broker WAL

JSON-line append-only log (`internal/storage/wal.go`):

```go
type Entry struct {
    Type      RecordType  // Message|Offset|Topic|RetryState|ConsumeIdempotency
    Topic     string
    Partition int
    Offset    int64
    Key       []byte
    Value     []byte
    // ... envelope fields, routing, retry state, etc.
}
```

**Record Types:**

| Type | Purpose |
|------|---------|
| `RecordTypeMessage` | Produced message |
| `RecordTypeOffset` | Consumer offset commit |
| `RecordTypeTopic` | Topic metadata |
| `RecordTypeRetryState` | Last error for retry tracking |
| `RecordTypeConsumeIdempotency` | Idempotency state transitions |

**Durability:** Each entry is JSON-encoded, appended, and `fsync`'d before returning.

### Engine Store

Two implementations:

| Store | Use Case |
|-------|----------|
| `MemoryStore` | Development, testing (default) |
| `FileStore` | Production (WAL-backed) |

The FileStore appends run/event/timer operations to a separate WAL file (`driftq.engine.wal`).

### Artifact Store

For large step outputs that shouldn't be inlined in the event log:

| Store | Use Case |
|-------|----------|
| `MemoryArtifactStore` | Development, testing |
| `LocalArtifactStore` | Production (filesystem) |

Artifacts are stored as:
- `blobs/<sha256>` — raw content
- `meta/<artifact_id>.json` — metadata (run_id, node_id, attempt, sha256, etc.)


## HTTP API Layer

### Request Flow

```
Client Request
  -> request logging middleware
  -> OpenTelemetry HTTP middleware (extract trace context, start server span)
  -> root mux
     -> /v1/* broker handlers
     -> /debug/* engine handlers
     -> /debug/evals/* eval handlers
     -> /metrics Prometheus handler
  -> JSON response
```

Telemetry export happens out-of-process from `driftqd` to an OTLP collector. The server does not expose an OTLP ingest endpoint.

### Endpoint Organization

| Prefix | Handler Location | Purpose |
|--------|------------------|---------|
| `/v1/*` | `cmd/driftqd/main.go` | Stable broker API |
| `/debug/*` | `internal/engine/http_debug.go` | Evolving engine API |
| `/debug/evals/*` | `internal/engine/http_evals.go` | Eval datasets, suites, and runs |
| `/metrics` | `promhttp.Handler()` | Prometheus metrics |
| OTLP export setup | `internal/observability` | Outbound OpenTelemetry traces + metrics |



## Data Flow

### Message Production

```
1. Client POST /v1/produce
2. Parse JSON body or query params
3. Validate topic exists
4. Check backpressure (partition limits)
5. Check idempotency key (dedup)
6. Call Router.Route() if configured
7. Append to WAL
8. Add to in-memory partition
9. Dispatch to waiting consumers
10. Return success
```

### Workflow Execution

```
1. Client POST /debug/run-spec
2. Parse workflow spec JSON
3. Pick the tenant-scoped handler registry
4. Validate and compile the DAG into executable nodes
5. Run authorization preflight
6. Run risk evaluation and enforce allow / sandbox / approval / block
7. Enforce tenant governance and active-run quota checks
8. Create or resume the Run record and emit run lifecycle events
9. Schedule ready DAG nodes up to maxParallel
10. For each node attempt:
    a. Build input from dependency outputs
    b. Check budget and concurrency caps
    c. Handle workflow-native human steps when present
    d. Emit node_started
    e. Execute the handler or replay-cache short-circuit
    f. Store output inline or as an artifact
    g. Update NodeExecution
    h. Emit node_finished, node_failed, or retry/throttle/budget events
11. If the run enters waiting state, resume later via timer or human response
12. When terminal, update Run status/reason and emit run_finished
```


## Durability Model

### Broker Guarantees

| Guarantee | Mechanism |
|-----------|-----------|
| **Message durability** | WAL append + fsync before ack |
| **At-least-once delivery** | Lease-based redelivery on timeout |
| **Idempotency** | Dedup by `(tenant, topic, group, key)` |
| **Ordered delivery** | Per-partition FIFO within consumer group |

### Engine Guarantees

| Guarantee | Mechanism |
|-----------|-----------|
| **Run durability** | WAL-backed FileStore |
| **Event sourcing** | Append-only event log |
| **Replay correctness** | Stored spec + initial input |
| **Timer durability** | Timers in store + resume loop on restart |

### Recovery Flow

```
On startup:
1. Load broker WAL → rebuild topics, partitions, offsets, retry state
2. Load engine WAL → rebuild runs, events, timers
3. Start redelivery loop (broker)
4. Start timer resume loop (engine)
5. Begin accepting requests
```

## Concurrency Model

### Broker

- Single `sync.RWMutex` protects all broker state
- Consumer channels are goroutine-safe
- WAL writes are serialized (one writer)
- Redelivery runs in a dedicated goroutine

### Engine

- `sync.RWMutex` protects the workflow graph cache and policy state
- `RunWorkflow` executes ordered workflows sequentially on the caller goroutine
- `RunDAG` fans out ready nodes onto worker goroutines up to `maxParallel`
- Budget, throttle, and in-flight cap bookkeeping use separate mutex-protected maps
- Timer resume and broker redelivery each run in background goroutines started by `driftqd`

### Graceful Shutdown

```
1. Receive SIGINT/SIGTERM
2. Stop accepting new requests
3. Cancel app-level background loops (timer resume, redelivery context)
4. Gracefully shut down the HTTP server
5. Close the broker and flush pending WAL state
6. Close the engine store if it is file-backed
7. Shut down OTLP exporters
```



## Package Structure

```
DriftQ-Core/
|-- cmd/
|   |-- driftqd/                 # Server binary, HTTP setup, OTLP wiring
|   `-- driftqctl/               # CLI for runs, artifacts, topics, replay, diffs
|
|-- internal/
|   |-- broker/                  # v1 broker core, routing, redelivery, metrics hooks
|   |-- engine/                  # Workflow runtime, replay, guardrails, governance, HITL
|   |-- observability/           # OTel setup, HTTP middleware, broker/router wrappers
|   |-- multiagent/              # v3 agent routing config and router
|   |-- storage/                 # WAL implementation
|   |-- httpapi/v1/              # Broker HTTP request/response helpers
|   `-- debugtypes/              # Shared debug response types
|
|-- docs/
|   |-- v1/v1-README.md          # Broker API documentation
|   |-- v2/v2-README.md          # Engine API documentation
|   |-- v3/v3-README.md          # v3 runtime foundations
|   `-- architecture.md          # This file
|
`-- README.md                    # Project overview
```


## Extension Points

### Router Interface

Plug in custom message routing logic:

```go
type Router interface {
    Route(ctx context.Context, topic string, msg Message) (RoutingDecision, error)
}

type RoutingDecision struct {
    Label           string            // Classification label
    TargetTopic     string            // Override destination topic
    PartitionOverride *int            // Force specific partition
    Meta            map[string]string // Arbitrary metadata
}
```

### Handler Registry

Register custom step handlers:

```go
type NodeFunc func(ctx context.Context, input json.RawMessage) (json.RawMessage, error)

registry := engine.NewHandlerRegistry()
registry.Register("my_step", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
    // Process input, return output
    return json.RawMessage(`{"result": "done"}`), nil
})
```

### Metrics Sink

Inject custom broker metrics collection:

```go
type MetricsSink interface {
    IncProduceRejected(reason string)
    IncDLQ(topic, reason string)
    IncTopicCreated(topic string)
    IncAck(topic, group string)
    IncNack(topic, group, reason string)
    IncLeaseTimeout(topic, group string)
    IncRedelivery(topic, group, cause string)
}
```


### Store Implementations

Create custom storage backends by implementing:

```go
type Store interface {
    CreateRun(r Run) error
    UpdateRun(r Run) error
    GetRun(runID string) (Run, bool)
    // ... plus node executions, events, timers, listing, and KV methods
}
```


## Design Decisions

### Why JSON-line WAL?

- Human-readable for debugging
- Simple implementation
- Forward-compatible (new fields ignored by old code)
- Trade-off: larger than binary formats

### Why in-memory broker with WAL?

- Fast reads (no disk I/O on consume)
- Simple consistency model
- Full state rebuild on restart
- Trade-off: memory-bound, single-node

### Why append-only event log?

- Complete audit trail
- Enables time-travel replay
- Simplifies debugging
- Trade-off: storage growth (compaction not yet implemented)

### Why separate v1 and v2?

- v1 is stable, production-ready
- v2 is evolving, experimental
- Different stability guarantees
- Clear migration path when v2 stabilizes
