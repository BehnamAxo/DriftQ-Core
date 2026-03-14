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
    topics          map[string]*TopicState      // topic -> partitions
    consumerOffsets map[string]map[string]map[int]int64  // topic -> group -> partition -> offset
    consumerChans   map[string]map[string][]consumerStream
    inFlight        map[string]map[string]map[int]map[int64]*inflightEntry
    wal             storage.WAL
    router          Router
    idem            *IdempotencyStore
    retryState      map[string]map[string]map[int]map[int64]*retryStateEntry
    lag             *LagTracker
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
    store           Store               // Durable state
    obs             *runtimeTelemetry   // OTel spans + metrics
    graphs          map[string]WorkflowGraph  // Cached workflow definitions
    registry        *HandlerRegistry    // Step handlers
    tenantRegistries map[string]*HandlerRegistry
    artifacts       ArtifactStore       // Large output storage
    cancels         map[string]context.CancelFunc
    policyBundle    *AuthorizationPolicyBundle
    riskPolicy      *RiskPolicy
    defaultRunBudget BudgetPolicy
    tenantBudgets   map[string]BudgetPolicy
    tenantRunCaps   map[string]int
    rateLimiter     RateLimiter
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

The engine also emits matching OTel metrics for run outcomes, node outcomes, authz/risk/governance decisions, human tasks, and replay activity. OTLP export complements the existing Prometheus `/metrics` endpoint and the structured logs that already include `trace_id`.

Phase 2 extends that foundation with:

- nested tool spans inside node execution
- artifact put/get/delete spans and metrics
- broker/topic spans around the v1 API operations
- an OTel broker metrics sink that mirrors broker counters and hot-path timings into OTLP

#### Store Interface

```go
type Store interface {
    // Runs
    CreateRun(r Run) error
    UpdateRun(r Run) error
    GetRun(runID string) (Run, bool)
    ListRuns() []string

    // Node Executions
    UpsertNodeExecution(n NodeExecution) error
    GetNodeExecution(runID, nodeID string, attempt int) (NodeExecution, bool)
    ListNodeExecutions(runID string) []NodeExecution

    // Event Log
    AppendEvent(e RunEvent) (RunEvent, error)
    ListEvents(runID string) []RunEvent

    // Timers
    UpsertTimer(t Timer) error
    ListDueTimers(now time.Time) []Timer

    // KV (for index pointer, etc.)
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
    RunID        string          // Unique identifier
    WorkflowID   string          // Workflow definition ID
    Status       RunStatus       // queued|running|waiting|succeeded|failed|canceled
    Spec         json.RawMessage // Stored workflow spec (for replay)
    InitialInput json.RawMessage // Original input (for replay)
    TenantID     string          // Tenant ownership / isolation scope
    RunBudget    BudgetPolicy    // Resource limits
    BudgetUsage  BudgetUsage     // Current usage
}

// NodeExecution tracks a single step attempt
type NodeExecution struct {
    RunID      string
    NodeID     string
    Attempt    int             // 1, 2, 3... (increments on retry)
    Status     NodeStatus
    Input      json.RawMessage
    Output     json.RawMessage
    Error      string
}

// RunEvent is an append-only log entry
type RunEvent struct {
    RunID   string
    Seq     int64           // Monotonic sequence number
    Type    RunEventType    // node_started, node_finished, etc.
    Payload json.RawMessage // Event-specific data
}

// Timer for durable delays
type Timer struct {
    RunID   string
    NodeID  string
    Attempt int
    Status  TimerStatus     // scheduled|fired|canceled
    FireAt  time.Time
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
      │
      ▼
┌─────────────────┐
│  Request Logger │  (trace_id, req_id, method, path, duration)
│  OTel Middleware│  (traceparent extract, server span, OTLP export)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Mux Router    │
│  /v1/* → broker │
│  /debug/* → eng │
│  /metrics → prom│
│  OTLP → collector│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Handler        │
│  - Parse input  │
│  - Call service │
│  - Write JSON   │
└─────────────────┘
```

### Endpoint Organization

| Prefix | Handler Location | Purpose |
|--------|------------------|---------|
| `/v1/*` | `cmd/driftqd/main.go` | Stable broker API |
| `/debug/*` | `internal/engine/http_debug.go` | Evolving engine API |
| `/metrics` | `promhttp.Handler()` | Prometheus metrics |
| OTLP/HTTP exporter | `internal/observability` | OpenTelemetry traces + metrics |


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
3. Validate DAG (no cycles, valid edges)
4. Create Run record (status=queued)
5. Emit run_created event
6. Topological sort nodes
7. For each ready node:
   a. Build input from dependencies
   b. Check throttle/budget limits
   c. Create NodeExecution (status=running)
   d. Emit node_started event
   e. Execute handler
   f. Store output (inline or artifact)
   g. Update NodeExecution (status=succeeded|failed)
   h. Emit node_finished|node_failed event
8. When all nodes done:
   a. Update Run (status=succeeded|failed)
   b. Emit run_finished event
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

- `sync.RWMutex` protects workflow graphs cache
- Each run executes in its own goroutine (via `RunWorkflow`)
- Node executions are sequential within a run (current implementation)
- Throttle tracking uses a separate mutex

### Graceful Shutdown

```
1. Receive SIGINT/SIGTERM
2. Stop accepting new requests
3. Cancel in-progress runs (with context)
4. Drain consumer channels
5. Flush WAL
6. Exit
```


## Package Structure

```
DriftQ-Core/
├── cmd/
│   ├── driftqd/               # Server binary
│   │   ├── main.go            # Entry point, HTTP setup, flag parsing
│   │   └── broker_collector.go # Prometheus metrics collector
│   └── driftqctl/             # CLI client
│       ├── main.go            # Entry point, subcommand dispatch
│       ├── runs.go            # runs subcommands
│       ├── runs_artifacts.go  # artifact commands
│       └── topics_lag.go      # lag inspection
│
├── internal/
│   ├── broker/                # v1 broker core
│   │   ├── broker.go          # InMemoryBroker implementation
│   │   ├── types.go           # Message, Envelope, Broker interface
│   │   ├── redelivery.go      # Lease expiry + retry logic
│   │   ├── idempotency.go     # Deduplication store
│   │   ├── dlq.go             # Dead letter queue routing
│   │   ├── lag.go             # Consumer lag tracking
│   │   └── *_test.go          # Unit tests
│   │
│   ├── engine/                # v2 workflow engine
│   │   ├── runner.go          # Runner (main executor)
│   │   ├── types.go           # Run, NodeExecution, RunEvent, Timer
│   │   ├── dag.go             # WorkflowGraph, topological sort
│   │   ├── spec.go            # WorkflowSpec, NodeSpec
│   │   ├── spec_parse.go      # JSON spec parsing
│   │   ├── replay.go          # Replay implementation
│   │   ├── replay_cache.go    # Output caching for time-travel
│   │   ├── cancel.go          # Run cancellation
│   │   ├── timer.go           # Durable timer primitives
│   │   ├── timer_resume.go    # Timer resume on restart
│   │   ├── budget.go          # Budget/throttle policies
│   │   ├── rate_limiter.go    # Rate limiting
│   │   ├── artifact_store.go  # Artifact storage interface
│   │   ├── artifact_store_*.go # Memory/file implementations
│   │   ├── memstore.go        # In-memory Store implementation
│   │   ├── store_file.go      # WAL-backed Store implementation
│   │   ├── http_debug.go      # Debug HTTP handlers
│   │   ├── index_meta.go      # Index pointer promote/rollback
│   │   └── *_test.go          # Unit tests
│   │
│   ├── storage/               # WAL implementation
│   │   └── wal.go             # FileWAL with JSON entries
│   │
│   ├── httpapi/
│   │   └── v1/
│   │       ├── types.go       # Request/response types
│   │       └── helpers.go     # JSON writing utilities
│   │
│   └── debugtypes/            # Shared debug types
│       └── lag.go             # ConsumerLagRow
│
├── docs/
│   ├── v1/README.md           # Broker API documentation
│   ├── v2/README.md           # Engine API documentation
│   └── architecture.md        # This file
│
├── Dockerfile                 # Container build
├── docker-compose.yml         # Local development
├── go.mod                     # Dependencies
└── README.md                  # Project overview
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
type Handler func(ctx context.Context, input json.RawMessage) (json.RawMessage, error)

registry := engine.NewHandlerRegistry()
registry.Register("my_step", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
    // Process input, return output
    return json.RawMessage(`{"result": "done"}`), nil
})
```

### Metrics Sink

Inject custom metrics collection:

```go
type MetricsSink interface {
    IncProduceRejected(reason string)
    IncDLQTotal(topic, reason string)
}
```

### Store Implementations

Create custom storage backends by implementing:

```go
type Store interface {
    CreateRun(r Run) error
    UpdateRun(r Run) error
    GetRun(runID string) (Run, bool)
    // ... full interface in internal/engine/memstore.go
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
