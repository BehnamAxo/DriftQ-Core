# DriftQ-Core - Architecture

> This document explains the architecture of DriftQ-Core in a visual, diagram-first way.
> It is meant to help readers build a mental model quickly, even on a first read.

---

## 0. TL;DR — One Sentence

> **DriftQ-Core is a single-process durable broker + a replayable workflow runtime, with v3 layers that make AI-style orchestration safer, more inspectable, and more controllable.**

---

## 1. What Is DriftQ-Core?

One Go binary — **`driftqd`** — ships three functional layers and a shared platform.

```mermaid
mindmap
  root((DriftQ-Core))
    v1 Broker
      Durable Topics
      Partitions & Offsets
      Consumer Groups
      Leases / Ack / Nack
      Retry & DLQ
      Router Hook
    v2/v3 Runtime
      Workflow Engine
      DAG Execution
      Replay
      Timers & HITL
      Evals & Risk
      Governance
      Agent State & Memory
      Tool Gateway
      Self-Healing
      Forensics
      Brain v1 Routing
    Multi-Agent Layer
      Message Schema
      Agent Routing
      Coordination Patterns
    Shared Platform
      HTTP API
      WAL Durability
      File Store
      OpenTelemetry
      Prometheus Metrics
      Embedded UI
```

---

## 2. Top-Level System Map

```mermaid
graph TB
    subgraph Clients
        CLI["driftqctl CLI"]
        HTTP["HTTP Clients / Agents"]
    end

    subgraph driftqd["driftqd — single process"]
        direction TB

        subgraph HTTPAPI["HTTP Layer"]
            V1["/v1/* — Broker API"]
            DBG["/debug/* — Runtime API"]
            MET["/metrics — Prometheus"]
            UI["/ui — Embedded Dashboard"]
        end

        subgraph BROKER["v1 Broker  (internal/broker/)"]
            B_TOPIC["Topics & Partitions"]
            B_DISPATCH["Dispatch & Consumer Groups"]
            B_LEASE["Lease Manager"]
            B_RETRY["Retry & DLQ"]
            B_ROUTER["Router Hook"]
        end

        subgraph ENGINE["v2/v3 Engine  (internal/engine/)"]
            E_RUNNER["Runner (core)"]
            E_DAG["DAG Executor"]
            E_REPLAY["Replay Engine"]
            E_TIMER["Timer & Resume"]
            E_ARTIFACT["Artifact Store"]
            E_V3["v3 Layers"]
        end

        subgraph MULTIAGENT["Multi-Agent  (internal/multiagent/)"]
            MA_MSG["Message Schema"]
            MA_COORD["Coordination Patterns"]
        end

        subgraph INFRA["Shared Infrastructure"]
            WAL["Broker WAL  (internal/storage/)"]
            FSTORE["Engine FileStore"]
            OTEL["OpenTelemetry  (internal/observability/)"]
            PROM["Prometheus"]
        end
    end

    CLI -->|HTTP| HTTPAPI
    HTTP -->|HTTP| HTTPAPI

    V1 --> BROKER
    DBG --> ENGINE
    DBG --> MULTIAGENT

    BROKER -->|durability| WAL
    ENGINE -->|durability| FSTORE
    ENGINE -->|uses transport| BROKER

    HTTPAPI -->|traces & spans| OTEL
    BROKER -->|metrics & spans| OTEL
    ENGINE -->|metrics & spans| OTEL
    OTEL --> PROM
```

---

## 3. Server Startup Sequence

```mermaid
flowchart TD
    A([Start driftqd]) --> B[Parse flags]
    B --> C[Configure structured logging]
    C --> D{OTel enabled?}
    D -->|yes| E[Init OpenTelemetry provider]
    D -->|no| F
    E --> F[Open Broker WAL]
    F --> G[Rebuild broker state from WAL]
    G --> H[Build Engine Store]
    H --> I[Build Runner with store + config]
    I --> J[Register handler registries]
    J --> K[Attach HTTP routes]
    K --> L[Start background loops]
    L --> M1[Broker redelivery loop]
    L --> M2[Engine timer resume loop]
    L --> M3[Offset flush loop]
    M1 & M2 & M3 --> N[Serve HTTP]
    N --> O{SIGINT / SIGTERM?}
    O -->|signal| P[Graceful shutdown]
    P --> P1[Stop accepting requests]
    P1 --> P2[Cancel app-level goroutines]
    P2 --> P3[Drain HTTP]
    P3 --> P4[Close broker resources]
    P4 --> P5[Close engine file store]
    P5 --> P6[Flush OTel exporters]
    P6 --> Q([Exit])
```

> **Key insight:** durable state is written **incrementally** during normal operation — restart recovery comes from the WAL and FileStore, not a final checkpoint.

---

## 4. The Broker Architecture

### 4.1 Broker Internal Structure

```mermaid
graph LR
    subgraph API["HTTP /v1/*"]
        PR[POST /v1/produce]
        CO[GET  /v1/consume]
        AC[POST /v1/ack]
        NA[POST /v1/nack]
    end

    subgraph InMemoryBroker["InMemoryBroker (broker.go)"]
        direction TB
        TM["Topic Manager\n(topics / partitions)"]
        CG["Consumer Group Manager\n(offsets / streams)"]
        LM["Lease Manager\n(in-flight tracking)"]
        RM["Retry Manager\n(backoff / DLQ)"]
        RT["Router Hook"]
        MS["Metrics Sink"]
        IDM["Idempotency Check"]
    end

    subgraph Durable["Durability (storage/wal.go)"]
        WAL["Write-Ahead Log"]
    end

    PR --> IDM --> RT --> TM --> WAL
    TM -->|dispatch| CG --> LM
    AC --> LM -->|commit offset| CG
    NA --> RM -->|reschedule or DLQ| TM
    CO --> CG
    LM -->|lease expired| RM
    TM --> MS
    CG --> MS
```

### 4.2 Broker Message Produce Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant H as HTTP /v1/produce
    participant B as InMemoryBroker
    participant R as Router Hook
    participant W as WAL
    participant P as Partition
    participant CS as Consumer Stream

    C->>H: POST /v1/produce {topic, payload}
    H->>B: Produce(ctx, msg)
    B->>B: validate topic / limits
    B->>B: check idempotency key
    B->>R: Route(msg) → enriched msg
    R-->>B: routing decision
    B->>W: append WAL record
    W-->>B: ok
    B->>P: store in active partition
    alt consumer is waiting
        P->>CS: dispatch with lease
        CS-->>C: (async stream delivery)
    end
    B-->>H: offset + id
    H-->>C: 200 OK
```

### 4.3 Broker Consume / Ack Flow

```mermaid
sequenceDiagram
    participant C as Consumer
    participant H as HTTP /v1/consume
    participant B as InMemoryBroker
    participant L as Lease Manager
    participant R as Retry/DLQ

    C->>H: GET /v1/consume?topic=T&group=G
    H->>B: ConsumeStream(topic, group, owner)
    B->>L: issue lease + track in-flight
    B-->>C: message + lease_id

    alt ACK path
        C->>H: POST /v1/ack {lease_id}
        H->>B: Ack(lease_id)
        B->>L: release lease
        B->>B: commit group offset
    else NACK path
        C->>H: POST /v1/nack {lease_id}
        H->>B: Nack(lease_id)
        B->>R: apply retry policy
        alt attempts < max
            R->>B: re-enqueue with backoff
        else exhausted
            R->>B: route to DLQ topic
        end
    else Lease timeout
        L->>R: lease expired → redeliver
        R->>B: make eligible again
    end
```

### 4.4 Broker Durability Mental Model

```mermaid
graph LR
    W["WAL\n(source of truth)"] -->|replay on restart| M["In-Memory State\n(working set)"]
    M -->|append on change| W

    style W fill:#d4edda,stroke:#28a745
    style M fill:#cce5ff,stroke:#004085
```

---

## 5. The Workflow Runtime Architecture

### 5.1 Runner Internals

```mermaid
graph TB
    subgraph Runner["Runner struct (engine/runner.go)"]
        direction TB
        STORE["Store\n(MemoryStore or FileStore)"]
        REG["HandlerRegistry\n+ TenantRegistries"]
        GRAPHS["Compiled WorkflowGraphs\n(DAG cache)"]
        POL["AuthorizationPolicyBundle"]
        RISK["RiskPolicy"]
        BRAIN["BrainPolicy (v3)"]
        TOOL["ToolGatewayBundle (v3)"]
        ART["ArtifactStore"]
        BUDGET["BudgetPolicy\n+ Throttles"]
        OBS["runtimeTelemetry"]
    end

    SPEC["WorkflowSpec (JSON)"] -->|CompileSpecToExecutable| GRAPHS
    GRAPHS --> DAG["DAG Executor\n(runner_dag.go)"]
    DAG --> REG
    DAG --> STORE
    DAG --> POL
    DAG --> RISK
    DAG --> BRAIN
    DAG --> ART
    DAG --> BUDGET
    DAG --> OBS
```

### 5.2 Workflow Execution Flow (Happy Path)

```mermaid
flowchart TD
    A([POST /debug/run-spec]) --> B[Parse WorkflowSpec]
    B --> C[Resolve tenant & principal]
    C --> D[Select tenant handler registry]
    D --> E[CompileSpecToExecutable]
    E --> F[Authorization preflight\nauthz.go]
    F --> G{Authorized?}
    G -->|denied| ERR1([Return 403])
    G -->|ok| H[Risk evaluation\nrisk.go]
    H --> I{Risk level?}
    I -->|blocked| ERR2([Return 4xx])
    I -->|ok| J[Governance & quota checks\ngovernance.go]
    J --> K{Within quota?}
    K -->|exceeded| ERR3([Return 429])
    K -->|ok| L[Create/persist Run record]
    L --> M[Schedule ready nodes]

    subgraph NODELOOP["Per-Node Execution Loop"]
        M --> N[Pop ready node]
        N --> O{Replay mode?}
        O -->|time-travel| CACHED([Use cached output])
        O -->|live / new| P[Execute NodeFunc handler]
        P --> Q[Persist node output\n+ artifacts + events]
        Q --> R{Timer or HITL needed?}
        R -->|timer| S([Enter waiting state])
        R -->|HITL| T([Pause for human task])
        R -->|none| U{More ready nodes?}
        U -->|yes| N
        U -->|no| V[Mark run terminal]
    end

    S --> W([Resume on timer tick])
    T --> X([Resume after approval])
    W & X --> N

    V --> Y([Emit final events])
```

### 5.3 DAG Execution (Parallel Nodes)

```mermaid
graph LR
    subgraph DAG["DAG Execution (runner_dag.go)"]
        direction TB
        A["Node A\n(ready)"]
        B["Node B\n(ready)"]
        C["Node C\n(depends on A)"]
        D["Node D\n(depends on A+B)"]
        E["Node E\n(depends on C+D)"]

        A -->|output| C
        A -->|output| D
        B -->|output| D
        C -->|output| E
        D -->|output| E
    end

    style A fill:#d4edda
    style B fill:#d4edda
    style C fill:#cce5ff
    style D fill:#cce5ff
    style E fill:#fff3cd
```

> Nodes A and B run in parallel. Node D becomes ready only when both A and B finish (join barrier). Fan-out up to `maxParallel` goroutines.

---

## 6. Replay Architecture

```mermaid
graph TB
    subgraph REPLAY_MODES["Replay Modes (engine/replay.go + replay_branches.go)"]
        TT["Time-Travel Replay\n→ reuse recorded outputs\n(no re-execution)"]
        LR["Live Replay\n→ re-execute from chosen step\n(fresh handler calls)"]
        BR["Branch Replay\n→ fork into alternate timeline\n(what-if analysis)"]
    end

    subgraph GUARDS["Replay-Safe Guards"]
        AG["Agent State\n→ blocks replay writes"]
        AM["Agent Memory\n→ blocks replay writes"]
        SE["Side Effects\n→ dry-run pattern"]
        FF["Forensics\n→ compare source vs replayed run"]
        SH["Self-Healing\n→ generates safer replay plan"]
    end

    RUN["Original Run"] --> TT & LR & BR
    BR --> NEW_BRANCH["New Branch Run\n(independent timeline)"]
    LR --> SAME_RUN["Same Run\n(from chosen node)"]
    TT --> SAME_RUN

    GUARDS -.->|protect| TT & LR & BR
```

### 6.1 Branch Replay Timelines

```mermaid
gitGraph
    commit id: "Run Start"
    commit id: "Node A"
    commit id: "Node B (failed)"
    branch what-if-branch-1
    checkout what-if-branch-1
    commit id: "Node B (fixed params)"
    commit id: "Node C (new outcome)"
    checkout main
    commit id: "Node B retry"
    commit id: "Node C"
    commit id: "Run End"
```

---

## 7. v3 Feature Layers

```mermaid
graph TB
    subgraph V3["v3 Runtime Layers (all inside internal/engine/)"]
        direction LR

        subgraph GUARDRAILS["Safety & Control"]
            AUTHZ["Authorization\nauthz.go\n─ policy bundles\n─ tool/workflow access"]
            RISK["Risk Engine\nrisk.go\n─ policies\n─ simulation"]
            EVALS["Evals & Gates\nevals.go\n─ regression datasets\n─ promotion gates"]
        end

        subgraph GOVHITL["Governance & HITL"]
            GOV["Governance\ngovernance.go\n─ tenant isolation\n─ quotas & audit"]
            HITL["Human-in-the-Loop\nhuman.go\n─ approval tasks\n─ review-edit steps\n─ timeout/resume"]
        end

        subgraph MEMORY["Agent Intelligence"]
            STATE["Agent State\nagent_state.go\n─ durable per-agent state\n─ lineage & versioning"]
            MEM["Agent Memory\nagent_memory.go\n─ semantic memory\n─ replay-safe reads"]
        end

        subgraph TOOLS["Side-Effect Control"]
            TG["Tool Gateway\ntool_gateway.go\n─ approved registry\n─ schema validation\n─ redaction\n─ audit records"]
            SE["Side Effects\nside_effects.go\n─ staged receipts\n─ commit/compensate\n─ dry-run flow"]
        end

        subgraph DEBUG["Debugging & Recovery"]
            REL["Releases\nworkflow_releases.go\n─ versioning\n─ canary/shadow"]
            HEAL["Self-Healing\nself_healing.go\n─ failure artifacts\n─ recovery plans"]
            FOR["Forensics\nforensics.go\n─ lineage graphs\n─ run diffs\n─ root-cause views"]
            REPB["Replay Branches\nreplay_branches.go\n─ what-if timelines"]
        end

        subgraph ROUTING["Smart Routing"]
            BRAIN["Brain v1\nbrain.go\n─ history-aware scoring\n─ explainable ranking\n─ cheap-first + escalation"]
        end
    end

    RUNNER["Runner (core)"] --> GUARDRAILS & GOVHITL & MEMORY & TOOLS & DEBUG & ROUTING
```

---

## 8. Multi-Agent Layer

```mermaid
graph LR
    subgraph MA["internal/multiagent/"]
        SCHEMA["Agent Message Schema"]
        VALID["Routing Validation\n(agent/team/capability/role)"]
        COORD["Coordination Patterns"]
    end

    subgraph PATTERNS["Coordination Patterns"]
        PW["Planner / Worker"]
        REV["Reviewer Flow"]
        RR["Request / Reply"]
        HO["Handoff"]
        ESC["Escalation"]
        BC["Broadcast"]
    end

    subgraph TRANSPORT["Transport"]
        BROKER["v1 Broker Topics"]
    end

    SCHEMA --> VALID --> COORD --> PATTERNS
    PATTERNS --> BROKER
    BROKER --> ENGINE["Engine Runner\n(processes messages as workflow inputs)"]
```

---

## 9. Persistence Architecture

```mermaid
graph TB
    subgraph LAYERS["Three Independent Stores"]

        subgraph WAL_STORE["Broker WAL  (internal/storage/wal.go)"]
            WAL_DATA["Stores:\n• topic metadata\n• produced messages\n• committed offsets\n• retry state\n• idempotency keys"]
        end

        subgraph ENGINE_STORE["Engine Store  (internal/engine/memstore.go + store_file.go)"]
            ES_DATA["Stores:\n• Run records\n• NodeExecution records\n• RunEvents\n• Timer records\n• v3 KV metadata"]
            ES_IMPL["Implementations:\nMemoryStore  → dev/test\nFileStore    → production"]
        end

        subgraph ART_STORE["Artifact Store  (internal/engine/artifact_store.go)"]
            AS_DATA["Stores:\n• large node outputs\n• metadata (tenant/run/node/hash/size)"]
            AS_IMPL["Implementations:\nMemoryArtifactStore\nLocalArtifactStore"]
        end
    end

    subgraph WHY["Why Three Stores?"]
        W1["Broker WAL → append-only log\nfor ordered message replay"]
        W2["Engine Store → structured run/event state\nfor workflow introspection"]
        W3["Artifact Store → separate large blobs\nkeeps event logs small\nenables replay + forensics"]
    end

    WAL_STORE -.->|rationale| W1
    ENGINE_STORE -.->|rationale| W2
    ART_STORE -.->|rationale| W3
```

---

## 10. HTTP API Surface

```mermaid
graph LR
    subgraph SERVER["driftqd HTTP Server"]
        direction TB

        subgraph STABLE["/v1/* — Stable Broker API"]
            V_CREATE["POST /v1/topics"]
            V_PROD["POST /v1/produce"]
            V_CONS["GET  /v1/consume"]
            V_ACK["POST /v1/ack"]
            V_NACK["POST /v1/nack"]
        end

        subgraph RUNTIME["/debug/* — Runtime API  (http_debug.go)"]
            D_RUNS["runs / replay / run state"]
            D_EVALS["evals  (http_evals.go)"]
            D_POLICY["policy & risk"]
            D_AUDIT["audit records"]
            D_HUMAN["human tasks"]
            D_STATE["agent state & memory"]
            D_TOOL["tool gateway & calls"]
            D_SE["side effects"]
            D_REL["workflow releases  (http_workflow_releases.go)"]
            D_HEAL["self-healing  (http_self_healing.go)"]
            D_FOR["forensics  (http_forensics.go)"]
            D_REPB["replay branches  (http_replay_branches.go)"]
            D_BRAIN["brain policy / route sim  (http_brain.go)"]
        end

        subgraph OBS["/metrics + /ui"]
            PROM["Prometheus /metrics"]
            UI["Embedded React UI /ui"]
        end
    end
```

---

## 11. Observability Architecture

```mermaid
graph TB
    subgraph APP["Application Instrumentation"]
        HTTP_MW["HTTP Trace Middleware\n(observability/middleware)"]
        BROKER_SPAN["Broker Spans\n(topic ops / hot-path)"]
        ENGINE_SPAN["Engine Spans\n(run / node / authz / risk\ngovernance / HITL / replay\ntool / artifact)"]
        PROM_M["Prometheus Metrics\n(broker + engine counters)"]
    end

    subgraph OTEL["OpenTelemetry (internal/observability/)"]
        PROVIDER["TracerProvider + MeterProvider"]
        EXPORTER["OTLP/HTTP Exporter"]
        BROKER_WRAP["OTel Broker Wrapper"]
        ROUTER_WRAP["OTel Router Wrapper"]
    end

    subgraph BACKENDS["Observability Backends"]
        OTEL_BACKEND["OTLP Collector / Jaeger / etc"]
        PROM_BACKEND["Prometheus Scraper"]
    end

    HTTP_MW --> PROVIDER
    BROKER_SPAN --> BROKER_WRAP --> PROVIDER
    ENGINE_SPAN --> PROVIDER
    PROM_M --> PROM_BACKEND

    PROVIDER --> EXPORTER --> OTEL_BACKEND

    NOTE["Logs include correlated trace IDs\nHTTP requests can continue upstream traces\nDriftQ does NOT host an OTLP ingest endpoint"]
```

---

## 12. Concurrency Model

```mermaid
graph TB
    subgraph BROKER_CONCURRENCY["Broker Concurrency"]
        BIG_LOCK["Single sync.RWMutex\naround all broker state"]
        REDELIVER["Redelivery loop\n(dedicated goroutine)"]
        WAL_WRITE["WAL writes serialized\nunder same lock"]
    end

    subgraph ENGINE_CONCURRENCY["Engine Concurrency"]
        GRAPH_MU["mu: graphs + registry"]
        POLICY_MU["policyMu: authz/risk/brain/tool bundles"]
        THROTTLE_MU["throttleMu: in-flight counters"]

        subgraph EXEC["Execution Model"]
            SEQ["Sequential workflow\n→ caller goroutine"]
            PAR["DAG workflow\n→ fan-out up to maxParallel goroutines"]
            TIMER_LOOP["Timer resume\n→ background goroutine"]
        end
    end

    NOTE2["DriftQ-Core is intentionally single-node.\nNo distributed scheduler complexity."]
```

---

## 13. Package Map (Annotated)

```mermaid
graph TD
    subgraph CMD["cmd/"]
        DRIFTQD["driftqd/\n→ server bootstrap\n→ HTTP routes\n→ middleware wiring\n→ main.go"]
        DRIFTQCTL["driftqctl/\n→ CLI for broker + runtime ops\n→ runs, topics, artifacts, replay"]
    end

    subgraph INTERNAL["internal/"]
        BROKER_PKG["broker/\n→ InMemoryBroker\n→ topics / partitions\n→ dispatch / leases\n→ retry / DLQ\n→ router hook\n→ metrics sink"]
        ENGINE_PKG["engine/\n→ Runner (core)\n→ DAG execution\n→ replay engine\n→ timers\n→ artifacts\n→ ALL v3 layers"]
        MA_PKG["multiagent/\n→ agent message schema\n→ routing validation\n→ coordination helpers"]
        OBS_PKG["observability/\n→ OTel setup\n→ HTTP middleware\n→ broker/router wrappers"]
        STORAGE_PKG["storage/\n→ broker WAL (wal.go)\n→ append-only log"]
        HTTPAPI_PKG["httpapi/v1/\n→ broker HTTP payload helpers"]
        DEBUG_PKG["debugtypes/\n→ shared debug response types"]
    end

    subgraph UI_PKG["ui/"]
        REACT["Embedded React Dashboard"]
    end

    DRIFTQD --> BROKER_PKG & ENGINE_PKG & MA_PKG & OBS_PKG & STORAGE_PKG & HTTPAPI_PKG
    ENGINE_PKG --> BROKER_PKG
    BROKER_PKG --> STORAGE_PKG
    ENGINE_PKG --> OBS_PKG
    BROKER_PKG --> OBS_PKG
```

---

## 14. Extension Points

```mermaid
graph LR
    subgraph SEAMS["Main Extension Seams"]
        direction TB

        RT["Router\ninternal/broker/types.go\n→ custom message routing / labeling"]
        MS_EXT["MetricsSink\ninternal/broker/metrics_sink.go\n→ custom metrics backend"]
        NF["NodeFunc + HandlerRegistry\ninternal/engine/runner.go + registry.go\n→ workflow handler implementation"]
        STORE_EXT["Store interface\ninternal/engine/memstore.go\n→ new runtime persistence backend"]
        ART_EXT["ArtifactStore interface\ninternal/engine/artifact_store.go\n→ remote / object-store backed artifacts"]
    end

    RT -.->|decouples| ROUTING_LOGIC["routing logic"]
    MS_EXT -.->|decouples| METRICS_BACKEND["metrics backend"]
    NF -.->|implements| WORKFLOW_WORK["user workflow work"]
    STORE_EXT -.->|implements| NEW_STORE["e.g. PostgreSQL backend"]
    ART_EXT -.->|implements| NEW_ART["e.g. S3 / GCS artifacts"]
```

---

## 15. Brain v1 Adaptive Routing

```mermaid
flowchart TD
    START([Incoming Node Execution]) --> SAFE[Apply safety/policy filters\nauthz.go + risk.go]
    SAFE --> HIST[Load run history\nbrain.go BrainPolicy]
    HIST --> SCORE[Score candidate routes\n─ cost ceilings\n─ provider/model policy\n─ past success rates\n─ latency / failure history]
    SCORE --> RANK[Explainable route ranking]
    RANK --> CHEAP{Cheap-first\nroute sufficient?}
    CHEAP -->|yes| EXEC_CHEAP([Execute on cheap route])
    CHEAP -->|no| ESC{Uncertainty /\nfailure / high risk?}
    ESC -->|yes| ESCALATE([Escalate to stronger model])
    ESC -->|no| EXEC_BEST([Execute on best-scored route])
```

> Brain v1 is **heuristic and explainable** — not a machine-learning model.

---

## 16. Design Philosophy — Tradeoffs Visualized

```mermaid
quadrantChart
    title DriftQ-Core Design Tradeoffs
    x-axis Simple --> Complex
    y-axis Local --> Distributed
    quadrant-1 Future roadmap
    quadrant-2 Distributed simplicity
    quadrant-3 Current DriftQ-Core
    quadrant-4 Local complexity
    Single-node operation: [0.1, 0.15]
    Append-only WAL durability: [0.2, 0.1]
    Explicit debug routes: [0.3, 0.1]
    Runtime layering (vs microservices): [0.25, 0.2]
    Distributed cloud control plane: [0.75, 0.85]
```

| Decision | Chosen | Alternative | Why |
|---|---|---|---|
| Single-node | ✅ | Distributed | Simpler ops & reasoning |
| Append-only WAL | ✅ | Complex query DB | Easier replay & debugging |
| Debug routes | ✅ | Abstraction purity | Debuggability over purity |
| Runtime layering | ✅ | Separate microservices | One engine, less network |

---

## 17. Suggested Reading Order

```mermaid
flowchart LR
    README["1. README.md"] --> MAIN["2. cmd/driftqd/main.go"]
    MAIN --> BROKER["3. internal/broker/\nbroker.go, types.go"]
    BROKER --> ENGINE["4. internal/engine/\nrunner.go, types.go"]
    ENGINE --> MA["5. internal/multiagent/\nregistry.go"]
    MA --> OBS["6. internal/observability/"]
    OBS --> CTL["7. cmd/driftqctl/"]

    style README fill:#d4edda
    style CTL fill:#d4edda
```

**If you only care about one area:**

| Goal | Start here |
|---|---|
| Broker only | `internal/broker/` → `broker.go`, `types.go` |
| Workflow runtime | `internal/engine/` → `runner.go`, `runner_dag.go`, `spec.go` |
| Agent routing / messages | `internal/multiagent/` → `registry.go` |
| OTel / traces / metrics | `internal/observability/` |
| v3 safety layers | `internal/engine/` → `authz.go`, `risk.go`, `governance.go`, `human.go` |
| Replay / forensics | `internal/engine/` → `replay.go`, `replay_branches.go`, `forensics.go` |
| Smart routing | `internal/engine/brain.go` |

---

## 18. Final Mental Model

```mermaid
graph TB
    BROKER["Broker\n= durable transport\n(messages flow here)"]
    ENGINE["Engine\n= durable execution\n(workflows run here)"]
    MULTIAGENT["Multi-Agent\n= structured communication\n(on top of broker)"]
    SAFETY["Guardrails / Governance / HITL\n= safety & control\n(around execution)"]
    INTEL["State / Memory / Releases / Replay / Forensics\n= long-lived runtime intelligence\n(debugging & self-improvement)"]
    OBS_FINAL["Observability\n= visibility across all layers"]

    BROKER -->|transport for| ENGINE
    ENGINE -->|uses| MULTIAGENT
    ENGINE -->|enforced by| SAFETY
    ENGINE -->|augmented by| INTEL
    BROKER & ENGINE & SAFETY & INTEL -->|instrumented by| OBS_FINAL
```
