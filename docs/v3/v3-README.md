# v3 - Multi-Agent Runtime & Real-Time AI Foundations

This document covers the current v3 foundations inside DriftQ-Core.

The goal of v3 is one coherent runtime track on top of the broker and replayable engine: multi-agent messaging, guardrails, governance, human review, durable agent memory, safer tool execution, smarter routing, release engineering, self-healing, forensics, and what-if replay.

The v3 runtime now includes these implemented foundation areas:

- multi-agent messaging and coordination
- guardrails, evaluation, authorization, and runtime risk
- governance and tenant isolation
- human-in-the-loop workflow steps
- OpenTelemetry-native observability
- durable agent state
- semantic agent memory
- secure tool gateway / MCP-ready control plane
- safe side-effect framework
- budgeting-aware adaptive routing
- realtime topic mode
- workflow release engineering
- self-healing from failures
- full lineage and forensic debugging
- branching replay timelines and what-if simulation
- Brain v1 smarter route selection

## What exists today

### Multi-agent messaging foundation

Implemented now:

- Agent inbox/outbox topics
- `agent.{id}.inbox`
- `agent.{id}.outbox`
- Team broadcast topics: `team.{id}.broadcast`
- Basic direct, capability, and broadcast routing
- Structured agent message contract
- Startup config and topic bootstrap in `driftqd`
- Outbox-aware routing and strict validation behavior

Topic conventions:

- `agent.{id}.inbox`
- `agent.{id}.outbox`
- `team.{id}.broadcast`

`{id}` may contain only:

- letters
- numbers
- `_`
- `-`

Agent message contract:

- required:
  - `sender`
  - `intent`
  - `payload` as a JSON object
  - exactly one route target:
    - `receiver`
    - `capability` or `role`
    - `team`
- optional:
  - `message_id`
  - `correlation_id`
  - `reply_to`
  - `created_at`
  - `tenant_id`
  - `route`
  - `coordination`

Current routing behavior:

- direct -> `agent.{receiver}.inbox`
- capability/role -> in-memory registry lookup -> `agent.{selected}.inbox`
- broadcast -> `team.{team}.broadcast`

Coordination primitives:

- planner -> worker request flows
- planner -> reviewer flows
- request/reply envelopes
- handoffs between agents
- escalation chains
- broadcast fan-out patterns

Current `driftqd` multi-agent flags:

- `-multiagent-config=/path/to/multiagent.json`
- `-bootstrap-multiagent-topics`

Config supports:

- `agents`
- `teams`
- `capabilities`
- `topic_partitions`
- `router_strict`
- `source_topics`

Important runtime notes:

- configured agent outboxes are always valid router source topics
- when `router_strict=true`, invalid agent payloads fail the produce request
- if `-multiagent-config` is not set, `driftqd` runs without the multi-agent router

Examples:

- `examples/multiagent/v3.1/multiagent.json`
- `examples/multiagent/v3.1/messages/direct.json`
- `examples/multiagent/v3.1/messages/capability.json`
- `examples/multiagent/v3.1/messages/broadcast.json`
- `scripts/multiagent_v31_smoke.sh`

### Guardrails and evaluation foundation

Implemented now:

- native eval suites with built-in evaluators
- regression datasets with persisted eval cases
- re-execution of old runs against new workflow specs
- pass/fail promotion gate over the active index pointer
- failure-to-test-case capture flow from existing runs
- authorization policy bundles for workflow/tool access
- runtime risk scoring and policy decisions
- risk simulation and policy inspection through debug routes

Built-in evaluators:

- `run_succeeded`
- `node_output_exact`

Current scope notes:

- exposed through debug endpoints
- no dedicated UI yet
- no dedicated CLI yet
- no external eval provider integrations yet
- no async eval scheduler yet

Core objects:

- eval dataset
- eval suite
- eval run/report
- authorization policy bundle
- risk policy
- workflow authorization report
- workflow risk report

Dataset case fields can include:

- `id`
- `name`
- `source_run_id`
- `spec`
- `input`
- `target_node_id`
- `expected_output`
- `labels`

Suite fields:

- `id`
- `dataset_id`
- `evaluator`
- `target_node_id`
- `pass_threshold`

Eval report includes:

- suite and dataset IDs
- evaluator used
- pass threshold
- pass rate
- per-case results
- final pass/fail

Authorization / risk scope:

- workflow/tool allow-deny policy bundles
- tenant-aware authorization checks
- risk scoring over prompt-injection-like input, suspicious tool chains, and unusual data movement
- allow / sandbox / require-approval / block decisions
- risk simulation through debug routes

### Governance and tenant isolation foundation

Implemented now:

- tenant-aware run access boundaries
- tenant-aware artifact access boundaries
- tenant-scoped handler registries/config
- per-tenant active run caps
- reuse of existing per-tenant budget and topic-cap controls
- audit-ready records for authorization, risk, governance, and human decisions
- safer multi-tenant runtime/debug defaults

Current scope notes:

- foundation layer rather than a full enterprise governance suite
- no dedicated admin UI yet
- no retention/export/search tooling for audits yet
- no org hierarchy or delegated admin model yet

Core objects:

- audit record
- tenant-scoped handler registry mapping
- tenant active run cap

### Human-in-the-loop foundation

Implemented now:

- workflow-native human approval steps
- workflow-native human review/edit steps
- timeout behavior with approve/reject/cancel actions
- resume-after-approval using the existing waiting/timer machinery
- risk-based escalation into human approval before execution continues
- debug endpoints to list and resolve pending human tasks

Current scope notes:

- foundation layer aimed at runtime behavior first
- no dedicated UI yet
- no async notification delivery yet
- no rich assignment/escalation rules yet

Core objects:

- human step spec
- human task
- human approval pending error

### OpenTelemetry-native observability foundation

Implemented now:

- standard OpenTelemetry traces for workflow lifecycle, node execution, authz, risk, governance, HITL, and replay
- standard OpenTelemetry metrics for runs, nodes, authz/risk/governance checks, human tasks, and replay activity
- nested tool execution spans and tool latency/outcome metrics
- artifact put/get/delete spans plus artifact operation metrics
- broker/topic operation spans for produce/consume/ack/nack/topic-create and OTel broker metrics sink coverage
- incoming trace-context extraction on HTTP requests so DriftQ spans correlate with upstream app traces
- OTLP/HTTP trace and metric export from `driftqd`
- existing structured logs continue to carry correlated `trace_id` values

Current scope notes:

- foundation layer aimed at production trace + metric export first
- no dedicated dashboards are shipped yet
- no OpenTelemetry log exporter is wired yet
- Prometheus `/metrics` still exists alongside OTLP export

Current `driftqd` observability flags:

- `-otel-enabled`
- `-otel-service-name`
- `-otel-endpoint`
- `-otel-insecure`
- `-otel-metrics-interval`

### Agent state storage foundation

Implemented now:

- durable per-agent state snapshots
- versioned state history
- lineage metadata tying state updates back to runs and steps
- tenant-scoped access boundaries
- replay-safe reads and blocked replay writes
- handler-context access to agent state

Core objects:

- agent state snapshot
- agent state write request
- agent state read options

### Semantic agent memory foundation

Implemented now:

- tenant-scoped semantic memory entries
- notes, runs, artifacts, and state snapshots as memory sources
- pluggable embedder interface with local similarity search foundation
- replay-safe memory reads/searches and blocked replay writes
- handler-context access to memory lookup

Core objects:

- agent memory entry
- memory write request
- memory search request
- memory search result

### Secure tool gateway / MCP-ready control plane foundation

Implemented now:

- approved tool/server bundle persisted in the engine store
- tenant-aware tool/server governance
- runtime tool-call policy enforcement
- input/output schema validation around tool execution
- secret redaction for stored tool-call records
- audit-ready tool-call records
- sandbox-aware runtime tool context
- MCP-ready server definitions and connector governance foundation

Core objects:

- tool gateway bundle
- tool call record
- tool/server definition

### Safe side-effect framework foundation

Implemented now:

- dry-run side-effect mode
- staged side-effect receipts
- explicit commit flow
- compensating action flow
- irreversible-action policy flags
- approval-before-commit through the existing HITL path

Core objects:

- side-effect policy
- side-effect receipt
- side-effect runtime context

### Budgeting-aware adaptive routing foundation

Implemented now:

- per-run and per-tenant budget-aware route filtering
- cost ceilings
- cheap-first route preference
- escalation on uncertainty/failure/risk
- provider/model selection policies
- route metadata on runtime and audit records

Current scope notes:

- foundation routing is still heuristic and policy-driven
- it is designed to feed the later smarter routing layer rather than replace it

### Realtime topics foundation

Implemented now:

- topic mode metadata persisted in broker state and WAL
- realtime / low-latency topic creation through broker APIs
- simplified low-latency consume path when full lease coordination is not needed

Current scope notes:

- aimed at responsiveness
- standard topic durability and ack/nack flows still exist for stronger control paths

### Workflow release engineering foundation

Implemented now:

- workflow version storage
- release channels
- canary resolution
- shadow runs using dry-run side-effect mode
- promotion and rollback
- prompt/model/tool/policy diffs through release diff views
- eval-gated canary finalization

Core objects:

- workflow release version
- workflow release channel
- workflow release resolution
- workflow release diff

### Self-healing orchestration foundation

Implemented now:

- automatic capture of failed runs into recovery artifacts
- replay suggestions
- safer rerun plans
- eval/test-case links from failure artifacts
- self-healing replay entry points

Core objects:

- self-healing artifact
- replay suggestion
- rerun plan

### Full lineage and forensic debugging foundation

Implemented now:

- end-to-end execution graph view
- run diffs
- workflow diffs
- root-cause view
- `what changed?` debugging helpers

Core objects:

- forensic execution graph
- forensic run diff
- forensic root-cause view
- forensic what-changed view

### Branching replay timelines and what-if simulation foundation

Implemented now:

- replay branch creation from existing runs
- alternate timelines without overwriting source history
- branch timeline listing
- branch comparisons against source or sibling runs
- replay from selected branch points with optional spec/input overrides

Core objects:

- replay branch request
- replay branch record

### Brain v1 smarter routing foundation

Implemented now:

- history-aware route scoring
- route ranking over cost, latency, success history, and escalation signals
- explainable route decision output
- debug policy management and route simulation

Current scope notes:

- Brain v1 is heuristic and explainable, not ML-driven
- it reuses the existing adaptive routing safety/policy filters and changes final ranking only when enabled

## Current debug endpoints

### Multi-agent

v3 messaging uses the existing broker API rather than new v3-specific endpoints:

- `POST /v1/topics`
- `POST /v1/produce`
- `GET /v1/consume`
- `POST /v1/ack`
- `POST /v1/nack`

### Evaluation

- `GET /debug/evals/evaluators`
- `GET /debug/evals/datasets`
- `POST /debug/evals/datasets`
- `GET /debug/evals/dataset?id=<DATASET_ID>`
- `GET /debug/evals/suites`
- `POST /debug/evals/suites`
- `GET /debug/evals/suite?id=<SUITE_ID>`
- `GET /debug/evals/runs`
- `POST /debug/evals/run`
- `GET /debug/evals/run?id=<EVAL_RUN_ID>`
- `POST /debug/evals/case-from-run`
- `POST /debug/evals/promote`

### Guardrails / Governance / HITL

- `GET /debug/policy`
- `POST /debug/policy`
- `GET /debug/risk-policy`
- `POST /debug/risk-policy`
- `GET /debug/tool-gateway`
- `POST /debug/tool-gateway`
- `GET /debug/tool-calls`
- `GET /debug/side-effects`
- `POST /debug/side-effects/commit`
- `POST /debug/side-effects/compensate`
- `GET /debug/audit`
- `GET /debug/human/tasks`
- `POST /debug/human/respond`
- `POST /debug/run-spec`

### Agent memory / state

- `GET /debug/agent-state`
- `POST /debug/agent-state`
- `GET /debug/agent-state/lineage`
- `GET /debug/agent-memory`
- `POST /debug/agent-memory`
- `POST /debug/agent-memory/search`

### Workflow release / self-healing / forensics / replay / brain

- `GET /debug/workflows/releases/versions`
- `POST /debug/workflows/releases/versions`
- `GET /debug/workflows/releases/version`
- `GET /debug/workflows/releases/channel`
- `POST /debug/workflows/releases/channel`
- `POST /debug/workflows/releases/promote`
- `POST /debug/workflows/releases/rollback`
- `GET /debug/workflows/releases/diff`
- `GET /debug/workflows/releases/resolve`
- `POST /debug/workflows/releases/finalize-canary`
- `GET /debug/self-heal/artifacts`
- `GET /debug/self-heal/artifact`
- `POST /debug/self-heal/artifact`
- `POST /debug/self-heal/replay`
- `GET /debug/forensics/lineage`
- `GET /debug/forensics/run-diff`
- `GET /debug/forensics/workflow-diff`
- `GET /debug/forensics/root-cause`
- `GET /debug/forensics/what-changed`
- `GET /debug/replay/branches`
- `POST /debug/replay/branches`
- `GET /debug/replay/branch`
- `GET /debug/replay/compare`
- `GET /debug/brain-policy`
- `POST /debug/brain-policy`
- `POST /debug/brain/route`

## Example flow

1. Start `driftqd` with multi-agent config if you need agent routing.
2. Configure policy, risk, tenant, and tool-gateway bundles for the runtime boundary you want.
3. Create or capture regression cases into an eval dataset.
4. Create an eval suite over that dataset.
5. Run the suite, optionally with a workflow `spec_override`.
6. Register workflow release versions and move traffic through channel/canary/shadow flows.
7. Inspect telemetry, audit logs, self-healing artifacts, and forensic diffs when runs fail.
8. Use replay branches and Brain v1 route explanations to investigate safer or smarter alternatives.

## Design intent

v3 is being built as a single track on top of the existing broker and workflow engine:

- messaging provides the transport and contract for agents
- evaluation and release engineering provide regression checks and guarded promotion for workflow changes
- authorization, risk, governance, tool gateway policy, and side-effect controls provide runtime guardrails and tenant safety
- human-in-the-loop provides first-class review/approval pauses inside workflow execution
- agent state and semantic memory provide durable per-agent context
- self-healing, forensics, and replay branching turn failures into debuggable and recoverable artifacts
- observability, adaptive routing, and Brain v1 turn runtime history into safer and smarter routing decisions

The current implementation intentionally reuses:

- broker topics and routing
- stored `Run.Spec`
- stored `Run.InitialInput`
- replayable engine execution
- existing active index promote/rollback primitives
- existing waiting/timer resume primitives for human approval flows
- HTTP request context propagation so engine spans nest under app/server traces
