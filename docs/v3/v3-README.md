# v3 - Multi-Agent Runtime & Guardrails Foundations

This document covers the current v3 foundations inside DriftQ-Core.

Right now, v3 has five implemented foundation areas:

- multi-agent messaging
- guardrails and evaluation
- governance and tenant isolation
- human-in-the-loop workflow steps
- OpenTelemetry-native observability

The goal is one coherent v3 track, not separate product docs per sub-slice.

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

Current routing behavior:

- direct -> `agent.{receiver}.inbox`
- capability/role -> in-memory registry lookup -> `agent.{selected}.inbox`
- broadcast -> `team.{team}.broadcast`

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

- Native eval suites with built-in evaluators
- Regression datasets with persisted eval cases
- Re-execution of old runs against new workflow specs
- Pass/fail promotion gate over the active index pointer
- Failure-to-test-case capture flow from existing runs
- Authorization policy bundles for workflow/tool access
- Runtime risk scoring and policy decisions
- Risk simulation and policy inspection through debug routes

Built-in evaluators:

- `run_succeeded`
- `node_output_exact`

Current scope notes:

- exposed through `debug` endpoints
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
- `GET /debug/audit`
- `GET /debug/human/tasks`
- `POST /debug/human/respond`
- `POST /debug/run-spec`

## Example flow

1. Start `driftqd` with multi-agent config if you need agent routing.
2. Create or capture regression cases into an eval dataset.
3. Create an eval suite over that dataset.
4. Run the suite, optionally with a workflow `spec_override`.
5. Inspect the eval report.
6. Promote only if the eval run passed.

## Design intent

v3 is being built as a single track on top of the existing broker and workflow engine:

- messaging provides the transport and contract for agents
- evaluation provides regression checks and guarded promotion for workflow changes
- authorization, risk, and governance provide runtime guardrails and tenant safety
- human-in-the-loop provides first-class review/approval pauses inside workflow execution

The current implementation intentionally reuses:

- broker topics and routing
- stored `Run.Spec`
- stored `Run.InitialInput`
- replayable engine execution
- existing active index promote/rollback primitives
- existing waiting/timer resume primitives for human approval flows
- HTTP request context propagation so engine spans nest under app/server traces
