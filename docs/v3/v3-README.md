# v3 - Multi-Agent Runtime & Guardrails Foundations

This document covers the current v3 foundations inside DriftQ-Core.

Right now, v3 has two implemented foundation areas:

- multi-agent messaging
- guardrails and evaluation

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

The current implementation intentionally reuses:

- broker topics and routing
- stored `Run.Spec`
- stored `Run.InitialInput`
- replayable engine execution
- existing active index promote/rollback primitives
