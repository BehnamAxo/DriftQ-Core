import { COMMON_TEXT, WORKFLOW_STATUS } from "../../../constants/ui";
import { safeNum } from "../../../utils/number";

function normalizeStatus(rawStatus) {
  const value = String(rawStatus || COMMON_TEXT.EMPTY).toLowerCase();

  if (value === WORKFLOW_STATUS.SUCCEEDED) {
    return WORKFLOW_STATUS.COMPLETED;
  }

  if (value === WORKFLOW_STATUS.CANCELED) {
    return WORKFLOW_STATUS.CANCELED;
  }

  if (value === WORKFLOW_STATUS.FAILED) {
    return WORKFLOW_STATUS.FAILED;
  }

  if (value === WORKFLOW_STATUS.WAITING) {
    return WORKFLOW_STATUS.WAITING;
  }

  return WORKFLOW_STATUS.RUNNING;
}

export function normalizeRun(detail) {
  if (!detail?.run) {
    return null;
  }

  const run = detail.run;
  const nodes = Array.isArray(detail.nodes) ? detail.nodes : [];
  const latestByNode = new Map();

  for (const node of nodes) {
    const id = node.node_id || "node";
    const prev = latestByNode.get(id);
    if (!prev || safeNum(node.attempt) >= safeNum(prev.attempt)) {
      latestByNode.set(id, node);
    }
  }

  const steps = Array.from(latestByNode.values())
    .sort((a, b) => String(a.node_id || "").localeCompare(String(b.node_id || "")))
    .map((node) => {
      const started = node.started_at ? Date.parse(node.started_at) : 0;
      const ended = node.ended_at ? Date.parse(node.ended_at) : 0;
      const duration = started > 0 && ended >= started ? ended - started : 0;
      const status = normalizeStatus(node.status);

      return {
        name: node.node_id || "node",
        status,
        rawStatus: String(node.status || COMMON_TEXT.EMPTY).toLowerCase(),
        duration,
        attempts: safeNum(node.attempt),
        replayable: status !== WORKFLOW_STATUS.RUNNING,
        startedAt: node.started_at || COMMON_TEXT.EMPTY,
        endedAt: node.ended_at || COMMON_TEXT.EMPTY,
        error: node.error || COMMON_TEXT.EMPTY,
        hasInput: Boolean(node.has_input),
        hasOutput: Boolean(node.has_output),
        inputBytes: safeNum(node.input_bytes),
        outputBytes: safeNum(node.output_bytes)
      };
    });

  const status = normalizeStatus(run.status);
  const started = run.started_at ? Date.parse(run.started_at) : 0;
  const ended = run.ended_at ? Date.parse(run.ended_at) : 0;
  const duration = started > 0 && ended >= started ? ended - started : 0;
  const counts = steps.reduce(
    (acc, step) => {
      acc[step.status] = (acc[step.status] || 0) + 1;
      return acc;
    },
    { completed: 0, failed: 0, canceled: 0, running: 0, waiting: 0 }
  );

  return {
    id: run.run_id,
    workflowId: run.workflow_id || COMMON_TEXT.EMPTY,
    status,
    rawStatus: String(run.status || COMMON_TEXT.EMPTY).toLowerCase(),
    startedAt: run.started_at,
    endedAt: run.ended_at || COMMON_TEXT.EMPTY,
    duration,
    terminalReason: run.terminal_reason || COMMON_TEXT.EMPTY,
    terminalMeta: run.terminal_meta || null,
    steps,
    counts,
    cancelable: status === WORKFLOW_STATUS.RUNNING || status === WORKFLOW_STATUS.WAITING
  };
}
