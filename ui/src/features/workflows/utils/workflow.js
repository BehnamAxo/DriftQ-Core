import { safeNum } from "../../../utils/number";

function normalizeStatus(rawStatus) {
  const value = String(rawStatus || "").toLowerCase();

  if (value === "succeeded") {
    return "completed";
  }

  if (value === "canceled") {
    return "canceled";
  }

  if (value === "failed") {
    return "failed";
  }

  if (value === "waiting") {
    return "waiting";
  }

  return "running";
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
        rawStatus: String(node.status || "").toLowerCase(),
        duration,
        attempts: safeNum(node.attempt),
        replayable: status !== "running",
        startedAt: node.started_at || "",
        endedAt: node.ended_at || "",
        error: node.error || "",
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
    workflowId: run.workflow_id || "",
    status,
    rawStatus: String(run.status || "").toLowerCase(),
    startedAt: run.started_at,
    endedAt: run.ended_at || "",
    duration,
    terminalReason: run.terminal_reason || "",
    terminalMeta: run.terminal_meta || null,
    steps,
    counts,
    cancelable: status === "running" || status === "waiting"
  };
}
