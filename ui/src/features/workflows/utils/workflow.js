import { safeNum } from "../../../utils/number";

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
      const rawStatus = String(node.status || "").toLowerCase();

      const status =
        rawStatus === "succeeded"
          ? "completed"
          : rawStatus === "canceled"
            ? "canceled"
            : rawStatus === "failed"
              ? "failed"
              : "running";

      return { name: node.node_id || "node", status, duration, replayable: status === "completed" };
    });

  const rawRun = String(run.status || "").toLowerCase();
  const status = rawRun === "succeeded" ? "completed" : rawRun === "canceled" ? "canceled" : rawRun === "failed" ? "failed" : "running";

  return {
    id: run.run_id,
    status,
    startedAt: run.started_at,
    steps,
    cancelable: status === "running"
  };
}
