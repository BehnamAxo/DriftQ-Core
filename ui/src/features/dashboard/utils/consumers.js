import { safeNum } from "../../../utils/number";

export async function loadConsumerLagDetails(metricRows, selectedGroup, signal, getJSON) {
  const lagMetrics = metricRows.filter((row) => row.name === "consumer_lag");
  const inflightMetrics = metricRows.filter((row) => row.name === "inflight_messages");
  const groups = new Set([selectedGroup]);

  for (const row of lagMetrics) {
    if (row.labels.group) {
      groups.add(row.labels.group);
    }
  }

  for (const row of inflightMetrics) {
    if (row.labels.group) {
      groups.add(row.labels.group);
    }
  }

  const groupList = Array.from(groups).sort();
  const lagDetails = {};
  await Promise.all(
    groupList.map(async (group) => {
      try {
        const payload = await getJSON(`/debug/topics/lag?group=${encodeURIComponent(group)}`, signal);
        lagDetails[group] = Array.isArray(payload.rows) ? payload.rows : [];
      } catch {
        lagDetails[group] = [];
      }
    })
  );

  return { groupList, lagDetails };
}

export function buildConsumers(groupList, lagDetails) {
  return groupList.map((group) => {
    const rows = lagDetails[group] || [];
    const topicsSet = new Set(rows.map((row) => row.topic));
    const topicSummaries = Array.from(topicsSet)
      .sort()
      .map((topicName) => {
        const topicRows = rows.filter((row) => row.topic === topicName);
        return {
          topic: topicName,
          lag: topicRows.reduce((sum, row) => sum + safeNum(row.lag), 0),
          inflight: topicRows.reduce((sum, row) => sum + safeNum(row.inflight), 0),
          committed: topicRows.reduce((sum, row) => sum + Math.max(0, safeNum(row.committed_offset)), 0),
          head: topicRows.reduce((sum, row) => sum + Math.max(0, safeNum(row.head_offset)), 0),
          partitions: topicRows.length
        };
      });

    return {
      group,
      topics: Array.from(topicsSet),
      activeLease: rows.reduce((sum, row) => sum + safeNum(row.inflight), 0),
      totalAcked: rows.reduce((sum, row) => sum + Math.max(0, safeNum(row.committed_offset)), 0),
      totalNacked: 0,
      totalLag: rows.reduce((sum, row) => sum + safeNum(row.lag), 0),
      partitions: rows.length,
      rows: rows
        .map((row) => ({
          topic: row.topic || "unknown",
          partition: safeNum(row.partition),
          headOffset: Math.max(0, safeNum(row.head_offset)),
          committedOffset: Math.max(0, safeNum(row.committed_offset)),
          inflight: safeNum(row.inflight),
          lag: safeNum(row.lag)
        }))
        .sort((a, b) => {
          if (a.topic !== b.topic) {
            return a.topic.localeCompare(b.topic);
          }
          return a.partition - b.partition;
        }),
      topicSummaries,
      status: rows.some((row) => safeNum(row.inflight) > 0) ? "connected" : rows.some((row) => safeNum(row.lag) > 0) ? "backlog" : "idle"
    };
  });
}
