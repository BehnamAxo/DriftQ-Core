import { API_PATHS, COMMON_TEXT, CONSUMER_STATUS, METRIC_NAME } from "../../../constants/ui";
import { safeNum } from "../../../utils/number";

export async function loadConsumerLagDetails(metricRows, selectedGroup, signal, getJSON) {
  const lagMetrics = metricRows.filter((row) => row.name === METRIC_NAME.CONSUMER_LAG);
  const inflightMetrics = metricRows.filter((row) => row.name === METRIC_NAME.INFLIGHT_MESSAGES);
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
        const payload = await getJSON(API_PATHS.topicLag(group), signal);
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
    const ownersSet = new Set();
    const stalledCount = rows.reduce((sum, row) => sum + (row.stalled ? 1 : 0), 0);
    for (const row of rows) {
      for (const owner of row.lease_owners || []) {
        if (owner) {
          ownersSet.add(owner);
        }
      }
      if (row.last_owner) {
        ownersSet.add(row.last_owner);
      }
    }
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
          partitions: topicRows.length,
          lastDeliveredAt: Math.max(0, ...topicRows.map((row) => safeNum(row.last_delivered_at_ms))),
          owners: Array.from(new Set(topicRows.flatMap((row) => row.lease_owners || []).concat(topicRows.map((row) => row.last_owner || "").filter(Boolean)))).sort()
        };
      });

    return {
      group,
      topics: Array.from(topicsSet),
      owners: Array.from(ownersSet).sort(),
      activeLease: rows.reduce((sum, row) => sum + safeNum(row.inflight), 0),
      totalAcked: rows.reduce((sum, row) => sum + Math.max(0, safeNum(row.committed_offset)), 0),
      totalNacked: 0,
      totalLag: rows.reduce((sum, row) => sum + safeNum(row.lag), 0),
      partitions: rows.length,
      stalledCount,
      rows: rows
        .map((row) => ({
          topic: row.topic || COMMON_TEXT.UNKNOWN,
          partition: safeNum(row.partition),
          headOffset: Math.max(0, safeNum(row.head_offset)),
          committedOffset: Math.max(0, safeNum(row.committed_offset)),
          inflight: safeNum(row.inflight),
          lag: safeNum(row.lag),
          leaseOwners: Array.isArray(row.lease_owners) ? row.lease_owners : [],
          lastOwner: row.last_owner || COMMON_TEXT.EMPTY,
          lastDeliveredAt: safeNum(row.last_delivered_at_ms),
          oldestLeaseAge: safeNum(row.oldest_lease_age_ms),
          leaseDurationMs: safeNum(row.lease_duration_ms),
          leaseExpiresAt: safeNum(row.lease_expires_at_ms),
          stalled: Boolean(row.stalled)
        }))
        .sort((a, b) => {
          if (a.topic !== b.topic) {
            return a.topic.localeCompare(b.topic);
          }
          return a.partition - b.partition;
        }),
      topicSummaries,
      status: rows.some((row) => safeNum(row.inflight) > 0) ? CONSUMER_STATUS.CONNECTED : rows.some((row) => safeNum(row.lag) > 0) ? CONSUMER_STATUS.BACKLOG : CONSUMER_STATUS.IDLE
    };
  });
}
