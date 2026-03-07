import { safeNum } from "../../../utils/number";
import { counterMap, normalizeReason, pushCounterEvents } from "./counters";

export function buildDashboardEvents({ prev, nextTopics, metricRows, totals, prevTotals, ts, nowMs, selectedGroup }) {
  const deltaEvents = [];

  if (prev.at > 0) {
    for (const topic of nextTopics) {
      const producedDelta = safeNum(topic.produced) - safeNum(prev.producedByTopic[topic.name]);
      if (producedDelta > 0) {
        deltaEvents.push({
          id: `p-${topic.name}-${nowMs}`,
          type: "PRODUCE",
          color: "#00ff9d",
          ts,
          topic: topic.name,
          group: "broker",
          count: producedDelta
        });
      }

      const inflightDelta = safeNum(topic.inflight) - safeNum(prev.inflightByTopic[topic.name]);
      if (inflightDelta > 0) {
        deltaEvents.push({
          id: `lease-${topic.name}-${nowMs}`,
          type: "LEASE",
          color: "#818cf8",
          ts,
          topic: topic.name,
          group: selectedGroup,
          count: inflightDelta
        });
      }
    }

    const topicCreatedCounters = counterMap(metricRows, "topic_created_total", ["topic"]);
    const ackCounters = counterMap(metricRows, "message_acks_total", ["topic", "group"]);
    const nackCounters = counterMap(metricRows, "message_nacks_total", ["topic", "group", "reason"]);
    const leaseTimeoutCounters = counterMap(metricRows, "message_lease_timeouts_total", ["topic", "group"]);
    const redeliveryCounters = counterMap(metricRows, "message_redeliveries_total", ["topic", "group", "cause"]);
    const dlqCountersByReason = counterMap(metricRows, "dlq_messages_total", ["topic", "reason"]);

    pushCounterEvents(deltaEvents, {
      current: topicCreatedCounters,
      previous: prev.eventCounters.topicCreated,
      type: "TOPIC",
      color: "#f59e0b",
      ts,
      parse: (key) => {
        const [topic] = key.split("\u0001");
        return { topic: topic || "unknown", group: "system", detail: "created" };
      }
    });

    pushCounterEvents(deltaEvents, {
      current: ackCounters,
      previous: prev.eventCounters.acks,
      type: "ACK",
      color: "#6ee7b7",
      ts,
      parse: (key) => {
        const [topic, eventGroup] = key.split("\u0001");
        return { topic: topic || "unknown", group: eventGroup || "unknown" };
      }
    });

    pushCounterEvents(deltaEvents, {
      current: nackCounters,
      previous: prev.eventCounters.nacks,
      type: "NACK",
      color: "#fb7185",
      ts,
      parse: (key) => {
        const [topic, eventGroup, reason] = key.split("\u0001");
        return { topic: topic || "unknown", group: eventGroup || "unknown", detail: normalizeReason(reason) };
      }
    });

    pushCounterEvents(deltaEvents, {
      current: leaseTimeoutCounters,
      previous: prev.eventCounters.leaseTimeouts,
      type: "LEASE_TIMEOUT",
      color: "#f97316",
      ts,
      parse: (key) => {
        const [topic, eventGroup] = key.split("\u0001");
        return { topic: topic || "unknown", group: eventGroup || "unknown" };
      }
    });

    pushCounterEvents(deltaEvents, {
      current: redeliveryCounters,
      previous: prev.eventCounters.redeliveries,
      type: "REDELIVERY",
      color: "#38bdf8",
      ts,
      parse: (key) => {
        const [topic, eventGroup, cause] = key.split("\u0001");
        return { topic: topic || "unknown", group: eventGroup || "unknown", detail: normalizeReason(cause) };
      }
    });

    pushCounterEvents(deltaEvents, {
      current: dlqCountersByReason,
      previous: prev.eventCounters.dlq,
      type: "DLQ",
      color: "#ef4444",
      ts,
      parse: (key) => {
        const [topic, reason] = key.split("\u0001");
        return { topic: topic || "dlq", group: "system", detail: normalizeReason(reason) };
      }
    });

    if (totals.runCount > prevTotals.runCount) {
      deltaEvents.push({
        id: `r-${nowMs}`,
        type: "RUN",
        color: "#38bdf8",
        ts,
        topic: "workflow",
        group: "v2",
        count: totals.runCount - prevTotals.runCount
      });
    }
  }

  if (deltaEvents.length === 0) {
    deltaEvents.push({ id: `h-${nowMs}`, type: "HEARTBEAT", color: "#64748b", ts, topic: "node", group: "local", count: 1 });
  }

  return {
    deltaEvents,
    eventCounters: {
      topicCreated: counterMap(metricRows, "topic_created_total", ["topic"]),
      acks: counterMap(metricRows, "message_acks_total", ["topic", "group"]),
      nacks: counterMap(metricRows, "message_nacks_total", ["topic", "group", "reason"]),
      leaseTimeouts: counterMap(metricRows, "message_lease_timeouts_total", ["topic", "group"]),
      redeliveries: counterMap(metricRows, "message_redeliveries_total", ["topic", "group", "cause"]),
      dlq: counterMap(metricRows, "dlq_messages_total", ["topic", "reason"])
    }
  };
}
