import { COMMON_TEXT, EVENT_GROUP, EVENT_TYPE, METRIC_NAME } from "../../../constants/ui";
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
          type: EVENT_TYPE.PRODUCE,
          color: "#00ff9d",
          ts,
          topic: topic.name,
          group: EVENT_GROUP.BROKER,
          count: producedDelta
        });
      }

      const inflightDelta = safeNum(topic.inflight) - safeNum(prev.inflightByTopic[topic.name]);
      if (inflightDelta > 0) {
        deltaEvents.push({
          id: `lease-${topic.name}-${nowMs}`,
          type: EVENT_TYPE.LEASE,
          color: "#818cf8",
          ts,
          topic: topic.name,
          group: selectedGroup,
          count: inflightDelta
        });
      }
    }

    const topicCreatedCounters = counterMap(metricRows, METRIC_NAME.TOPIC_CREATED_TOTAL, ["topic"]);
    const ackCounters = counterMap(metricRows, METRIC_NAME.MESSAGE_ACKS_TOTAL, ["topic", "group"]);
    const nackCounters = counterMap(metricRows, METRIC_NAME.MESSAGE_NACKS_TOTAL, ["topic", "group", "reason"]);
    const leaseTimeoutCounters = counterMap(metricRows, METRIC_NAME.MESSAGE_LEASE_TIMEOUTS_TOTAL, ["topic", "group"]);
    const redeliveryCounters = counterMap(metricRows, METRIC_NAME.MESSAGE_REDELIVERIES_TOTAL, ["topic", "group", "cause"]);
    const dlqCountersByReason = counterMap(metricRows, METRIC_NAME.DLQ_MESSAGES_TOTAL, ["topic", "reason"]);

    pushCounterEvents(deltaEvents, {
      current: topicCreatedCounters,
      previous: prev.eventCounters.topicCreated,
      type: EVENT_TYPE.TOPIC,
      color: "#f59e0b",
      ts,
      parse: (key) => {
        const [topic] = key.split("\u0001");
        return { topic: topic || COMMON_TEXT.UNKNOWN, group: EVENT_GROUP.SYSTEM, detail: "created" };
      }
    });

    pushCounterEvents(deltaEvents, {
      current: ackCounters,
      previous: prev.eventCounters.acks,
      type: EVENT_TYPE.ACK,
      color: "#6ee7b7",
      ts,
      parse: (key) => {
        const [topic, eventGroup] = key.split("\u0001");
        return { topic: topic || COMMON_TEXT.UNKNOWN, group: eventGroup || COMMON_TEXT.UNKNOWN };
      }
    });

    pushCounterEvents(deltaEvents, {
      current: nackCounters,
      previous: prev.eventCounters.nacks,
      type: EVENT_TYPE.NACK,
      color: "#fb7185",
      ts,
      parse: (key) => {
        const [topic, eventGroup, reason] = key.split("\u0001");
        return { topic: topic || COMMON_TEXT.UNKNOWN, group: eventGroup || COMMON_TEXT.UNKNOWN, detail: normalizeReason(reason) };
      }
    });

    pushCounterEvents(deltaEvents, {
      current: leaseTimeoutCounters,
      previous: prev.eventCounters.leaseTimeouts,
      type: EVENT_TYPE.LEASE_TIMEOUT,
      color: "#f97316",
      ts,
      parse: (key) => {
        const [topic, eventGroup] = key.split("\u0001");
        return { topic: topic || COMMON_TEXT.UNKNOWN, group: eventGroup || COMMON_TEXT.UNKNOWN };
      }
    });

    pushCounterEvents(deltaEvents, {
      current: redeliveryCounters,
      previous: prev.eventCounters.redeliveries,
      type: EVENT_TYPE.REDELIVERY,
      color: "#38bdf8",
      ts,
      parse: (key) => {
        const [topic, eventGroup, cause] = key.split("\u0001");
        return { topic: topic || COMMON_TEXT.UNKNOWN, group: eventGroup || COMMON_TEXT.UNKNOWN, detail: normalizeReason(cause) };
      }
    });

    pushCounterEvents(deltaEvents, {
      current: dlqCountersByReason,
      previous: prev.eventCounters.dlq,
      type: EVENT_TYPE.DLQ,
      color: "#ef4444",
      ts,
      parse: (key) => {
        const [topic, reason] = key.split("\u0001");
        return { topic: topic || EVENT_GROUP.DLQ, group: EVENT_GROUP.SYSTEM, detail: normalizeReason(reason) };
      }
    });

    if (totals.runCount > prevTotals.runCount) {
      deltaEvents.push({
        id: `r-${nowMs}`,
        type: EVENT_TYPE.RUN,
        color: "#38bdf8",
        ts,
        topic: EVENT_GROUP.WORKFLOW,
        group: EVENT_GROUP.WORKFLOW_VERSION,
        count: totals.runCount - prevTotals.runCount
      });
    }
  }

  if (deltaEvents.length === 0) {
    deltaEvents.push({ id: `h-${nowMs}`, type: EVENT_TYPE.HEARTBEAT, color: "#64748b", ts, topic: EVENT_GROUP.NODE, group: EVENT_GROUP.LOCAL, count: 1 });
  }

  return {
    deltaEvents,
    eventCounters: {
      topicCreated: counterMap(metricRows, METRIC_NAME.TOPIC_CREATED_TOTAL, ["topic"]),
      acks: counterMap(metricRows, METRIC_NAME.MESSAGE_ACKS_TOTAL, ["topic", "group"]),
      nacks: counterMap(metricRows, METRIC_NAME.MESSAGE_NACKS_TOTAL, ["topic", "group", "reason"]),
      leaseTimeouts: counterMap(metricRows, METRIC_NAME.MESSAGE_LEASE_TIMEOUTS_TOTAL, ["topic", "group"]),
      redeliveries: counterMap(metricRows, METRIC_NAME.MESSAGE_REDELIVERIES_TOTAL, ["topic", "group", "cause"]),
      dlq: counterMap(metricRows, METRIC_NAME.DLQ_MESSAGES_TOTAL, ["topic", "reason"])
    }
  };
}
