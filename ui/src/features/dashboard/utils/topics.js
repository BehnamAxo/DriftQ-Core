import { COMMON_TEXT } from "../../../constants/ui";
import { safeNum } from "../../../utils/number";

export function buildTopics(topicRows, selectedLagRows, dlqByTopic, prev, sparkRef, nowMs) {
  const lagByTopic = new Map();
  for (const row of selectedLagRows) {
    const topic = row.topic || COMMON_TEXT.UNKNOWN;
    const current = lagByTopic.get(topic) || {
      lag: 0,
      inflight: 0,
      produced: 0,
      consumed: 0,
      partitions: new Set()
    };
    current.lag += safeNum(row.lag);
    current.inflight += safeNum(row.inflight);
    current.produced += Math.max(0, safeNum(row.head_offset));
    current.consumed += Math.max(0, safeNum(row.committed_offset));
    current.partitions.add(safeNum(row.partition));
    lagByTopic.set(topic, current);
  }

  const dt = prev.at > 0 ? Math.max(0.001, (nowMs - prev.at) / 1000) : 0;
  const nextSpark = { ...sparkRef.current };

  const nextTopics = topicRows.map((topicRow) => {
    const topic = topicRow.topic;
    const lag = lagByTopic.get(topic);
    const produced = lag ? lag.produced : safeNum(topicRow.messages);
    const consumed = lag ? lag.consumed : 0;
    const inflight = lag ? lag.inflight : 0;
    const totalLag = lag ? lag.lag : 0;
    const partitions = lag ? lag.partitions.size || 1 : 1;
    const dlq = dlqByTopic.get(topic) || 0;

    const prevProduced = safeNum(prev.producedByTopic[topic]);
    const prevConsumed = safeNum(prev.consumedByTopic[topic]);
    const rateIn = dt > 0 ? Math.max(0, (produced - prevProduced) / dt) : 0;
    const rateOut = dt > 0 ? Math.max(0, (consumed - prevConsumed) / dt) : 0;

    const history = nextSpark[topic] || [];
    nextSpark[topic] = [...history.slice(-19), rateIn];

    return {
      name: topic,
      partitions,
      produced,
      consumed,
      inflight,
      lag: totalLag,
      dlq,
      rateIn: Math.round(rateIn),
      rateOut: Math.round(rateOut)
    };
  });

  return { nextTopics, nextSpark };
}
