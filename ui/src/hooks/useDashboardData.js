import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { getJSON, getText } from "../utils/http";
import { metricsByName, parsePrometheus } from "../utils/metrics";
import { safeNum } from "../utils/number";
import { nowClock } from "../utils/time";
import { normalizeRun } from "../utils/workflow";

export function useDashboardData(activeTab) {
  const [group, setGroup] = useState("bench");
  const [health, setHealth] = useState("unknown");
  const [version, setVersion] = useState({ version: "unknown", commit: "unknown", wal_enabled: false });
  const [updatedAt, setUpdatedAt] = useState("-");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [tick, setTick] = useState(0);

  const [topics, setTopics] = useState([]);
  const [spark, setSpark] = useState({});
  const sparkRef = useRef({});

  const [producerReasons, setProducerReasons] = useState([]);
  const [consumers, setConsumers] = useState([]);
  const [events, setEvents] = useState([]);
  const [runs, setRuns] = useState([]);

  const [dlqTopic, setDlqTopic] = useState("");
  const [dlqMessages, setDlqMessages] = useState([]);

  const prevRef = useRef({
    at: 0,
    producedByTopic: {},
    consumedByTopic: {},
    totals: { produced: 0, consumed: 0, inflight: 0, dlq: 0, runCount: 0 }
  });

  const refresh = useCallback(
    async (signal) => {
      const errors = [];
      const [healthRes, versionRes, topicsRes, lagRes, metricsRes, runsRes] = await Promise.allSettled([
        getJSON("/v1/healthz", signal),
        getJSON("/v1/version", signal),
        getJSON("/debug/topics", signal),
        getJSON(`/debug/topics/lag?group=${encodeURIComponent(group)}`, signal),
        getText("/metrics", signal),
        getJSON("/debug/runs?limit=10", signal)
      ]);

      if (healthRes.status === "fulfilled") setHealth(healthRes.value.status || "unknown");
      else errors.push(healthRes.reason?.message || "health failed");

      if (versionRes.status === "fulfilled") setVersion(versionRes.value);
      else errors.push(versionRes.reason?.message || "version failed");

      const topicRows = topicsRes.status === "fulfilled" ? topicsRes.value.topics || [] : [];
      if (topicsRes.status !== "fulfilled") {
        errors.push(topicsRes.reason?.message || "topics failed");
      }

      const selectedLagRows = lagRes.status === "fulfilled" ? lagRes.value.rows || [] : [];
      if (lagRes.status !== "fulfilled") {
        errors.push(lagRes.reason?.message || "lag failed");
      }

      const metricRows = metricsRes.status === "fulfilled" ? parsePrometheus(metricsRes.value) : [];
      if (metricsRes.status !== "fulfilled") {
        errors.push(metricsRes.reason?.message || "metrics failed");
      }

      const rejected = metricsByName(metricRows, "produce_rejected_total")
        .map((r) => ({ reason: r.labels.reason || "unknown", value: safeNum(r.value) }))
        .sort((a, b) => b.value - a.value);
      setProducerReasons(rejected);

      const dlqCounters = metricsByName(metricRows, "dlq_messages_total");
      const dlqByTopic = new Map();
      for (const row of dlqCounters) {
        const topic = row.labels.topic || "";
        dlqByTopic.set(topic, (dlqByTopic.get(topic) || 0) + safeNum(row.value));
      }

      const lagByTopic = new Map();
      for (const row of selectedLagRows) {
        const topic = row.topic || "unknown";
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

      const nowMs = Date.now();
      const prev = prevRef.current;
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

      setTopics(nextTopics);
      setSpark(nextSpark);
      sparkRef.current = nextSpark;

      const lagMetrics = metricsByName(metricRows, "consumer_lag");
      const inflightMetrics = metricsByName(metricRows, "inflight_messages");
      const groups = new Set([group]);
      for (const r of lagMetrics) {
        if (r.labels.group) groups.add(r.labels.group);
      }

      for (const r of inflightMetrics) {
        if (r.labels.group) groups.add(r.labels.group);
      }

      const groupList = Array.from(groups).sort();
      const lagDetails = {};
      await Promise.all(
        groupList.map(async (g) => {
          try {
            const payload = await getJSON(`/debug/topics/lag?group=${encodeURIComponent(g)}`, signal);
            lagDetails[g] = Array.isArray(payload.rows) ? payload.rows : [];
          } catch {
            lagDetails[g] = [];
          }
        })
      );

      setConsumers(
        groupList.map((g) => {
          const rows = lagDetails[g] || [];
          const topicsSet = new Set(rows.map((r) => r.topic));
          return {
            group: g,
            topics: Array.from(topicsSet),
            activeLease: rows.reduce((sum, r) => sum + safeNum(r.inflight), 0),
            totalAcked: rows.reduce((sum, r) => sum + Math.max(0, safeNum(r.committed_offset)), 0),
            totalNacked: 0,
            status: rows.some((r) => safeNum(r.inflight) > 0) ? "connected" : "idle"
          };
        })
      );

      const runIDs = runsRes.status === "fulfilled" ? runsRes.value.runs || [] : [];
      if (runsRes.status !== "fulfilled") {
        errors.push(runsRes.reason?.message || "runs failed");
      }

      if (activeTab === "Workflows (v2)" || activeTab === "Overview") {
        const details = await Promise.all(
          runIDs.slice(0, 6).map(async (id) => {
            try {
              return await getJSON(`/debug/run?run_id=${encodeURIComponent(id)}`, signal);
            } catch {
              return null;
            }
          })
        );
        setRuns(details.map(normalizeRun).filter(Boolean));
      }

      const dlqTopics = topicRows.map((t) => t.topic).filter((t) => t.startsWith("dlq."));
      const effectiveDlq = dlqTopic || dlqTopics[0] || "";
      if (effectiveDlq !== dlqTopic) {
        setDlqTopic(effectiveDlq);
      }

      if (activeTab === "Dead Letters" && effectiveDlq) {
        try {
          const payload = await getJSON(`/debug/topics/peek?topic=${encodeURIComponent(effectiveDlq)}&limit=50`, signal);
          const msgs = Array.isArray(payload.messages) ? payload.messages : [];

          setDlqMessages(
            msgs.map((m) => {
              const dlqMeta = m.envelope?.dlq || {};
              const routedMs = safeNum(dlqMeta.routed_at_ms);
              return {
                id: `${m.topic}:${m.partition}:${m.offset}`,
                topic: m.topic || effectiveDlq,
                reason: dlqMeta.last_error || m.last_error || "unknown",
                retries: safeNum(dlqMeta.attempts || m.attempts),
                failedAt: routedMs > 0 ? new Date(routedMs).toISOString() : "",
                value: m.value || ""
              };
            })
          );
        } catch (e) {
          errors.push(e?.message || "dlq peek failed");
          setDlqMessages([]);
        }
      } else if (!effectiveDlq) {
        setDlqMessages([]);
      }

      const totals = {
        produced: nextTopics.reduce((sum, t) => sum + safeNum(t.produced), 0),
        consumed: nextTopics.reduce((sum, t) => sum + safeNum(t.consumed), 0),
        inflight: nextTopics.reduce((sum, t) => sum + safeNum(t.inflight), 0),
        dlq: nextTopics.reduce((sum, t) => sum + safeNum(t.dlq), 0),
        runCount: runIDs.length
      };

      const prevTotals = prev.totals;
      const ts = nowClock();
      const deltaEvents = [];

      if (totals.produced > prevTotals.produced) {
        deltaEvents.push({ id: `p-${nowMs}`, type: "PRODUCE", color: "#00ff9d", ts, topic: "broker", group });
      }

      if (totals.consumed > prevTotals.consumed) {
        deltaEvents.push({ id: `a-${nowMs}`, type: "ACK", color: "#6ee7b7", ts, topic: "broker", group });
      }

      if (totals.inflight > prevTotals.inflight) {
        deltaEvents.push({ id: `l-${nowMs}`, type: "LEASE", color: "#818cf8", ts, topic: "broker", group });
      }

      if (totals.dlq > prevTotals.dlq) {
        deltaEvents.push({ id: `d-${nowMs}`, type: "DLQ", color: "#ef4444", ts, topic: "dlq", group: "system" });
      }

      if (totals.runCount > prevTotals.runCount) {
        deltaEvents.push({ id: `r-${nowMs}`, type: "RUN", color: "#38bdf8", ts, topic: "workflow", group: "v2" });
      }

      if (deltaEvents.length === 0) {
        deltaEvents.push({ id: `h-${nowMs}`, type: "HEARTBEAT", color: "#64748b", ts, topic: "node", group: "local" });
      }

      setEvents((prevEvents) => [...deltaEvents, ...prevEvents].slice(0, 40));

      prevRef.current = {
        at: nowMs,
        producedByTopic: Object.fromEntries(nextTopics.map((t) => [t.name, t.produced])),
        consumedByTopic: Object.fromEntries(nextTopics.map((t) => [t.name, t.consumed])),
        totals
      };

      setUpdatedAt(ts);
      setTick((t) => t + 1);
      setError(errors.join(" | "));
      setLoading(false);
    },
    [activeTab, dlqTopic, group]
  );

  useEffect(() => {
    const controller = new AbortController();
    refresh(controller.signal);
    const timer = setInterval(() => {
      const pollController = new AbortController();
      refresh(pollController.signal);
    }, 4000);

    return () => {
      controller.abort();
      clearInterval(timer);
    };
  }, [refresh]);

  const totalProduced = useMemo(() => topics.reduce((sum, t) => sum + safeNum(t.produced), 0), [topics]);
  const totalConsumed = useMemo(() => topics.reduce((sum, t) => sum + safeNum(t.consumed), 0), [topics]);
  const totalInflight = useMemo(() => topics.reduce((sum, t) => sum + safeNum(t.inflight), 0), [topics]);
  const totalDLQ = useMemo(() => topics.reduce((sum, t) => sum + safeNum(t.dlq), 0), [topics]);
  const totalRejected = useMemo(() => producerReasons.reduce((sum, r) => sum + safeNum(r.value), 0), [producerReasons]);

  return {
    consumers,
    dlqMessages,
    dlqTopic,
    error,
    events,
    group,
    health,
    loading,
    producerReasons,
    refresh,
    runs,
    setDlqTopic,
    setGroup,
    spark,
    tick,
    topics,
    totalConsumed,
    totalDLQ,
    totalInflight,
    totalProduced,
    totalRejected,
    updatedAt,
    version
  };
}
