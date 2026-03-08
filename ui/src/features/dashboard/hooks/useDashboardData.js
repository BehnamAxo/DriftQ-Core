import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { getJSON, getText } from "../../../utils/http";
import { metricsByName, parsePrometheus } from "../../../utils/metrics";
import { safeNum } from "../../../utils/number";
import { nowClock } from "../../../utils/time";
import { normalizeRun } from "../../workflows/utils/workflow";
import { loadConsumerLagDetails, buildConsumers } from "../utils/consumers";
import { buildDashboardEvents } from "../utils/events";
import { buildTopics } from "../utils/topics";

function normalizeRefreshError(err) {
  const message = String(err?.message || "").trim();
  if (!message) {
    return "refresh failed";
  }

  if (message === "Failed to fetch" || message.includes("NetworkError")) {
    return "Disconnected from broker. Retrying...";
  }

  return message;
}

function summarizeRefreshErrors(errors) {
  const unique = Array.from(new Set(errors.filter(Boolean).map(normalizeRefreshError)));
  if (!unique.length) {
    return "";
  }

  if (unique.length === 1) {
    return unique[0];
  }

  return unique.join(" | ");
}

export function useDashboardData(activeTab) {
  const [group, setGroup] = useState("bench");
  const [health, setHealth] = useState("unknown");
  const [version, setVersion] = useState({ version: "unknown", commit: "unknown", wal_enabled: false });
  const [config, setConfig] = useState({
    addr: "",
    wal_path: "",
    access_log: false,
    engine_store: "unknown",
    engine_wal: "unknown",
    artifacts_dir: "",
    log_level: "unknown",
    log_format: "unknown",
    max_partition_bytes: 0,
    max_partition_msgs: 0,
    max_inflight: 0,
    wal_sync_interval: "",
    wal_buffer_bytes: 0
  });
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
    inflightByTopic: {},
    eventCounters: {
      topicCreated: {},
      acks: {},
      nacks: {},
      leaseTimeouts: {},
      redeliveries: {},
      dlq: {}
    },
    totals: { produced: 0, consumed: 0, inflight: 0, dlq: 0, runCount: 0 }
  });

  const refresh = useCallback(
    async (signal) => {
      const selectedGroup = group.trim() || "bench";
      const errors = [];
      const results = await Promise.allSettled([
        getJSON("/v1/healthz", signal),
        getJSON("/v1/version", signal),
        getJSON("/v1/config", signal),
        getJSON("/debug/topics", signal),
        getJSON(`/debug/topics/lag?group=${encodeURIComponent(selectedGroup)}`, signal),
        getText("/metrics", signal),
        getJSON("/debug/runs?limit=10", signal)
      ]);
      const [healthRes, versionRes, configRes, topicsRes, lagRes, metricsRes, runsRes] = results;
      const successCount = results.filter((result) => result.status === "fulfilled").length;

      if (healthRes.status === "fulfilled") {
        setHealth(healthRes.value.status || "unknown");
      } else {
        errors.push(healthRes.reason?.message || "health failed");
        setHealth(successCount === 0 ? "disconnected" : "degraded");
      }

      if (versionRes.status === "fulfilled") {
        setVersion(versionRes.value);
      } else {
        errors.push(versionRes.reason?.message || "version failed");
      }

      if (configRes.status === "fulfilled") {
        setConfig(configRes.value);
      } else {
        errors.push(configRes.reason?.message || "config failed");
      }

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
        .map((row) => ({ reason: row.labels.reason || "unknown", value: safeNum(row.value) }))
        .sort((a, b) => b.value - a.value);
      setProducerReasons(rejected);

      const dlqByTopic = new Map();
      for (const row of metricsByName(metricRows, "dlq_messages_total")) {
        const topic = row.labels.topic || "";
        dlqByTopic.set(topic, (dlqByTopic.get(topic) || 0) + safeNum(row.value));
      }

      const nowMs = Date.now();
      const prev = prevRef.current;
      const { nextTopics, nextSpark } = buildTopics(topicRows, selectedLagRows, dlqByTopic, prev, sparkRef, nowMs);
      setTopics(nextTopics);
      setSpark(nextSpark);
      sparkRef.current = nextSpark;

      const { groupList, lagDetails } = await loadConsumerLagDetails(metricRows, selectedGroup, signal, getJSON);
      setConsumers(buildConsumers(groupList, lagDetails));

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

      const dlqTopics = topicRows.map((topic) => topic.topic).filter((topic) => topic.startsWith("dlq."));
      const effectiveDlq = dlqTopic || dlqTopics[0] || "";
      if (effectiveDlq !== dlqTopic) {
        setDlqTopic(effectiveDlq);
      }

      if (activeTab === "Dead Letters" && effectiveDlq) {
        try {
          const payload = await getJSON(`/debug/topics/peek?topic=${encodeURIComponent(effectiveDlq)}&limit=50`, signal);
          const messages = Array.isArray(payload.messages) ? payload.messages : [];

          setDlqMessages(
            messages.map((message) => {
              const dlqMeta = message.envelope?.dlq || {};
              const routedMs = safeNum(dlqMeta.routed_at_ms);
              return {
                id: `${message.topic}:${message.partition}:${message.offset}`,
                topic: message.topic || effectiveDlq,
                originalTopic: dlqMeta.original_topic || "",
                originalPartition: safeNum(dlqMeta.original_partition),
                originalOffset: safeNum(dlqMeta.original_offset),
                reason: dlqMeta.last_error || message.last_error || "unknown",
                retries: safeNum(dlqMeta.attempts || message.attempts),
                failedAt: routedMs > 0 ? new Date(routedMs).toISOString() : "",
                key: message.key || "",
                value: message.value || "",
                envelope: message.envelope || null,
                routing: message.routing || null
              };
            })
          );
        } catch (err) {
          errors.push(err?.message || "dlq peek failed");
          setDlqMessages([]);
        }
      } else if (!effectiveDlq) {
        setDlqMessages([]);
      }

      const totals = {
        produced: nextTopics.reduce((sum, topic) => sum + safeNum(topic.produced), 0),
        consumed: nextTopics.reduce((sum, topic) => sum + safeNum(topic.consumed), 0),
        inflight: nextTopics.reduce((sum, topic) => sum + safeNum(topic.inflight), 0),
        dlq: nextTopics.reduce((sum, topic) => sum + safeNum(topic.dlq), 0),
        runCount: runIDs.length
      };

      const ts = nowClock();
      const prevTotals = prev.totals;
      const { deltaEvents, eventCounters } = buildDashboardEvents({
        prev,
        nextTopics,
        metricRows,
        totals,
        prevTotals,
        ts,
        nowMs,
        selectedGroup
      });

      setEvents((prevEvents) => [...deltaEvents, ...prevEvents].slice(0, 40));

      prevRef.current = {
        at: nowMs,
        producedByTopic: Object.fromEntries(nextTopics.map((topic) => [topic.name, topic.produced])),
        consumedByTopic: Object.fromEntries(nextTopics.map((topic) => [topic.name, topic.consumed])),
        inflightByTopic: Object.fromEntries(nextTopics.map((topic) => [topic.name, topic.inflight])),
        eventCounters,
        totals
      };

      setUpdatedAt(ts);
      setTick((value) => value + 1);
      setError(summarizeRefreshErrors(errors));
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

  const totalProduced = useMemo(() => topics.reduce((sum, topic) => sum + safeNum(topic.produced), 0), [topics]);
  const totalConsumed = useMemo(() => topics.reduce((sum, topic) => sum + safeNum(topic.consumed), 0), [topics]);
  const totalInflight = useMemo(() => topics.reduce((sum, topic) => sum + safeNum(topic.inflight), 0), [topics]);
  const totalDLQ = useMemo(() => topics.reduce((sum, topic) => sum + safeNum(topic.dlq), 0), [topics]);
  const totalRejected = useMemo(() => producerReasons.reduce((sum, reason) => sum + safeNum(reason.value), 0), [producerReasons]);

  return {
    consumers,
    config,
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
