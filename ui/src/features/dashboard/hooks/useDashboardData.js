import {
  API_PATHS,
  APP_TAB,
  COMMON_TEXT,
  DASHBOARD_COPY,
  DEFAULTS,
  HEALTH_STATUS,
  METRIC_NAME,
  PROMISE_STATUS,
  TOPIC_PREFIX,
  UI_LIMITS
} from "../../../constants/ui";
import { buildDashboardEvents } from "../utils/events";
import { buildTopics } from "../utils/topics";
import { getJSON, getText } from "../../../utils/http";
import { loadConsumerLagDetails, buildConsumers } from "../utils/consumers";
import { metricsByName, parsePrometheus } from "../../../utils/metrics";
import { normalizeRun } from "../../workflows/utils/workflow";
import { nowClock } from "../../../utils/time";
import { safeNum } from "../../../utils/number";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

function normalizeRefreshError(err) {
  const message = String(err?.message || COMMON_TEXT.EMPTY).trim();
  if (!message) {
    return DASHBOARD_COPY.REFRESH_FAILED;
  }

  if (message === DASHBOARD_COPY.NETWORK_FETCH_FAILED || message.includes(DASHBOARD_COPY.NETWORK_ERROR_TOKEN)) {
    return DASHBOARD_COPY.DISCONNECTED_RETRYING;
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
  const [group, setGroup] = useState(DEFAULTS.GROUP);
  const [health, setHealth] = useState(HEALTH_STATUS.UNKNOWN);
  const [version, setVersion] = useState(DEFAULTS.VERSION);
  const [config, setConfig] = useState(DEFAULTS.CONFIG);
  const [updatedAt, setUpdatedAt] = useState(DEFAULTS.UPDATED_AT);
  const [error, setError] = useState(COMMON_TEXT.EMPTY);
  const [loading, setLoading] = useState(true);
  const [tick, setTick] = useState(0);

  const [topics, setTopics] = useState([]);
  const [spark, setSpark] = useState({});
  const sparkRef = useRef({});

  const [producerReasons, setProducerReasons] = useState([]);
  const [consumers, setConsumers] = useState([]);
  const [events, setEvents] = useState([]);
  const [runs, setRuns] = useState([]);

  const [dlqTopic, setDlqTopic] = useState(COMMON_TEXT.EMPTY);
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
      const selectedGroup = group.trim() || DEFAULTS.GROUP;
      const errors = [];
      const results = await Promise.allSettled([
        getJSON(API_PATHS.HEALTH, signal),
        getJSON(API_PATHS.VERSION, signal),
        getJSON(API_PATHS.CONFIG, signal),
        getJSON(API_PATHS.TOPICS, signal),
        getJSON(API_PATHS.topicLag(selectedGroup), signal),
        getText(API_PATHS.METRICS, signal),
        getJSON(API_PATHS.runs(UI_LIMITS.RUN_LIST_LIMIT), signal)
      ]);
      const [healthRes, versionRes, configRes, topicsRes, lagRes, metricsRes, runsRes] = results;
      const successCount = results.filter((result) => result.status === PROMISE_STATUS.FULFILLED).length;

      if (healthRes.status === PROMISE_STATUS.FULFILLED) {
        setHealth(healthRes.value.status || HEALTH_STATUS.UNKNOWN);
      } else {
        errors.push(healthRes.reason?.message || DASHBOARD_COPY.HEALTH_FAILED);
        setHealth(successCount === 0 ? HEALTH_STATUS.DISCONNECTED : HEALTH_STATUS.DEGRADED);
      }

      if (versionRes.status === PROMISE_STATUS.FULFILLED) {
        setVersion(versionRes.value);
      } else {
        errors.push(versionRes.reason?.message || DASHBOARD_COPY.VERSION_FAILED);
      }

      if (configRes.status === PROMISE_STATUS.FULFILLED) {
        setConfig(configRes.value);
      } else {
        errors.push(configRes.reason?.message || DASHBOARD_COPY.CONFIG_FAILED);
      }

      const topicRows = topicsRes.status === PROMISE_STATUS.FULFILLED ? topicsRes.value.topics || [] : [];
      if (topicsRes.status !== PROMISE_STATUS.FULFILLED) {
        errors.push(topicsRes.reason?.message || DASHBOARD_COPY.TOPICS_FAILED);
      }

      const selectedLagRows = lagRes.status === PROMISE_STATUS.FULFILLED ? lagRes.value.rows || [] : [];
      if (lagRes.status !== PROMISE_STATUS.FULFILLED) {
        errors.push(lagRes.reason?.message || DASHBOARD_COPY.LAG_FAILED);
      }

      const metricRows = metricsRes.status === PROMISE_STATUS.FULFILLED ? parsePrometheus(metricsRes.value) : [];
      if (metricsRes.status !== PROMISE_STATUS.FULFILLED) {
        errors.push(metricsRes.reason?.message || DASHBOARD_COPY.METRICS_FAILED);
      }

      const rejected = metricsByName(metricRows, METRIC_NAME.PRODUCE_REJECTED_TOTAL)
        .map((row) => ({ reason: row.labels.reason || COMMON_TEXT.UNKNOWN, value: safeNum(row.value) }))
        .sort((a, b) => b.value - a.value);
      setProducerReasons(rejected);

      const dlqByTopic = new Map();
      for (const row of metricsByName(metricRows, METRIC_NAME.DLQ_MESSAGES_TOTAL)) {
        const topic = row.labels.topic || COMMON_TEXT.EMPTY;
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

      const runIDs = runsRes.status === PROMISE_STATUS.FULFILLED ? runsRes.value.runs || [] : [];
      if (runsRes.status !== PROMISE_STATUS.FULFILLED) {
        errors.push(runsRes.reason?.message || DASHBOARD_COPY.RUNS_FAILED);
      }

      if (activeTab === APP_TAB.WORKFLOWS || activeTab === APP_TAB.OVERVIEW) {
        const details = await Promise.all(
          runIDs.slice(0, UI_LIMITS.WORKFLOW_PREVIEW_RUNS).map(async (id) => {
            try {
              return await getJSON(API_PATHS.runDetail(id), signal);
            } catch {
              return null;
            }
          })
        );
        setRuns(details.map(normalizeRun).filter(Boolean));
      }

      const dlqTopics = topicRows.map((topic) => topic.topic).filter((topic) => topic.startsWith(TOPIC_PREFIX.DLQ));
      const effectiveDlq = dlqTopic || dlqTopics[0] || COMMON_TEXT.EMPTY;
      if (effectiveDlq !== dlqTopic) {
        setDlqTopic(effectiveDlq);
      }

      if (activeTab === APP_TAB.DEAD_LETTERS && effectiveDlq) {
        try {
          const payload = await getJSON(API_PATHS.topicPeek(effectiveDlq, UI_LIMITS.DLQ_PEEK_LIMIT), signal);
          const messages = Array.isArray(payload.messages) ? payload.messages : [];

          setDlqMessages(
            messages.map((message) => {
              const dlqMeta = message.envelope?.dlq || {};
              const routedMs = safeNum(dlqMeta.routed_at_ms);
              return {
                id: `${message.topic}:${message.partition}:${message.offset}`,
                topic: message.topic || effectiveDlq,
                originalTopic: dlqMeta.original_topic || COMMON_TEXT.EMPTY,
                originalPartition: safeNum(dlqMeta.original_partition),
                originalOffset: safeNum(dlqMeta.original_offset),
                reason: dlqMeta.last_error || message.last_error || COMMON_TEXT.UNKNOWN,
                retries: safeNum(dlqMeta.attempts || message.attempts),
                failedAt: routedMs > 0 ? new Date(routedMs).toISOString() : COMMON_TEXT.EMPTY,
                key: message.key || COMMON_TEXT.EMPTY,
                value: message.value || COMMON_TEXT.EMPTY,
                envelope: message.envelope || null,
                routing: message.routing || null
              };
            })
          );
        } catch (err) {
          errors.push(err?.message || DASHBOARD_COPY.DLQ_PEEK_FAILED);
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
    }, UI_LIMITS.REFRESH_INTERVAL_MS);

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
