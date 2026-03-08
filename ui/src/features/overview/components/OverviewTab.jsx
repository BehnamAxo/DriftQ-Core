import Sparkline from "../../shared/components/Sparkline";
import { COMMON_TEXT, OVERVIEW_COPY } from "../../../constants/ui";
import { fmt } from "../../../utils/number";

function formatBytes(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) {
    return `0 ${OVERVIEW_COPY.BYTE_UNITS[0]}`;
  }

  const units = OVERVIEW_COPY.BYTE_UNITS;
  let size = n;
  let index = 0;
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index += 1;
  }

  const digits = size >= 10 || index === 0 ? 0 : 1;
  return `${size.toFixed(digits)} ${units[index]}`;
}

function snapshotRows(config) {
  return [
    {
      label: OVERVIEW_COPY.SNAPSHOT_ROWS.WAL_PATH_LABEL,
      value: config.wal_path || COMMON_TEXT.NOT_AVAILABLE,
      description: OVERVIEW_COPY.SNAPSHOT_ROWS.WAL_PATH_DESCRIPTION,
      tooltip: OVERVIEW_COPY.SNAPSHOT_ROWS.WAL_PATH_TOOLTIP
    },
    {
      label: OVERVIEW_COPY.SNAPSHOT_ROWS.WAL_ENGINE_LABEL,
      value: config.engine_wal || COMMON_TEXT.UNKNOWN,
      description: OVERVIEW_COPY.SNAPSHOT_ROWS.WAL_ENGINE_DESCRIPTION,
      tooltip: OVERVIEW_COPY.SNAPSHOT_ROWS.WAL_ENGINE_TOOLTIP
    },
    {
      label: OVERVIEW_COPY.SNAPSHOT_ROWS.LOG_MODE_LABEL,
      value: `${config.log_level || COMMON_TEXT.UNKNOWN} / ${config.log_format || COMMON_TEXT.UNKNOWN}`,
      description: OVERVIEW_COPY.SNAPSHOT_ROWS.LOG_MODE_DESCRIPTION,
      tooltip: OVERVIEW_COPY.SNAPSHOT_ROWS.LOG_MODE_TOOLTIP
    },
    {
      label: OVERVIEW_COPY.SNAPSHOT_ROWS.ACCESS_LOG_LABEL,
      value: config.access_log ? OVERVIEW_COPY.SNAPSHOT_ROWS.ACCESS_LOG_ENABLED : OVERVIEW_COPY.SNAPSHOT_ROWS.ACCESS_LOG_DISABLED,
      description: OVERVIEW_COPY.SNAPSHOT_ROWS.ACCESS_LOG_DESCRIPTION,
      tooltip: OVERVIEW_COPY.SNAPSHOT_ROWS.ACCESS_LOG_TOOLTIP
    },
    {
      label: OVERVIEW_COPY.SNAPSHOT_ROWS.WAL_SYNC_LABEL,
      value: config.wal_sync_interval || COMMON_TEXT.NOT_AVAILABLE,
      description: OVERVIEW_COPY.SNAPSHOT_ROWS.WAL_SYNC_DESCRIPTION,
      tooltip: OVERVIEW_COPY.SNAPSHOT_ROWS.WAL_SYNC_TOOLTIP
    },
    {
      label: OVERVIEW_COPY.SNAPSHOT_ROWS.WAL_BUFFER_LABEL,
      value: formatBytes(config.wal_buffer_bytes),
      description: OVERVIEW_COPY.SNAPSHOT_ROWS.WAL_BUFFER_DESCRIPTION,
      tooltip: OVERVIEW_COPY.SNAPSHOT_ROWS.WAL_BUFFER_TOOLTIP
    },
    {
      label: OVERVIEW_COPY.SNAPSHOT_ROWS.ARTIFACTS_DIR_LABEL,
      value: config.artifacts_dir || COMMON_TEXT.NOT_AVAILABLE,
      description: OVERVIEW_COPY.SNAPSHOT_ROWS.ARTIFACTS_DIR_DESCRIPTION,
      tooltip: OVERVIEW_COPY.SNAPSHOT_ROWS.ARTIFACTS_DIR_TOOLTIP
    }
  ];
}

function HelpTip({ text }) {
  return (
    <span className="dq-help" tabIndex={0} aria-label={text}>
      <span className="dq-help-trigger">{OVERVIEW_COPY.HELP_TRIGGER}</span>
      <span className="dq-help-tooltip" role="tooltip">{text}</span>
    </span>
  );
}

function MetricCard({ item, secondary = false }) {
  const cardClassName = `dq-card ${secondary ? "dq-card-secondary" : "dq-card-primary"} ${item.unavailable ? "dq-card-unavailable" : ""}`;
  const valueClassName = `dq-value ${secondary ? "dq-value-secondary" : ""} ${item.tone}`;

  return (
    <div className={cardClassName}>
      <div className="dq-card-head">
        <div className={valueClassName}>{item.value}</div>
        {item.tooltip ? <HelpTip text={item.tooltip} /> : null}
      </div>
      <div className="dq-label">{item.label}</div>
      {item.description ? <p className="dq-card-note">{item.description}</p> : null}
    </div>
  );
}

function compressEvents(events) {
  const grouped = [];

  for (const event of events) {
    const previous = grouped[grouped.length - 1];
    if (
      previous &&
      event.type === "HEARTBEAT" &&
      previous.type === event.type &&
      previous.topic === event.topic &&
      previous.group === event.group &&
      previous.detail === event.detail
    ) {
      previous.count += event.count || 1;
      previous.ts = event.ts;
      continue;
    }

    grouped.push({ ...event });
  }

  return grouped;
}

function snapshotSummary(config, version) {
  return [
    {
      label: OVERVIEW_COPY.SNAPSHOT_SUMMARY.ENDPOINT_LABEL,
      description: OVERVIEW_COPY.SNAPSHOT_SUMMARY.ENDPOINT_DESCRIPTION,
      tooltip: OVERVIEW_COPY.SNAPSHOT_SUMMARY.ENDPOINT_TOOLTIP,
      value: `${OVERVIEW_COPY.SNAPSHOT_SUMMARY.ENDPOINT_VALUE_PREFIX} ${config.addr || COMMON_TEXT.UNKNOWN}`,
      meta: [
        {
          label: `${OVERVIEW_COPY.SNAPSHOT_SUMMARY.BUILD_PREFIX} ${version.version || COMMON_TEXT.UNKNOWN}`,
          detail: OVERVIEW_COPY.SNAPSHOT_SUMMARY.BUILD_DETAIL
        },
        {
          label: version.wal_enabled ? OVERVIEW_COPY.SNAPSHOT_SUMMARY.WAL_ENABLED : OVERVIEW_COPY.SNAPSHOT_SUMMARY.WAL_DISABLED,
          detail: OVERVIEW_COPY.SNAPSHOT_SUMMARY.WAL_DETAIL
        }
      ]
    },
    {
      label: OVERVIEW_COPY.SNAPSHOT_SUMMARY.STORAGE_LABEL,
      description: OVERVIEW_COPY.SNAPSHOT_SUMMARY.STORAGE_DESCRIPTION,
      tooltip: OVERVIEW_COPY.SNAPSHOT_SUMMARY.STORAGE_TOOLTIP,
      value: `${OVERVIEW_COPY.SNAPSHOT_SUMMARY.STORAGE_VALUE_PREFIX} ${config.engine_store || COMMON_TEXT.UNKNOWN}`,
      meta: [
        {
          label: `${OVERVIEW_COPY.SNAPSHOT_SUMMARY.ENGINE_WAL_PREFIX} ${config.engine_wal || COMMON_TEXT.UNKNOWN}`,
          detail: OVERVIEW_COPY.SNAPSHOT_SUMMARY.ENGINE_WAL_DETAIL
        },
        {
          label: `${OVERVIEW_COPY.SNAPSHOT_SUMMARY.ARTIFACTS_PREFIX} ${config.artifacts_dir || COMMON_TEXT.NOT_AVAILABLE}`,
          detail: OVERVIEW_COPY.SNAPSHOT_SUMMARY.ARTIFACTS_DETAIL
        }
      ]
    },
    {
      label: OVERVIEW_COPY.SNAPSHOT_SUMMARY.LIMITS_LABEL,
      description: OVERVIEW_COPY.SNAPSHOT_SUMMARY.LIMITS_DESCRIPTION,
      tooltip: OVERVIEW_COPY.SNAPSHOT_SUMMARY.LIMITS_TOOLTIP,
      value: OVERVIEW_COPY.SNAPSHOT_SUMMARY.LIMITS_VALUE_TEMPLATE(fmt(config.max_inflight)),
      meta: [
        {
          label: `${OVERVIEW_COPY.SNAPSHOT_SUMMARY.PARTITION_MSGS_PREFIX} ${fmt(config.max_partition_msgs)} ${OVERVIEW_COPY.SNAPSHOT_SUMMARY.PARTITION_MSGS_SUFFIX}`,
          detail: OVERVIEW_COPY.SNAPSHOT_SUMMARY.PARTITION_MSGS_DETAIL
        },
        {
          label: `${OVERVIEW_COPY.SNAPSHOT_SUMMARY.PARTITION_MSGS_PREFIX} ${formatBytes(config.max_partition_bytes)}`,
          detail: OVERVIEW_COPY.SNAPSHOT_SUMMARY.PARTITION_BYTES_DETAIL
        }
      ]
    },
    {
      label: OVERVIEW_COPY.SNAPSHOT_SUMMARY.RUNTIME_LABEL,
      description: OVERVIEW_COPY.SNAPSHOT_SUMMARY.RUNTIME_DESCRIPTION,
      tooltip: OVERVIEW_COPY.SNAPSHOT_SUMMARY.RUNTIME_TOOLTIP,
      value: OVERVIEW_COPY.SNAPSHOT_SUMMARY.RUNTIME_VALUE_TEMPLATE(
        config.log_level || COMMON_TEXT.UNKNOWN,
        config.log_format || COMMON_TEXT.UNKNOWN
      ),
      meta: [
        {
          label: config.access_log ? OVERVIEW_COPY.SNAPSHOT_SUMMARY.ACCESS_LOG_ENABLED : OVERVIEW_COPY.SNAPSHOT_SUMMARY.ACCESS_LOG_DISABLED,
          detail: OVERVIEW_COPY.SNAPSHOT_SUMMARY.ACCESS_LOG_DETAIL
        },
        {
          label: `${OVERVIEW_COPY.SNAPSHOT_SUMMARY.WAL_FLUSH_PREFIX} ${config.wal_sync_interval || COMMON_TEXT.NOT_AVAILABLE}`,
          detail: OVERVIEW_COPY.SNAPSHOT_SUMMARY.WAL_FLUSH_DETAIL
        }
      ]
    }
  ];
}

export default function OverviewTab({
  config,
  version,
  totalProduced,
  totalConsumed,
  totalInflight,
  totalDLQ,
  consumersCount,
  totalRejected,
  topics,
  spark,
  events
}) {
  const primaryMetrics = [
    { label: OVERVIEW_COPY.PRIMARY_METRICS.PRODUCED, value: fmt(totalProduced), tone: "green" },
    { label: OVERVIEW_COPY.PRIMARY_METRICS.CONSUMED, value: fmt(totalConsumed), tone: "blue" },
    { label: OVERVIEW_COPY.PRIMARY_METRICS.IN_FLIGHT, value: fmt(totalInflight), tone: "amber" },
    { label: OVERVIEW_COPY.PRIMARY_METRICS.DEAD_LETTERS, value: fmt(totalDLQ), tone: "red" }
  ];

  const secondaryMetrics = [
    {
      label: OVERVIEW_COPY.SECONDARY_METRICS.ACTIVE_PRODUCERS,
      value: OVERVIEW_COPY.SECONDARY_METRICS.NOT_TRACKED,
      tone: "muted",
      unavailable: true,
      description: OVERVIEW_COPY.SECONDARY_METRICS.ACTIVE_PRODUCERS_DESCRIPTION,
      tooltip: OVERVIEW_COPY.SECONDARY_METRICS.ACTIVE_PRODUCERS_TOOLTIP
    },
    {
      label: OVERVIEW_COPY.SECONDARY_METRICS.CONSUMER_GROUPS,
      value: fmt(consumersCount),
      tone: "green2",
      description: OVERVIEW_COPY.SECONDARY_METRICS.CONSUMER_GROUPS_DESCRIPTION
    },
    {
      label: OVERVIEW_COPY.SECONDARY_METRICS.DEDUPLICATED,
      value: OVERVIEW_COPY.SECONDARY_METRICS.NOT_TRACKED,
      tone: "muted",
      unavailable: true,
      description: OVERVIEW_COPY.SECONDARY_METRICS.DEDUPLICATED_DESCRIPTION,
      tooltip: OVERVIEW_COPY.SECONDARY_METRICS.DEDUPLICATED_TOOLTIP
    },
    {
      label: OVERVIEW_COPY.SECONDARY_METRICS.BACKPRESSURE_REJECTED,
      value: fmt(totalRejected),
      tone: totalRejected > 20 ? "red" : "amber",
      description: OVERVIEW_COPY.SECONDARY_METRICS.BACKPRESSURE_REJECTED_DESCRIPTION
    }
  ];

  const recentEvents = compressEvents(events.slice(0, 20));
  const eventStreamIdle = recentEvents.length === 0 || recentEvents.every((event) => event.type === "HEARTBEAT");

  return (
    <>
      <section className="dq-metrics">
        {primaryMetrics.map((item) => (
          <MetricCard item={item} key={item.label} />
        ))}
      </section>

      <section className="dq-metrics dq-metrics-secondary">
        {secondaryMetrics.map((item) => (
          <MetricCard item={item} secondary key={item.label} />
        ))}
      </section>

      <section className="dq-split">
        <div className="dq-panel dq-topic-throughput-panel">
          <h3>{OVERVIEW_COPY.TOPIC_THROUGHPUT}</h3>
          <div className="dq-topic-throughput-scroll">
            <table>
              <thead>
                <tr>
                  <th>{OVERVIEW_COPY.TABLE_HEADERS.TOPIC}</th>
                  <th className="right">{OVERVIEW_COPY.TABLE_HEADERS.RATE}</th>
                  <th className="right">{OVERVIEW_COPY.TABLE_HEADERS.LAG}</th>
                  <th>{OVERVIEW_COPY.TABLE_HEADERS.TREND}</th>
                </tr>
              </thead>
              <tbody>
                {
                  topics.map((t) => (
                    <tr key={t.name}>
                      <td>{t.name}</td>
                      <td className="right">
                        <span className="green">^ {t.rateIn}</span> <span className="blue">v {t.rateOut}</span>
                      </td>
                      <td className={`right ${t.lag > 100 ? "red" : t.lag > 30 ? "amber" : "green2"}`}>{fmt(t.lag)}</td>
                      <td>
                        <Sparkline values={spark[t.name]} />
                      </td>
                    </tr>
                  ))
                }
              </tbody>
            </table>
          </div>
        </div>
        <div className="dq-panel">
          <h3>{OVERVIEW_COPY.LIVE_EVENT_STREAM}</h3>
          {eventStreamIdle ? <p className="dq-note">{OVERVIEW_COPY.EVENTS_IDLE_NOTE}</p> : null}
          <div className="dq-events">
            {
              recentEvents.map((e) => (
                <div className="dq-event" key={e.id}>
                  <span className="ts">{e.ts}</span>
                  <span className="badge" style={{ borderColor: `${e.color}66`, color: e.color }}>
                    {e.type}
                  </span>
                  <span>{e.topic}{e.count > 1 ? ` x${e.count}` : ""}</span>
                  <span className="dim">{[e.group, e.detail].filter(Boolean).join(" | ")}</span>
                </div>
              ))
            }
          </div>
        </div>
      </section>

      <section className="dq-overview-grid">
        <div className="dq-panel">
          <h3>{OVERVIEW_COPY.BROKER_SNAPSHOT}</h3>
          <p className="dq-note">
            {OVERVIEW_COPY.SNAPSHOT_NOTE}
          </p>
          <div className="dq-broker-snapshot">
            {
              snapshotSummary(config, version).map((item) => (
                <div className="dq-summary-card" key={item.label}>
                  <div className="dq-summary-head">
                    <span className="dq-kv-label">{item.label}</span>
                    <HelpTip text={item.tooltip} />
                  </div>
                  <strong className="dq-summary-value">{item.value}</strong>
                  <p className="dq-summary-description">{item.description}</p>
                  <div className="dq-summary-meta">
                    {
                      item.meta.map((meta) => (
                        <span className="dq-chip" key={`${item.label}-${meta.label}`} title={meta.detail}>
                          {meta.label}
                        </span>
                      ))
                    }
                  </div>
                </div>
              ))
            }
          </div>

          <details className="dq-disclosure">
            <summary>{OVERVIEW_COPY.ADVANCED_CONFIG}</summary>
            <p className="dq-note dq-note-tight">
              {OVERVIEW_COPY.ADVANCED_CONFIG_NOTE}
            </p>
            <div className="dq-kv-grid dq-kv-grid-compact">
              {
                snapshotRows(config).map((item) => (
                  <div className="dq-kv-row dq-kv-row-detailed" key={item.label}>
                    <div className="dq-summary-head">
                      <span className="dq-kv-label">{item.label}</span>
                      <HelpTip text={item.tooltip} />
                    </div>
                    <span className="dq-kv-value">{item.value}</span>
                    <p className="dq-kv-description">{item.description}</p>
                  </div>
                ))
              }
            </div>
          </details>
        </div>
      </section>
    </>
  );
}
