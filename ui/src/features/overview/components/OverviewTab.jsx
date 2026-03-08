import Sparkline from "../../shared/components/Sparkline";
import { fmt } from "../../../utils/number";

function formatBytes(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) {
    return "0 B";
  }

  const units = ["B", "KB", "MB", "GB", "TB"];
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
      label: "WAL Path",
      value: config.wal_path || "n/a",
      description: "Filesystem location of the broker write-ahead log file.",
      tooltip: "If WAL is enabled, broker writes are appended here so they can be replayed after a restart."
    },
    {
      label: "WAL Engine",
      value: config.engine_wal || "unknown",
      description: "Durability backend used by the workflow engine side of DriftQ.",
      tooltip: "This identifies which write-ahead log implementation the workflow engine is currently using."
    },
    {
      label: "Log Mode",
      value: `${config.log_level || "unknown"} / ${config.log_format || "unknown"}`,
      description: "Current log verbosity and output format for server logs.",
      tooltip: "The first value is the log level and the second is the output format written by the server."
    },
    {
      label: "Access Log",
      value: config.access_log ? "enabled" : "disabled",
      description: "Whether HTTP requests are written to the request log.",
      tooltip: "When enabled, incoming API and UI requests are logged for debugging and audit trails."
    },
    {
      label: "WAL Sync",
      value: config.wal_sync_interval || "n/a",
      description: "How often buffered WAL data is forced to durable storage.",
      tooltip: "Shorter intervals improve durability but may reduce throughput because writes are flushed more often."
    },
    {
      label: "WAL Buffer",
      value: formatBytes(config.wal_buffer_bytes),
      description: "Amount of WAL data that can be buffered before a flush.",
      tooltip: "A larger buffer can improve throughput, but increases the amount of unwritten data held in memory."
    },
    {
      label: "Artifacts Dir",
      value: config.artifacts_dir || "n/a",
      description: "Directory where workflow artifacts and generated outputs are stored.",
      tooltip: "This is where files produced by workflow steps are persisted for later inspection or download."
    }
  ];
}

function HelpTip({ text }) {
  return (
    <span className="dq-help" tabIndex={0} aria-label={text}>
      <span className="dq-help-trigger">?</span>
      <span className="dq-help-tooltip" role="tooltip">{text}</span>
    </span>
  );
}

function snapshotSummary(config, version) {
  return [
    {
      label: "Endpoint",
      description: "Where this DriftQ node is listening for API and dashboard traffic.",
      tooltip: "This is the network address clients use to talk to the broker and open the embedded UI.",
      value: `Listening on ${config.addr || "unknown"}`,
      meta: [
        {
          label: `Build ${version.version || "unknown"}`,
          detail: "Version of the running DriftQ server build."
        },
        {
          label: version.wal_enabled ? "Durability WAL enabled" : "Durability in-memory only",
          detail: "Whether broker writes are persisted to the write-ahead log."
        }
      ]
    },
    {
      label: "Storage",
      description: "How broker and workflow state are stored while the node is running.",
      tooltip: "This summarizes the active storage engine, durability backend, and where artifacts are written.",
      value: `State stored in ${config.engine_store || "unknown"}`,
      meta: [
        {
          label: `Engine WAL ${config.engine_wal || "unknown"}`,
          detail: "Workflow engine write-ahead log backend."
        },
        {
          label: `Artifacts ${config.artifacts_dir || "n/a"}`,
          detail: "Directory used to store workflow outputs and artifacts."
        }
      ]
    },
    {
      label: "Limits",
      description: "Active safety caps that bound how much work the broker will accept at once.",
      tooltip: "These caps prevent too many unacked leases or oversized partitions from overwhelming the broker.",
      value: `Allows ${fmt(config.max_inflight)} unacked messages in flight`,
      meta: [
        {
          label: `Partition cap ${fmt(config.max_partition_msgs)} msgs`,
          detail: "Maximum number of messages allowed in a single partition."
        },
        {
          label: `Partition cap ${formatBytes(config.max_partition_bytes)}`,
          detail: "Maximum total bytes allowed in a single partition."
        }
      ]
    },
    {
      label: "Runtime",
      description: "How the server logs activity and flushes durable writes while it is running.",
      tooltip: "This combines the current log verbosity, output format, request logging, and WAL sync cadence.",
      value: `Logs at ${config.log_level || "unknown"} level in ${config.log_format || "unknown"} format`,
      meta: [
        {
          label: config.access_log ? "HTTP access logging enabled" : "HTTP access logging disabled",
          detail: "Whether incoming HTTP requests are written to the access log."
        },
        {
          label: `WAL flush interval ${config.wal_sync_interval || "n/a"}`,
          detail: "How often buffered WAL writes are forced to durable storage."
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
  return (
    <>
      <section className="dq-metrics">
        {[
          ["Messages Produced", fmt(totalProduced), "green"],
          ["Messages Consumed", fmt(totalConsumed), "blue"],
          ["In-Flight", fmt(totalInflight), "amber"],
          ["Dead Letters", fmt(totalDLQ), "red"],
          ["Active Producers", "N/A", "muted"],
          ["Consumer Groups", fmt(consumersCount), "green2"],
          ["Deduplicated", "N/A", "muted"],
          ["Backpressure Rejected", fmt(totalRejected), totalRejected > 20 ? "red" : "amber"]
        ].map(([label, value, tone]) => (
          <div className="dq-card" key={label}>
            <div className={`dq-value ${tone}`}>{value}</div>
            <div className="dq-label">{label}</div>
          </div>
        ))}
      </section>

      <section className="dq-overview-grid">
        <div className="dq-panel">
          <h3>Broker Snapshot</h3>
          <p className="dq-note">
            Quick read of where this node is listening, how it stores state, and which runtime limits are active.
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
            <summary>Advanced Broker Config</summary>
            <p className="dq-note dq-note-tight">
              Lower-frequency broker settings for durability, logging, and artifact storage.
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

      <section className="dq-split">
        <div className="dq-panel">
          <h3>Topic Throughput</h3>
          <table>
            <thead>
              <tr>
                <th>Topic</th>
                <th className="right">Rate</th>
                <th className="right">Lag</th>
                <th>Trend</th>
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
        <div className="dq-panel">
          <h3>Live Event Stream</h3>
          <div className="dq-events">
            {
              events.slice(0, 20).map((e) => (
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
    </>
  );
}
