import Sparkline from "../Sparkline";
import { fmt } from "../../utils/number";

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

function snapshotRows(config, version) {
  return [
    ["Listen Addr", config.addr || "unknown"],
    ["WAL", version.wal_enabled ? "enabled" : "disabled"],
    ["WAL Path", config.wal_path || "n/a"],
    ["Store Engine", config.engine_store || "unknown"],
    ["WAL Engine", config.engine_wal || "unknown"],
    ["Log Mode", `${config.log_level || "unknown"} / ${config.log_format || "unknown"}`],
    ["Access Log", config.access_log ? "on" : "off"],
    ["Max In-Flight", fmt(config.max_inflight)],
    ["Max Partition Msgs", fmt(config.max_partition_msgs)],
    ["Max Partition Bytes", formatBytes(config.max_partition_bytes)],
    ["WAL Sync", config.wal_sync_interval || "n/a"],
    ["WAL Buffer", formatBytes(config.wal_buffer_bytes)],
    ["Artifacts Dir", config.artifacts_dir || "n/a"],
    ["Version", version.version || "unknown"]
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
          <div className="dq-kv-grid">
            {
              snapshotRows(config, version).map(([label, value]) => (
                <div className="dq-kv-row" key={label}>
                  <span className="dq-kv-label">{label}</span>
                  <span className="dq-kv-value">{value}</span>
                </div>
              ))
            }
          </div>
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
                  <span>{e.topic}</span>
                  <span className="dim">{e.group}</span>
                </div>
              ))
            }
          </div>
        </div>
      </section>
    </>
  );
}
