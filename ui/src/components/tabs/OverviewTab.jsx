import Sparkline from "../Sparkline";
import { fmt } from "../../utils/number";

export default function OverviewTab({
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
