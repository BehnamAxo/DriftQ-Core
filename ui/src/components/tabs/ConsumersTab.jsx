import { fmt } from "../../utils/number";

export default function ConsumersTab({ consumers }) {
  return (
    <section className="dq-grid">
      {consumers.map((c) => (
        <div className="dq-panel" key={c.group}>
          <div className="row">
            <strong>{c.group}</strong>
            <span className={c.status === "connected" ? "green" : "dim"}>{c.status}</span>
          </div>
          <div className="tags">
            {c.topics.map((t) => (
              <span key={t}>{t}</span>
            ))}
            {c.topics.length === 0 ? <span>no topics</span> : null}
          </div>
          <div className="mini-grid">
            <div>
              <b>{fmt(c.activeLease)}</b>
              <small>active leases</small>
            </div>
            <div>
              <b>{fmt(c.totalAcked)}</b>
              <small>acked offset</small>
            </div>
            <div>
              <b>{fmt(c.totalNacked)}</b>
              <small>nacked</small>
            </div>
          </div>
        </div>
      ))}
    </section>
  );
}
