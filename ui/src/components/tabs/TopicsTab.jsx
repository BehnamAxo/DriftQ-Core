import Sparkline from "../Sparkline";
import { fmt } from "../../utils/number";

export default function TopicsTab({ topics, spark }) {
  return (
    <section className="dq-grid">
      {
        topics.map((t) => (
          <div className="dq-panel topic" key={t.name}>
            <div className="row">
              <strong>{t.name}</strong>
              <span>{t.partitions}P</span>
            </div>
            <Sparkline values={spark[t.name]} />
            <div className="mini-grid">
              <div>
                <b>{fmt(t.produced)}</b>
                <small>produced</small>
              </div>

              <div>
                <b>{fmt(t.consumed)}</b>
                <small>consumed</small>
              </div>

              <div>
                <b>{fmt(t.inflight)}</b>
                <small>inflight</small>
              </div>

              <div>
                <b>{fmt(t.dlq)}</b>
                <small>DLQ</small>
              </div>
            </div>
          </div>
        ))
      }
    </section>
  );
}
