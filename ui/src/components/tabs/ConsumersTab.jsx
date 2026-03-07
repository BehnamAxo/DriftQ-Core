import { useMemo, useState } from "react";
import { fmt } from "../../utils/number";

export default function ConsumersTab({ consumers }) {
  const [selectedGroup, setSelectedGroup] = useState("");

  const activeGroup = useMemo(() => {
    if (!consumers.length) {
      return null;
    }

    if (!selectedGroup) {
      return consumers[0];
    }

    return consumers.find((consumer) => consumer.group === selectedGroup) || consumers[0];
  }, [consumers, selectedGroup]);

  return (
    <div className="dq-stack">
      <section className="dq-grid">
        {consumers.map((c) => (
          <div className={`dq-panel ${activeGroup?.group === c.group ? "selected" : ""}`} key={c.group}>
            <div className="row">
              <strong>{c.group}</strong>
              <span className={c.status === "connected" ? "green" : c.status === "backlog" ? "amber" : "dim"}>{c.status}</span>
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
                <b>{fmt(c.totalLag)}</b>
                <small>lag</small>
              </div>
              <div>
                <b>{fmt(c.partitions)}</b>
                <small>partitions</small>
              </div>
              <div>
                <b>{fmt(c.totalAcked)}</b>
                <small>acked offset</small>
              </div>
            </div>
            <div className="dq-form-actions top-gap">
              <button type="button" className="mini-btn" onClick={() => setSelectedGroup(c.group)}>
                {activeGroup?.group === c.group ? "Inspecting" : "Inspect"}
              </button>
            </div>
          </div>
        ))}
      </section>

      <section className="dq-panel">
        <div className="row">
          <div>
            <strong>Consumer Group Detail</strong>
            <div className="dim">
              {activeGroup ? `Deep view for ${activeGroup.group}.` : "No consumer groups detected yet."}
            </div>
          </div>
        </div>

        {
          activeGroup ? (
            <div className="dq-stack">
              <p className="dq-note">
                Owner identity and last-delivery timestamps are not exposed yet. This view shows the real lag and lease state available today.
              </p>

              <div className="dq-grid compact">
                {activeGroup.topicSummaries.map((topic) => (
                  <div className="dq-panel topic" key={topic.topic}>
                    <div className="row">
                      <strong>{topic.topic}</strong>
                      <span>{fmt(topic.partitions)} partitions</span>
                    </div>
                    <div className="mini-grid">
                      <div>
                        <b>{fmt(topic.lag)}</b>
                        <small>lag</small>
                      </div>
                      <div>
                        <b>{fmt(topic.inflight)}</b>
                        <small>inflight</small>
                      </div>
                      <div>
                        <b>{fmt(topic.committed)}</b>
                        <small>committed</small>
                      </div>
                      <div>
                        <b>{fmt(topic.head)}</b>
                        <small>head</small>
                      </div>
                    </div>
                  </div>
                ))}
              </div>

              <table>
                <thead>
                  <tr>
                    <th>Topic</th>
                    <th className="right">Partition</th>
                    <th className="right">Head</th>
                    <th className="right">Committed</th>
                    <th className="right">Lag</th>
                    <th className="right">Inflight</th>
                    <th>State</th>
                  </tr>
                </thead>
                <tbody>
                  {activeGroup.rows.map((row) => (
                    <tr key={`${row.topic}:${row.partition}`}>
                      <td>{row.topic}</td>
                      <td className="right">{fmt(row.partition)}</td>
                      <td className="right">{fmt(row.headOffset)}</td>
                      <td className="right">{fmt(row.committedOffset)}</td>
                      <td className={`right ${row.lag > 0 ? "amber" : "green2"}`}>{fmt(row.lag)}</td>
                      <td className={`right ${row.inflight > 0 ? "green" : "dim"}`}>{fmt(row.inflight)}</td>
                      <td>
                        {row.inflight > 0 ? (
                          <span className="green">leased</span>
                        ) : row.lag > 0 ? (
                          <span className="amber">waiting</span>
                        ) : (
                          <span className="dim">caught up</span>
                        )}
                      </td>
                    </tr>
                  ))}
                  {
                    activeGroup.rows.length === 0 ? (
                      <tr>
                        <td colSpan={7}>no partition detail available for this consumer group</td>
                      </tr>
                    ) : null
                  }
                </tbody>
              </table>
            </div>
          ) : (
            <p className="dq-note">No consumer groups to inspect yet.</p>
          )
        }
      </section>
    </div>
  );
}
