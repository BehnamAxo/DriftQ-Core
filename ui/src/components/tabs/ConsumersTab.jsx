import { useEffect, useMemo, useState } from "react";
import { fmt } from "../../utils/number";
import { postJSON, readFirstNDJSON } from "../../utils/http";

function parseMessageValue(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return { raw };
  }
}

export default function ConsumersTab({ consumers, onConsumerChanged }) {
  const [selectedGroup, setSelectedGroup] = useState("");
  const [consumeTopic, setConsumeTopic] = useState("");
  const [owner, setOwner] = useState("debug-ui");
  const [leaseMs, setLeaseMs] = useState("10000");
  const [nackReason, setNackReason] = useState("debug reject");
  const [pendingMessage, setPendingMessage] = useState(null);
  const [actionError, setActionError] = useState("");
  const [actionSuccess, setActionSuccess] = useState("");
  const [consuming, setConsuming] = useState(false);
  const [acking, setAcking] = useState(false);
  const [nacking, setNacking] = useState(false);

  const activeGroup = useMemo(() => {
    if (!consumers.length) {
      return null;
    }

    if (!selectedGroup) {
      return consumers[0];
    }

    return consumers.find((consumer) => consumer.group === selectedGroup) || consumers[0];
  }, [consumers, selectedGroup]);

  useEffect(() => {
    if (!activeGroup) {
      setConsumeTopic("");
      return;
    }

    if (!consumeTopic || !activeGroup.topics.includes(consumeTopic)) {
      setConsumeTopic(activeGroup.topics[0] || "");
    }
  }, [activeGroup, consumeTopic]);

  async function handleConsume(e) {
    e.preventDefault();

    if (!activeGroup) {
      return;
    }

    const topic = consumeTopic.trim();
    const trimmedOwner = owner.trim();
    const parsedLease = Math.max(1, Number.parseInt(leaseMs, 10) || 10000);

    if (!topic) {
      setActionError("pick a topic to consume from");
      setActionSuccess("");
      return;
    }

    if (!trimmedOwner) {
      setActionError("owner is required");
      setActionSuccess("");
      return;
    }

    setConsuming(true);
    setActionError("");
    setActionSuccess("");

    try {
      const item = await readFirstNDJSON(
        `/v1/consume?topic=${encodeURIComponent(topic)}&group=${encodeURIComponent(activeGroup.group)}&owner=${encodeURIComponent(trimmedOwner)}&lease_ms=${parsedLease}`,
        { timeoutMs: Math.max(parsedLease, 4000) }
      );

      setPendingMessage({
        topic,
        group: activeGroup.group,
        owner: trimmedOwner,
        leaseMs: parsedLease,
        partition: item.partition,
        offset: item.offset,
        attempts: item.attempts,
        key: item.key || "",
        value: item.value || "",
        lastError: item.last_error || "",
        envelope: item.envelope || null,
        routing: item.routing || null
      });
      setActionSuccess(`leased offset ${item.offset} from ${topic}`);
    } catch (err) {
      setPendingMessage(null);
      setActionError(err?.message || "failed to consume message");
    } finally {
      setConsuming(false);
    }
  }

  async function handleAck() {
    if (!pendingMessage) {
      return;
    }

    setAcking(true);
    setActionError("");
    setActionSuccess("");

    try {
      await postJSON("/v1/ack", {
        topic: pendingMessage.topic,
        group: pendingMessage.group,
        owner: pendingMessage.owner,
        partition: pendingMessage.partition,
        offset: pendingMessage.offset
      });
      setActionSuccess(`acked offset ${pendingMessage.offset}`);
      setPendingMessage(null);
      await onConsumerChanged?.();
    } catch (err) {
      setActionError(err?.message || "failed to ack message");
    } finally {
      setAcking(false);
    }
  }

  async function handleNack() {
    if (!pendingMessage) {
      return;
    }

    setNacking(true);
    setActionError("");
    setActionSuccess("");

    try {
      await postJSON("/v1/nack", {
        topic: pendingMessage.topic,
        group: pendingMessage.group,
        owner: pendingMessage.owner,
        partition: pendingMessage.partition,
        offset: pendingMessage.offset,
        reason: nackReason.trim()
      });
      setActionSuccess(`nacked offset ${pendingMessage.offset}`);
      setPendingMessage(null);
      await onConsumerChanged?.();
    } catch (err) {
      setActionError(err?.message || "failed to nack message");
    } finally {
      setNacking(false);
    }
  }

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
                {
                  activeGroup.topicSummaries.map((topic) => (
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
                  ))
                }
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
                  {
                    activeGroup.rows.map((row) => (
                      <tr key={`${row.topic}:${row.partition}`}>
                        <td>{row.topic}</td>
                        <td className="right">{fmt(row.partition)}</td>
                        <td className="right">{fmt(row.headOffset)}</td>
                        <td className="right">{fmt(row.committedOffset)}</td>
                        <td className={`right ${row.lag > 0 ? "amber" : "green2"}`}>{fmt(row.lag)}</td>
                        <td className={`right ${row.inflight > 0 ? "green" : "dim"}`}>{fmt(row.inflight)}</td>
                        <td>
                          {row.inflight > 0 ? <span className="green">leased</span> : row.lag > 0 ? <span className="amber">waiting</span> : <span className="dim">caught up</span>}
                        </td>
                      </tr>
                    ))
                  }
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

      <section className="dq-panel">
        <div className="row">
          <div>
            <strong>Debug Consume</strong>
            <div className="dim">Lease one message for inspection, then ack or nack it from the dashboard.</div>
          </div>
        </div>

        {actionError ? <div className="dq-error compact">{actionError}</div> : null}
        {actionSuccess ? <div className="dq-success">{actionSuccess}</div> : null}

        {
          activeGroup ? (
            <div className="dq-stack">
              <form className="dq-form-grid consume" onSubmit={handleConsume}>
                <label className="dq-input-stack">
                  <span>Group</span>
                  <input value={activeGroup.group} disabled />
                </label>

                <label className="dq-input-stack">
                  <span>Topic</span>
                  <select className="dq-select" value={consumeTopic} onChange={(e) => setConsumeTopic(e.target.value)} disabled={!activeGroup.topics.length || consuming}>
                    {!activeGroup.topics.length ? <option value="">No topics available</option> : null}
                    {activeGroup.topics.map((topic) => (
                      <option key={topic} value={topic}>
                        {topic}
                      </option>
                    ))}
                  </select>
                </label>

                <label className="dq-input-stack">
                  <span>Owner</span>
                  <input value={owner} onChange={(e) => setOwner(e.target.value)} placeholder="debug-ui" autoComplete="off" />
                </label>

                <label className="dq-input-stack small">
                  <span>Lease Ms</span>
                  <input type="number" min="1" step="1" value={leaseMs} onChange={(e) => setLeaseMs(e.target.value)} />
                </label>

                <div className="dq-form-actions">
                  <button type="submit" className="mini-btn" disabled={consuming || !consumeTopic}>
                    {consuming ? "Leasing..." : "Consume Next"}
                  </button>
                </div>
              </form>

              {pendingMessage ? (
                <div className="dq-stack">
                  <div className="tags">
                    <span>{pendingMessage.topic}</span>
                    <span>group {pendingMessage.group}</span>
                    <span>owner {pendingMessage.owner}</span>
                    <span>partition {pendingMessage.partition}</span>
                    <span>offset {pendingMessage.offset}</span>
                    <span>attempts {pendingMessage.attempts}</span>
                  </div>

                  <pre className="dq-payload">{JSON.stringify(parseMessageValue(pendingMessage.value || ""), null, 2)}</pre>
                  {pendingMessage.envelope ? <pre className="dq-payload">{JSON.stringify({ envelope: pendingMessage.envelope }, null, 2)}</pre> : null}
                  {pendingMessage.routing ? <pre className="dq-payload">{JSON.stringify({ routing: pendingMessage.routing }, null, 2)}</pre> : null}

                  <div className="dq-form-grid consume-actions">
                    <label className="dq-input-stack">
                      <span>Nack Reason</span>
                      <input value={nackReason} onChange={(e) => setNackReason(e.target.value)} placeholder="debug reject" />
                    </label>

                    <div className="dq-form-actions">
                      <button type="button" className="mini-btn" onClick={handleAck} disabled={acking || nacking}>
                        {acking ? "Acking..." : "Ack"}
                      </button>
                      <button type="button" className="mini-btn danger" onClick={handleNack} disabled={acking || nacking}>
                        {nacking ? "Nacking..." : "Nack"}
                      </button>
                    </div>
                  </div>
                </div>
              ) : (
                <p className="dq-note">No leased message yet. Use `Consume Next` to fetch one message into the inspector.</p>
              )}
            </div>
          ) : (
            <p className="dq-note">Create or attach a consumer group first so there is something to inspect.</p>
          )
        }
      </section>
    </div>
  );
}
