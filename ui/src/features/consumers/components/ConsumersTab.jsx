import { useEffect, useMemo, useRef, useState } from "react";
import { fmt } from "../../../utils/number";
import { postJSON, streamNDJSON } from "../../../utils/http";
import { formatClock } from "../../../utils/time";

function parseMessageValue(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return { raw };
  }
}

function toStreamMessage(item, session) {
  return {
    id: `${session.group}:${session.topic}:${item.partition}:${item.offset}`,
    topic: session.topic,
    group: session.group,
    owner: session.owner,
    leaseMs: session.leaseMs,
    partition: item.partition,
    offset: item.offset,
    attempts: item.attempts,
    key: item.key || "",
    value: item.value || "",
    lastError: item.last_error || "",
    envelope: item.envelope || null,
    routing: item.routing || null,
    receivedAt: Date.now()
  };
}

export default function ConsumersTab({ consumers, onConsumerChanged }) {
  const [selectedGroup, setSelectedGroup] = useState("");
  const [consumeTopic, setConsumeTopic] = useState("");
  const [owner, setOwner] = useState("debug-ui");
  const [leaseMs, setLeaseMs] = useState("10000");
  const [nackReason, setNackReason] = useState("debug reject");
  const [actionError, setActionError] = useState("");
  const [actionSuccess, setActionSuccess] = useState("");
  const [streaming, setStreaming] = useState(false);
  const [acking, setAcking] = useState(false);
  const [nacking, setNacking] = useState(false);
  const [streamSession, setStreamSession] = useState(null);
  const [streamMessages, setStreamMessages] = useState([]);
  const [selectedMessageID, setSelectedMessageID] = useState("");
  const [receivedCount, setReceivedCount] = useState(0);
  const streamControllerRef = useRef(null);

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

  useEffect(() => {
    if (!selectedMessageID) {
      if (streamMessages[0]) {
        setSelectedMessageID(streamMessages[0].id);
      }
      return;
    }

    if (!streamMessages.some((message) => message.id === selectedMessageID)) {
      setSelectedMessageID(streamMessages[0]?.id || "");
    }
  }, [selectedMessageID, streamMessages]);

  useEffect(() => () => {
    if (streamControllerRef.current) {
      streamControllerRef.current.abort(new Error("stream stopped"));
      streamControllerRef.current = null;
    }
  }, []);

  const selectedMessage = useMemo(
    () => streamMessages.find((message) => message.id === selectedMessageID) || null,
    [selectedMessageID, streamMessages]
  );

  function stopStream(successMessage = "temporary stream stopped") {
    const controller = streamControllerRef.current;
    if (!controller) {
      return;
    }

    streamControllerRef.current = null;
    controller.abort(new Error("stream stopped"));
    setStreaming(false);
    setActionError("");
    setActionSuccess(successMessage);
  }

  async function handleStreamSubmit(e) {
    e.preventDefault();

    if (streaming) {
      stopStream();
      return;
    }

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

    const session = {
      topic,
      group: activeGroup.group,
      owner: trimmedOwner,
      leaseMs: parsedLease
    };

    const controller = new AbortController();
    streamControllerRef.current = controller;

    setStreaming(true);
    setStreamSession(session);
    setStreamMessages([]);
    setSelectedMessageID("");
    setReceivedCount(0);
    setActionError("");
    setActionSuccess(`streaming ${topic} for ${activeGroup.group}`);

    try {
      await streamNDJSON(
        `/v1/consume?topic=${encodeURIComponent(topic)}&group=${encodeURIComponent(activeGroup.group)}&owner=${encodeURIComponent(trimmedOwner)}&lease_ms=${parsedLease}`,
        {
          signal: controller.signal,
          onMessage: (item) => {
            const nextMessage = toStreamMessage(item, session);
            setReceivedCount((value) => value + 1);
            setStreamMessages((prev) => {
              const remaining = prev.filter((message) => message.id !== nextMessage.id);
              return [nextMessage, ...remaining].slice(0, 25);
            });
            setSelectedMessageID((current) => current || nextMessage.id);
          }
        }
      );

      if (streamControllerRef.current === controller) {
        streamControllerRef.current = null;
        setStreaming(false);
        setActionSuccess("temporary stream ended");
      }
    } catch (err) {
      if (controller.signal.aborted) {
        return;
      }

      if (streamControllerRef.current === controller) {
        streamControllerRef.current = null;
      }
      setStreaming(false);
      setActionError(err?.message || "failed to consume stream");
    }
  }

  function clearStreamBuffer() {
    setStreamMessages([]);
    setSelectedMessageID("");
    setReceivedCount(0);
    setActionError("");
    setActionSuccess("stream buffer cleared");
  }

  async function handleAck() {
    if (!selectedMessage) {
      return;
    }

    setAcking(true);
    setActionError("");
    setActionSuccess("");

    try {
      await postJSON("/v1/ack", {
        topic: selectedMessage.topic,
        group: selectedMessage.group,
        owner: selectedMessage.owner,
        partition: selectedMessage.partition,
        offset: selectedMessage.offset
      });
      setActionSuccess(`acked offset ${selectedMessage.offset}`);
      setStreamMessages((prev) => prev.filter((message) => message.id !== selectedMessage.id));
      await onConsumerChanged?.();
    } catch (err) {
      setActionError(err?.message || "failed to ack message");
    } finally {
      setAcking(false);
    }
  }

  async function handleNack() {
    if (!selectedMessage) {
      return;
    }

    setNacking(true);
    setActionError("");
    setActionSuccess("");

    try {
      await postJSON("/v1/nack", {
        topic: selectedMessage.topic,
        group: selectedMessage.group,
        owner: selectedMessage.owner,
        partition: selectedMessage.partition,
        offset: selectedMessage.offset,
        reason: nackReason.trim()
      });
      setActionSuccess(`nacked offset ${selectedMessage.offset}`);
      setStreamMessages((prev) => prev.filter((message) => message.id !== selectedMessage.id));
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
              {
                c.topics.map((t) => (
                  <span key={t}>{t}</span>
                ))
              }
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
                <b>{fmt(c.owners.length)}</b>
                <small>owners</small>
              </div>
              <div>
                <b>{fmt(c.stalledCount)}</b>
                <small>stalled</small>
              </div>
            </div>
            <div className="dq-form-actions top-gap">
              <button type="button" className="mini-btn" onClick={() => setSelectedGroup(c.group)} disabled={streaming}>
                {streaming && activeGroup?.group === c.group ? "Streaming" : activeGroup?.group === c.group ? "Inspecting" : "Inspect"}
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
                      <div className="tags">
                        {topic.owners.length ? topic.owners.map((ownerName) => <span key={`${topic.topic}-${ownerName}`}>{ownerName}</span>) : <span>no active owner</span>}
                        <span>last delivery {topic.lastDeliveredAt ? formatClock(topic.lastDeliveredAt) : "-"}</span>
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
                    <th>Owner</th>
                    <th>Last Delivery</th>
                    <th className="right">Lease Age</th>
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
                        <td>{row.leaseOwners.join(", ") || row.lastOwner || "-"}</td>
                        <td>{row.lastDeliveredAt ? formatClock(row.lastDeliveredAt) : "-"}</td>
                        <td className={`right ${row.stalled ? "red" : row.oldestLeaseAge > 0 ? "amber" : "dim"}`}>
                          {row.oldestLeaseAge > 0 ? `${fmt(row.oldestLeaseAge)}ms` : "-"}
                        </td>
                        <td className="right">{fmt(row.headOffset)}</td>
                        <td className="right">{fmt(row.committedOffset)}</td>
                        <td className={`right ${row.lag > 0 ? "amber" : "green2"}`}>{fmt(row.lag)}</td>
                        <td className={`right ${row.inflight > 0 ? "green" : "dim"}`}>{fmt(row.inflight)}</td>
                        <td>
                          {
                            row.stalled
                            ? <span className="red">stalled</span>
                            : row.inflight > 0
                              ? <span className="green">leased</span>
                              : row.lag > 0
                                ? <span className="amber">waiting</span>
                                : <span className="dim">caught up</span>
                          }
                        </td>
                      </tr>
                    ))
                  }
                  {
                    activeGroup.rows.length === 0 ? (
                      <tr>
                        <td colSpan={10}>no partition detail available for this consumer group</td>
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
            <strong>Temporary Consumer Stream</strong>
            <div className="dim">Open a live leased stream for one group/topic/owner tuple, then inspect and ack or nack messages as they arrive.</div>
          </div>
          <span className={streaming ? "green" : "dim"}>{streaming ? "live" : "stopped"}</span>
        </div>

        {actionError ? <div className="dq-error compact">{actionError}</div> : null}
        {actionSuccess ? <div className="dq-success">{actionSuccess}</div> : null}

        {
          activeGroup ? (
            <div className="dq-stack">
              <form className="dq-form-grid consume" onSubmit={handleStreamSubmit}>
                <label className="dq-input-stack">
                  <span>Group</span>
                  <input value={activeGroup.group} disabled />
                </label>

                <label className="dq-input-stack">
                  <span>Topic</span>
                  <select className="dq-select" value={consumeTopic} onChange={(e) => setConsumeTopic(e.target.value)} disabled={!activeGroup.topics.length || streaming}>
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
                  <input value={owner} onChange={(e) => setOwner(e.target.value)} placeholder="debug-ui" autoComplete="off" disabled={streaming} />
                </label>

                <label className="dq-input-stack small">
                  <span>Lease Ms</span>
                  <input type="number" min="1" step="1" value={leaseMs} onChange={(e) => setLeaseMs(e.target.value)} disabled={streaming} />
                </label>

                <div className="dq-form-actions">
                  <button type="submit" className={`mini-btn ${streaming ? "danger" : ""}`} disabled={!streaming && !consumeTopic}>
                    {streaming ? "Stop Stream" : "Start Stream"}
                  </button>
                  <button type="button" className="mini-btn" onClick={clearStreamBuffer} disabled={streaming || !streamMessages.length}>
                    Clear Buffer
                  </button>
                </div>
              </form>

              {
                streamSession ? (
                  <div className="tags">
                    <span>{streamSession.topic}</span>
                    <span>group {streamSession.group}</span>
                    <span>owner {streamSession.owner}</span>
                    <span>lease {fmt(streamSession.leaseMs)}ms</span>
                    <span>{streaming ? "live stream" : "last session"}</span>
                    <span>{fmt(receivedCount)} received</span>
                    <span>{fmt(streamMessages.length)} buffered</span>
                  </div>
                ) : null
              }

              {
                streamMessages.length ? (
                  <div className="dq-stack">
                    <table>
                      <thead>
                        <tr>
                          <th className="right">Partition</th>
                          <th className="right">Offset</th>
                          <th className="right">Attempts</th>
                          <th>Received</th>
                          <th>Error</th>
                          <th></th>
                        </tr>
                      </thead>
                      <tbody>
                        {
                          streamMessages.map((message) => (
                            <tr key={message.id} className={selectedMessageID === message.id ? "dq-row-active" : ""}>
                              <td className="right">{fmt(message.partition)}</td>
                              <td className="right">{fmt(message.offset)}</td>
                              <td className="right">{fmt(message.attempts)}</td>
                              <td>{formatClock(message.receivedAt)}</td>
                              <td className="dim">{message.lastError || "-"}</td>
                              <td className="right">
                                <button type="button" className="mini-btn" onClick={() => setSelectedMessageID(message.id)}>
                                  {selectedMessageID === message.id ? "Inspecting" : "Inspect"}
                                </button>
                              </td>
                            </tr>
                          ))
                        }
                      </tbody>
                    </table>

                    <div className="row">
                      <div>
                        <strong>Streamed Message Detail</strong>
                        <div className="dim">{selectedMessage ? `${selectedMessage.topic}:${selectedMessage.partition}:${selectedMessage.offset}` : "Select a streamed message to inspect it."}</div>
                      </div>
                    </div>

                    {
                      selectedMessage ? (
                        <div className="dq-stack">
                          <div className="tags">
                            <span>{selectedMessage.topic}</span>
                            <span>group {selectedMessage.group}</span>
                            <span>owner {selectedMessage.owner}</span>
                            <span>partition {selectedMessage.partition}</span>
                            <span>offset {selectedMessage.offset}</span>
                            <span>attempts {selectedMessage.attempts}</span>
                            <span>received {formatClock(selectedMessage.receivedAt)}</span>
                          </div>

                          <pre className="dq-payload">{JSON.stringify(parseMessageValue(selectedMessage.value || ""), null, 2)}</pre>
                          {selectedMessage.envelope ? <pre className="dq-payload">{JSON.stringify({ envelope: selectedMessage.envelope }, null, 2)}</pre> : null}
                          {selectedMessage.routing ? <pre className="dq-payload">{JSON.stringify({ routing: selectedMessage.routing }, null, 2)}</pre> : null}

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
                        <p className="dq-note">No streamed message selected.</p>
                      )
                    }
                  </div>
                ) : (
                  <p className="dq-note">
                    {streaming
                      ? "Stream is open and waiting for messages."
                      : "No streamed messages yet. Start the temporary stream to watch messages arrive live."}
                  </p>
                )
              }
            </div>
          ) : (
            <p className="dq-note">Create or attach a consumer group first so there is something to inspect.</p>
          )
        }
      </section>
    </div>
  );
}
