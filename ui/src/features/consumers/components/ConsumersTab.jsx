import { API_PATHS, COMMON_TEXT, CONSUMER_STATUS, CONSUMERS_COPY, DEFAULTS, UI_LIMITS } from "../../../constants/ui";
import { fmt } from "../../../utils/number";
import { formatClock } from "../../../utils/time";
import { postJSON, streamNDJSON } from "../../../utils/http";
import { useEffect, useMemo, useRef, useState } from "react";

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
  const [selectedGroup, setSelectedGroup] = useState(COMMON_TEXT.EMPTY);
  const [consumeTopic, setConsumeTopic] = useState(COMMON_TEXT.EMPTY);
  const [owner, setOwner] = useState(DEFAULTS.CONSUMER_OWNER);
  const [leaseMs, setLeaseMs] = useState(DEFAULTS.CONSUMER_LEASE_MS);
  const [nackReason, setNackReason] = useState(DEFAULTS.CONSUMER_NACK_REASON);
  const [actionError, setActionError] = useState(COMMON_TEXT.EMPTY);
  const [actionSuccess, setActionSuccess] = useState(COMMON_TEXT.EMPTY);
  const [streaming, setStreaming] = useState(false);
  const [acking, setAcking] = useState(false);
  const [nacking, setNacking] = useState(false);
  const [streamSession, setStreamSession] = useState(null);
  const [streamMessages, setStreamMessages] = useState([]);
  const [selectedMessageID, setSelectedMessageID] = useState(COMMON_TEXT.EMPTY);
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
      setConsumeTopic(COMMON_TEXT.EMPTY);
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
      setSelectedMessageID(streamMessages[0]?.id || COMMON_TEXT.EMPTY);
    }
  }, [selectedMessageID, streamMessages]);

  useEffect(() => () => {
    if (streamControllerRef.current) {
      streamControllerRef.current.abort(new Error(CONSUMERS_COPY.STREAM_STOPPED));
      streamControllerRef.current = null;
    }
  }, []);

  const selectedMessage = useMemo(
    () => streamMessages.find((message) => message.id === selectedMessageID) || null,
    [selectedMessageID, streamMessages]
  );

  function stopStream(successMessage = CONSUMERS_COPY.TEMP_STREAM_STOPPED) {
    const controller = streamControllerRef.current;
    if (!controller) {
      return;
    }

    streamControllerRef.current = null;
    controller.abort(new Error(CONSUMERS_COPY.STREAM_STOPPED));
    setStreaming(false);
    setActionError(COMMON_TEXT.EMPTY);
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
      setActionError(CONSUMERS_COPY.PICK_TOPIC);
      setActionSuccess(COMMON_TEXT.EMPTY);
      return;
    }

    if (!trimmedOwner) {
      setActionError(CONSUMERS_COPY.OWNER_REQUIRED);
      setActionSuccess(COMMON_TEXT.EMPTY);
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
    setSelectedMessageID(COMMON_TEXT.EMPTY);
    setReceivedCount(0);
    setActionError(COMMON_TEXT.EMPTY);
    setActionSuccess(`${CONSUMERS_COPY.STREAMING_PREFIX} ${topic} ${CONSUMERS_COPY.STREAMING_FOR} ${activeGroup.group}`);

    try {
      await streamNDJSON(
        API_PATHS.consume({
          topic,
          group: activeGroup.group,
          owner: trimmedOwner,
          leaseMs: parsedLease
        }),
        {
          signal: controller.signal,
          onMessage: (item) => {
            const nextMessage = toStreamMessage(item, session);
            setReceivedCount((value) => value + 1);
            setStreamMessages((prev) => {
              const remaining = prev.filter((message) => message.id !== nextMessage.id);
              return [nextMessage, ...remaining].slice(0, UI_LIMITS.STREAM_BUFFER_LIMIT);
            });
            setSelectedMessageID((current) => current || nextMessage.id);
          }
        }
      );

      if (streamControllerRef.current === controller) {
        streamControllerRef.current = null;
        setStreaming(false);
        setActionSuccess(CONSUMERS_COPY.TEMP_STREAM_ENDED);
      }
    } catch (err) {
      if (controller.signal.aborted) {
        return;
      }

      if (streamControllerRef.current === controller) {
        streamControllerRef.current = null;
      }
      setStreaming(false);
      setActionError(err?.message || CONSUMERS_COPY.CONSUME_STREAM_FAILED);
    }
  }

  function clearStreamBuffer() {
    setStreamMessages([]);
    setSelectedMessageID(COMMON_TEXT.EMPTY);
    setReceivedCount(0);
    setActionError(COMMON_TEXT.EMPTY);
    setActionSuccess(CONSUMERS_COPY.STREAM_BUFFER_CLEARED);
  }

  async function handleAck() {
    if (!selectedMessage) {
      return;
    }

    setAcking(true);
    setActionError(COMMON_TEXT.EMPTY);
    setActionSuccess(COMMON_TEXT.EMPTY);

    try {
      await postJSON(API_PATHS.ACK, {
        topic: selectedMessage.topic,
        group: selectedMessage.group,
        owner: selectedMessage.owner,
        partition: selectedMessage.partition,
        offset: selectedMessage.offset
      });
      setActionSuccess(`${CONSUMERS_COPY.ACKED_OFFSET_PREFIX} ${selectedMessage.offset}`);
      setStreamMessages((prev) => prev.filter((message) => message.id !== selectedMessage.id));
      await onConsumerChanged?.();
    } catch (err) {
      setActionError(err?.message || CONSUMERS_COPY.ACK_FAILED);
    } finally {
      setAcking(false);
    }
  }

  async function handleNack() {
    if (!selectedMessage) {
      return;
    }

    setNacking(true);
    setActionError(COMMON_TEXT.EMPTY);
    setActionSuccess(COMMON_TEXT.EMPTY);

    try {
      await postJSON(API_PATHS.NACK, {
        topic: selectedMessage.topic,
        group: selectedMessage.group,
        owner: selectedMessage.owner,
        partition: selectedMessage.partition,
        offset: selectedMessage.offset,
        reason: nackReason.trim()
      });
      setActionSuccess(`${CONSUMERS_COPY.NACKED_OFFSET_PREFIX} ${selectedMessage.offset}`);
      setStreamMessages((prev) => prev.filter((message) => message.id !== selectedMessage.id));
      await onConsumerChanged?.();
    } catch (err) {
      setActionError(err?.message || CONSUMERS_COPY.NACK_FAILED);
    } finally {
      setNacking(false);
    }
  }

  return (
    <div className="dq-stack">
      <section className="dq-grid dq-consumer-groups">
        {consumers.map((c) => (
          <div className={`dq-panel dq-consumer-group-card ${activeGroup?.group === c.group ? "selected" : ""}`} key={c.group}>
            <div className="row">
              <strong>{c.group}</strong>
              <span className={c.status === CONSUMER_STATUS.CONNECTED ? "green" : c.status === CONSUMER_STATUS.BACKLOG ? "amber" : "dim"}>{c.status}</span>
            </div>
            <div className="tags dq-consumer-topic-tags">
              {
                c.topics.map((t) => (
                  <span key={t}>{t}</span>
                ))
              }
              {c.topics.length === 0 ? <span>{CONSUMERS_COPY.NO_TOPICS}</span> : null}
            </div>
            <div className="mini-grid">
              <div>
                <b>{fmt(c.activeLease)}</b>
                <small>{CONSUMERS_COPY.ACTIVE_LEASES}</small>
              </div>
              <div>
                <b>{fmt(c.totalLag)}</b>
                <small>{CONSUMERS_COPY.LAG}</small>
              </div>
              <div>
                <b>{fmt(c.partitions)}</b>
                <small>{CONSUMERS_COPY.PARTITIONS}</small>
              </div>
              <div>
                <b>{fmt(c.owners.length)}</b>
                <small>{CONSUMERS_COPY.OWNERS}</small>
              </div>
              <div>
                <b>{fmt(c.stalledCount)}</b>
                <small>{CONSUMERS_COPY.STALLED}</small>
              </div>
            </div>
            <div className="dq-form-actions top-gap">
              <button type="button" className="mini-btn" onClick={() => setSelectedGroup(c.group)} disabled={streaming}>
                {streaming && activeGroup?.group === c.group ? CONSUMERS_COPY.STREAMING : activeGroup?.group === c.group ? CONSUMERS_COPY.INSPECTING : CONSUMERS_COPY.INSPECT}
              </button>
            </div>
          </div>
        ))}
      </section>

      <section className="dq-panel">
        <div className="row">
          <div>
            <strong>{CONSUMERS_COPY.DETAIL_TITLE}</strong>
            <div className="dim">
              {activeGroup ? `${CONSUMERS_COPY.DETAIL_PREFIX} ${activeGroup.group}.` : CONSUMERS_COPY.DETAIL_EMPTY}
            </div>
          </div>
        </div>

        {
          activeGroup ? (
            <div className="dq-stack">
              <div className="dq-consumer-topic-grid-scroll">
                <div className="dq-grid compact dq-consumer-topic-grid">
                  {
                    activeGroup.topicSummaries.map((topic) => (
                      <div className="dq-panel topic dq-consumer-topic-card" key={topic.topic}>
                        <div className="row">
                          <strong>{topic.topic}</strong>
                          <span>{fmt(topic.partitions)} {CONSUMERS_COPY.PARTITIONS}</span>
                        </div>
                        <div className="mini-grid dq-consumer-mini-grid">
                          <div>
                            <b>{fmt(topic.lag)}</b>
                            <small>{CONSUMERS_COPY.LAG}</small>
                          </div>
                          <div>
                            <b>{fmt(topic.inflight)}</b>
                            <small>{CONSUMERS_COPY.INFLIGHT}</small>
                          </div>
                          <div>
                            <b>{fmt(topic.committed)}</b>
                            <small>{CONSUMERS_COPY.TABLE_HEADERS.COMMITTED}</small>
                          </div>
                          <div>
                            <b>{fmt(topic.head)}</b>
                            <small>{CONSUMERS_COPY.TABLE_HEADERS.HEAD}</small>
                          </div>
                        </div>
                        <div className="tags dq-consumer-topic-tags">
                          {topic.owners.length ? topic.owners.map((ownerName) => <span key={`${topic.topic}-${ownerName}`}>{ownerName}</span>) : <span>{CONSUMERS_COPY.NO_ACTIVE_OWNER}</span>}
                          <span>{CONSUMERS_COPY.LAST_DELIVERY_PREFIX} {topic.lastDeliveredAt ? formatClock(topic.lastDeliveredAt) : COMMON_TEXT.DASH}</span>
                        </div>
                      </div>
                    ))
                  }
                </div>
              </div>

              <div className="dq-consumer-table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>{CONSUMERS_COPY.TABLE_HEADERS.TOPIC}</th>
                      <th className="right">{CONSUMERS_COPY.TABLE_HEADERS.PARTITION}</th>
                      <th>{CONSUMERS_COPY.TABLE_HEADERS.OWNER}</th>
                      <th>{CONSUMERS_COPY.TABLE_HEADERS.LAST_DELIVERY}</th>
                      <th className="right">{CONSUMERS_COPY.TABLE_HEADERS.LEASE_AGE}</th>
                      <th className="right">{CONSUMERS_COPY.TABLE_HEADERS.HEAD}</th>
                      <th className="right">{CONSUMERS_COPY.TABLE_HEADERS.COMMITTED}</th>
                      <th className="right">{CONSUMERS_COPY.TABLE_HEADERS.LAG}</th>
                      <th className="right">{CONSUMERS_COPY.TABLE_HEADERS.INFLIGHT}</th>
                      <th>{CONSUMERS_COPY.TABLE_HEADERS.STATE}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {
                      activeGroup.rows.map((row) => (
                        <tr key={`${row.topic}:${row.partition}`}>
                          <td>{row.topic}</td>
                          <td className="right">{fmt(row.partition)}</td>
                          <td>{row.leaseOwners.join(", ") || row.lastOwner || COMMON_TEXT.DASH}</td>
                          <td>{row.lastDeliveredAt ? formatClock(row.lastDeliveredAt) : COMMON_TEXT.DASH}</td>
                          <td className={`right ${row.stalled ? "red" : row.oldestLeaseAge > 0 ? "amber" : "dim"}`}>
                            {row.oldestLeaseAge > 0 ? `${fmt(row.oldestLeaseAge)}ms` : COMMON_TEXT.DASH}
                          </td>
                          <td className="right">{fmt(row.headOffset)}</td>
                          <td className="right">{fmt(row.committedOffset)}</td>
                          <td className={`right ${row.lag > 0 ? "amber" : "green2"}`}>{fmt(row.lag)}</td>
                          <td className={`right ${row.inflight > 0 ? "green" : "dim"}`}>{fmt(row.inflight)}</td>
                          <td>
                            {
                              row.stalled
                              ? <span className="red">{CONSUMERS_COPY.STATE_LABELS.STALLED}</span>
                              : row.inflight > 0
                                ? <span className="green">{CONSUMERS_COPY.STATE_LABELS.LEASED}</span>
                                : row.lag > 0
                                  ? <span className="amber">{CONSUMERS_COPY.STATE_LABELS.WAITING}</span>
                                  : <span className="dim">{CONSUMERS_COPY.STATE_LABELS.CAUGHT_UP}</span>
                            }
                          </td>
                        </tr>
                      ))
                    }
                    {
                      activeGroup.rows.length === 0 ? (
                        <tr>
                          <td colSpan={10}>{CONSUMERS_COPY.NO_PARTITION_DETAIL}</td>
                        </tr>
                      ) : null
                    }
                  </tbody>
                </table>
              </div>
            </div>
          ) : (
            <p className="dq-note">{CONSUMERS_COPY.NO_CONSUMER_GROUPS}</p>
          )
        }
      </section>

      <section className="dq-panel">
        <div className="row">
          <div>
            <strong>{CONSUMERS_COPY.TEMP_STREAM_TITLE}</strong>
            <div className="dim">{CONSUMERS_COPY.TEMP_STREAM_DESCRIPTION}</div>
          </div>
          <span className={streaming ? "green" : "dim"}>{streaming ? CONSUMERS_COPY.LIVE : CONSUMERS_COPY.STOPPED}</span>
        </div>

        {actionError ? <div className="dq-error compact">{actionError}</div> : null}
        {actionSuccess ? <div className="dq-success">{actionSuccess}</div> : null}

        {
          activeGroup ? (
            <div className="dq-stack">
              <form className="dq-form-grid consume" onSubmit={handleStreamSubmit}>
                <label className="dq-input-stack">
                  <span>{CONSUMERS_COPY.GROUP}</span>
                  <input value={activeGroup.group} disabled />
                </label>

                <label className="dq-input-stack">
                  <span>{CONSUMERS_COPY.TOPIC}</span>
                  <select className="dq-select" value={consumeTopic} onChange={(e) => setConsumeTopic(e.target.value)} disabled={!activeGroup.topics.length || streaming}>
                    {!activeGroup.topics.length ? <option value={COMMON_TEXT.EMPTY}>{COMMON_TEXT.NO_TOPICS_AVAILABLE}</option> : null}
                    {activeGroup.topics.map((topic) => (
                      <option key={topic} value={topic}>
                        {topic}
                      </option>
                    ))}
                  </select>
                </label>

                <label className="dq-input-stack">
                  <span>{CONSUMERS_COPY.OWNER}</span>
                  <input value={owner} onChange={(e) => setOwner(e.target.value)} placeholder={CONSUMERS_COPY.OWNER_PLACEHOLDER} autoComplete="off" disabled={streaming} />
                </label>

                <label className="dq-input-stack small">
                  <span>{CONSUMERS_COPY.LEASE_MS}</span>
                  <input type="number" min="1" step="1" value={leaseMs} onChange={(e) => setLeaseMs(e.target.value)} disabled={streaming} />
                </label>

                <div className="dq-form-actions">
                  <button type="submit" className={`mini-btn ${streaming ? "danger" : ""}`} disabled={!streaming && !consumeTopic}>
                    {streaming ? CONSUMERS_COPY.STOP_STREAM : CONSUMERS_COPY.START_STREAM}
                  </button>
                  <button type="button" className="mini-btn" onClick={clearStreamBuffer} disabled={streaming || !streamMessages.length}>
                    {CONSUMERS_COPY.CLEAR_BUFFER}
                  </button>
                </div>
              </form>

              {
                streamSession ? (
                  <div className="tags">
                    <span>{streamSession.topic}</span>
                    <span>{CONSUMERS_COPY.GROUP_PREFIX} {streamSession.group}</span>
                    <span>{CONSUMERS_COPY.OWNER_PREFIX} {streamSession.owner}</span>
                    <span>{CONSUMERS_COPY.LEASE_PREFIX} {fmt(streamSession.leaseMs)}ms</span>
                    <span>{streaming ? CONSUMERS_COPY.LIVE_STREAM : CONSUMERS_COPY.LAST_SESSION}</span>
                    <span>{fmt(receivedCount)} {CONSUMERS_COPY.RECEIVED}</span>
                    <span>{fmt(streamMessages.length)} {CONSUMERS_COPY.BUFFERED}</span>
                  </div>
                ) : null
              }

              {
                streamMessages.length ? (
                  <div className="dq-stack">
                    <table>
                      <thead>
                        <tr>
                          <th className="right">{CONSUMERS_COPY.TABLE_HEADERS.PARTITION}</th>
                          <th className="right">{CONSUMERS_COPY.OFFSET}</th>
                          <th className="right">{CONSUMERS_COPY.ATTEMPTS}</th>
                          <th>{CONSUMERS_COPY.RECEIVED_AT}</th>
                          <th>{CONSUMERS_COPY.ERROR}</th>
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
                              <td className="dim">{message.lastError || COMMON_TEXT.DASH}</td>
                              <td className="right">
                                <button type="button" className="mini-btn" onClick={() => setSelectedMessageID(message.id)}>
                                  {selectedMessageID === message.id ? CONSUMERS_COPY.INSPECTING : CONSUMERS_COPY.INSPECT}
                                </button>
                              </td>
                            </tr>
                          ))
                        }
                      </tbody>
                    </table>

                    <div className="row">
                      <div>
                        <strong>{CONSUMERS_COPY.STREAMED_DETAIL_TITLE}</strong>
                        <div className="dim">{selectedMessage ? `${selectedMessage.topic}:${selectedMessage.partition}:${selectedMessage.offset}` : CONSUMERS_COPY.STREAMED_DETAIL_EMPTY}</div>
                      </div>
                    </div>

                    {
                      selectedMessage ? (
                        <div className="dq-stack">
                          <div className="tags">
                            <span>{selectedMessage.topic}</span>
                            <span>{CONSUMERS_COPY.GROUP_PREFIX} {selectedMessage.group}</span>
                            <span>{CONSUMERS_COPY.OWNER_PREFIX} {selectedMessage.owner}</span>
                            <span>{CONSUMERS_COPY.TABLE_HEADERS.PARTITION.toLowerCase()} {selectedMessage.partition}</span>
                            <span>{CONSUMERS_COPY.OFFSET.toLowerCase()} {selectedMessage.offset}</span>
                            <span>{CONSUMERS_COPY.ATTEMPTS.toLowerCase()} {selectedMessage.attempts}</span>
                            <span>{CONSUMERS_COPY.RECEIVED.toLowerCase()} {formatClock(selectedMessage.receivedAt)}</span>
                          </div>

                          <pre className="dq-payload">{JSON.stringify(parseMessageValue(selectedMessage.value || ""), null, 2)}</pre>
                          {selectedMessage.envelope ? <pre className="dq-payload">{JSON.stringify({ envelope: selectedMessage.envelope }, null, 2)}</pre> : null}
                          {selectedMessage.routing ? <pre className="dq-payload">{JSON.stringify({ routing: selectedMessage.routing }, null, 2)}</pre> : null}

                          <div className="dq-form-grid consume-actions">
                            <label className="dq-input-stack">
                              <span>{CONSUMERS_COPY.NACK_REASON}</span>
                              <input value={nackReason} onChange={(e) => setNackReason(e.target.value)} placeholder={CONSUMERS_COPY.NACK_REASON_PLACEHOLDER} />
                            </label>

                            <div className="dq-form-actions">
                              <button type="button" className="mini-btn" onClick={handleAck} disabled={acking || nacking}>
                                {acking ? CONSUMERS_COPY.ACKING : CONSUMERS_COPY.ACK}
                              </button>
                              <button type="button" className="mini-btn danger" onClick={handleNack} disabled={acking || nacking}>
                                {nacking ? CONSUMERS_COPY.NACKING : CONSUMERS_COPY.NACK}
                              </button>
                            </div>
                          </div>
                        </div>
                      ) : (
                        <p className="dq-note">{CONSUMERS_COPY.NO_STREAMED_MESSAGE}</p>
                      )
                    }
                  </div>
                ) : (
                  <p className="dq-note">
                    {streaming
                      ? CONSUMERS_COPY.STREAM_WAITING
                      : CONSUMERS_COPY.STREAM_EMPTY}
                  </p>
                )
              }
            </div>
          ) : (
            <p className="dq-note">{CONSUMERS_COPY.ATTACH_GROUP_FIRST}</p>
          )
        }
      </section>
    </div>
  );
}
