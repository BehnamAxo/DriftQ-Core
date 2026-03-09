import Sparkline from "../../shared/components/Sparkline";
import { API_PATHS, COMMON_TEXT, DEFAULTS, TOPICS_COPY, UI_LIMITS } from "../../../constants/ui";
import { fmt } from "../../../utils/number";
import { getJSON, postJSON } from "../../../utils/http";
import { useEffect, useState } from "react";

function parseMessageValue(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return { raw };
  }
}

export default function TopicsTab({ topics, spark, onTopicsChanged }) {
  const [name, setName] = useState(COMMON_TEXT.EMPTY);
  const [partitions, setPartitions] = useState(DEFAULTS.TOPIC_PARTITIONS);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState(COMMON_TEXT.EMPTY);
  const [success, setSuccess] = useState(COMMON_TEXT.EMPTY);
  const [selectedTopic, setSelectedTopic] = useState(COMMON_TEXT.EMPTY);
  const [peekMessages, setPeekMessages] = useState([]);
  const [peekLoading, setPeekLoading] = useState(false);
  const [peekError, setPeekError] = useState(COMMON_TEXT.EMPTY);
  const [selectedMessageID, setSelectedMessageID] = useState(COMMON_TEXT.EMPTY);

  useEffect(() => {
    if (!selectedTopic) {
      return;
    }

    if (!topics.some((item) => item.name === selectedTopic)) {
      setSelectedTopic(COMMON_TEXT.EMPTY);
      setPeekMessages([]);
      setPeekError(COMMON_TEXT.EMPTY);
      setSelectedMessageID(COMMON_TEXT.EMPTY);
    }
  }, [selectedTopic, topics]);

  const selectedMessage = selectedMessageID ? peekMessages.find((item) => item.id === selectedMessageID) : null;

  async function handleCreate(e) {
    e.preventDefault();

    const trimmedName = name.trim();
    const parsedPartitions = Number.parseInt(partitions, 10) || 1;

    if (!trimmedName) {
      setError(TOPICS_COPY.TOPIC_NAME_REQUIRED);
      setSuccess(COMMON_TEXT.EMPTY);
      return;
    }

    if (parsedPartitions <= 0) {
      setError(TOPICS_COPY.PARTITIONS_MINIMUM);
      setSuccess(COMMON_TEXT.EMPTY);
      return;
    }

    setSubmitting(true);
    setError(COMMON_TEXT.EMPTY);
    setSuccess(COMMON_TEXT.EMPTY);

    try {
      await postJSON(API_PATHS.CREATE_TOPIC, {
        name: trimmedName,
        partitions: parsedPartitions
      });

      setName(COMMON_TEXT.EMPTY);
      setPartitions(DEFAULTS.TOPIC_PARTITIONS);
      setSuccess(`${TOPICS_COPY.CREATED_TOPIC_PREFIX} ${trimmedName}`);

      await onTopicsChanged?.();
    } catch (err) {
      setError(err?.message || TOPICS_COPY.CREATE_TOPIC_FAILED);
    } finally {
      setSubmitting(false);
    }
  }

  async function loadPeek(topicName) {
    if (!topicName) {
      return;
    }

    setSelectedTopic(topicName);
    setPeekLoading(true);
    setPeekError(COMMON_TEXT.EMPTY);

    try {
      const payload = await getJSON(API_PATHS.topicPeek(topicName, UI_LIMITS.TOPIC_PEEK_LIMIT));
      const messages = Array.isArray(payload.messages) ? payload.messages : [];
      const normalized = messages.map((message) => ({
        id: `${topicName}:${message.partition}:${message.offset}`,
        topic: message.topic || topicName,
        partition: message.partition ?? 0,
        offset: message.offset ?? 0,
        attempts: message.attempts ?? 0,
        lastError: message.last_error || COMMON_TEXT.EMPTY,
        key: message.key || COMMON_TEXT.EMPTY,
        value: message.value || COMMON_TEXT.EMPTY,
        envelope: message.envelope || null,
        routing: message.routing || null
      }));

      setPeekMessages(normalized);
      setSelectedMessageID((prev) => (normalized.some((item) => item.id === prev) ? prev : normalized[0]?.id || COMMON_TEXT.EMPTY));
    } catch (err) {
      setPeekMessages([]);
      setSelectedMessageID(COMMON_TEXT.EMPTY);
      setPeekError(err?.message || TOPICS_COPY.LOAD_TOPIC_MESSAGES_FAILED);
    } finally {
      setPeekLoading(false);
    }
  }

  return (
    <div className="dq-stack">
      <section className="dq-panel">
        <div className="row">
          <div>
            <strong>{TOPICS_COPY.CREATE_TOPIC}</strong>
            <div className="dim">{TOPICS_COPY.CREATE_TOPIC_DESCRIPTION}</div>
          </div>
        </div>

        {error ? <div className="dq-error compact">{error}</div> : null}
        {success ? <div className="dq-success">{success}</div> : null}

        <form className="dq-form-grid" onSubmit={handleCreate}>
          <label className="dq-input-stack">
            <span>{TOPICS_COPY.NAME}</span>
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder={TOPICS_COPY.NAME_PLACEHOLDER}
              autoComplete="off"
            />
          </label>

          <label className="dq-input-stack small">
            <span>{TOPICS_COPY.PARTITIONS}</span>
            <input
              type="number"
              min="1"
              step="1"
              value={partitions}
              onChange={(e) => setPartitions(e.target.value)}
            />
          </label>

          <div className="dq-form-actions">
            <button type="submit" className="mini-btn" disabled={submitting}>
              {submitting ? TOPICS_COPY.CREATING : TOPICS_COPY.CREATE_TOPIC_BUTTON}
            </button>
          </div>
        </form>
      </section>

      <section className="dq-grid">
        {
          topics.map((t) => (
            <div className="dq-panel topic" key={t.name}>
              <div className="row">
                <strong>{t.name}</strong>
                <div className="dq-inline-actions">
                  <span>{t.partitions}P</span>
                  <button type="button" className="mini-btn" onClick={() => loadPeek(t.name)}>
                    {selectedTopic === t.name ? TOPICS_COPY.REFRESH_PEEK : TOPICS_COPY.INSPECT}
                  </button>
                </div>
              </div>
              <Sparkline values={spark[t.name]} />
              <div className="mini-grid">
                <div>
                  <b>{fmt(t.produced)}</b>
                  <small>{TOPICS_COPY.PRODUCED}</small>
                </div>

                <div>
                  <b>{fmt(t.consumed)}</b>
                  <small>{TOPICS_COPY.CONSUMED}</small>
                </div>

                <div>
                  <b>{fmt(t.inflight)}</b>
                  <small>{TOPICS_COPY.INFLIGHT}</small>
                </div>

                <div>
                  <b>{fmt(t.dlq)}</b>
                  <small>{TOPICS_COPY.DLQ}</small>
                </div>
              </div>
            </div>
          ))
        }
      </section>

      {
        selectedTopic ? (
          <section className="dq-panel">
            <div className="row">
              <div>
                <strong>{TOPICS_COPY.TOPIC_INSPECTOR}</strong>
                <div className="dim">{TOPICS_COPY.RECENT_MESSAGES_PREFIX} <code>{selectedTopic}</code>.</div>
              </div>
              <div className="dq-inline-actions">
                <button type="button" className="mini-btn" onClick={() => loadPeek(selectedTopic)} disabled={peekLoading}>
                  {peekLoading ? TOPICS_COPY.LOADING : TOPICS_COPY.REFRESH}
                </button>
                <button
                  type="button"
                  className="mini-btn"
                  onClick={() => {
                    setSelectedTopic(COMMON_TEXT.EMPTY);
                    setPeekMessages([]);
                    setPeekError(COMMON_TEXT.EMPTY);
                    setSelectedMessageID(COMMON_TEXT.EMPTY);
                  }}
                >
                  {TOPICS_COPY.CLOSE}
                </button>
              </div>
            </div>

            {peekError ? <div className="dq-error compact">{peekError}</div> : null}

            <table>
              <thead>
                <tr>
                  <th>{TOPICS_COPY.OFFSET}</th>
                  <th>{TOPICS_COPY.PARTITIONS}</th>
                  <th>{TOPICS_COPY.KEY}</th>
                  <th className="right">{TOPICS_COPY.ATTEMPTS}</th>
                  <th>{TOPICS_COPY.LAST_ERROR}</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {peekMessages.map((message) => (
                  <tr key={message.id}>
                    <td>{fmt(message.offset)}</td>
                    <td>{fmt(message.partition)}</td>
                    <td>{message.key || "-"}</td>
                    <td className="right">{fmt(message.attempts)}</td>
                    <td className={message.lastError ? "amber" : "dim"}>{message.lastError || COMMON_TEXT.DASH}</td>
                    <td>
                      <button type="button" className="mini-btn" onClick={() => setSelectedMessageID(message.id)}>
                        {selectedMessageID === message.id ? TOPICS_COPY.SELECTED : TOPICS_COPY.INSPECT}
                      </button>
                    </td>
                  </tr>
                ))}
                {
                  !peekLoading && peekMessages.length === 0 ? (
                    <tr>
                      <td colSpan={6}>{TOPICS_COPY.NO_MESSAGES_YET}</td>
                    </tr>
                  ) : null
                }
              </tbody>
            </table>

            {
              selectedMessage ? (
                <div className="dq-stack">
                  <div className="tags">
                    <span>{selectedMessage.topic}</span>
                    <span>{TOPICS_COPY.PARTITION_PREFIX} {selectedMessage.partition}</span>
                    <span>{TOPICS_COPY.OFFSET_PREFIX} {selectedMessage.offset}</span>
                    <span>{TOPICS_COPY.ATTEMPTS_PREFIX} {selectedMessage.attempts}</span>
                  </div>
                  <pre className="dq-payload">{JSON.stringify(parseMessageValue(selectedMessage.value || ""), null, 2)}</pre>
                  {selectedMessage.envelope ? <pre className="dq-payload">{JSON.stringify({ envelope: selectedMessage.envelope }, null, 2)}</pre> : null}
                  {selectedMessage.routing ? <pre className="dq-payload">{JSON.stringify({ routing: selectedMessage.routing }, null, 2)}</pre> : null}
                </div>
              ) : null
            }
          </section>
        ) : null
      }
    </div>
  );
}
