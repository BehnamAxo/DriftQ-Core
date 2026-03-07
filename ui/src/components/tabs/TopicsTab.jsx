import Sparkline from "../Sparkline";
import { fmt } from "../../utils/number";
import { getJSON, postJSON } from "../../utils/http";
import { useEffect, useState } from "react";

function parseMessageValue(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return { raw };
  }
}

export default function TopicsTab({ topics, spark, onTopicsChanged }) {
  const [name, setName] = useState("");
  const [partitions, setPartitions] = useState("1");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const [selectedTopic, setSelectedTopic] = useState("");
  const [peekMessages, setPeekMessages] = useState([]);
  const [peekLoading, setPeekLoading] = useState(false);
  const [peekError, setPeekError] = useState("");
  const [selectedMessageID, setSelectedMessageID] = useState("");

  useEffect(() => {
    if (!selectedTopic) {
      return;
    }

    if (!topics.some((item) => item.name === selectedTopic)) {
      setSelectedTopic("");
      setPeekMessages([]);
      setPeekError("");
      setSelectedMessageID("");
    }
  }, [selectedTopic, topics]);

  const selectedMessage = selectedMessageID ? peekMessages.find((item) => item.id === selectedMessageID) : null;

  async function handleCreate(e) {
    e.preventDefault();

    const trimmedName = name.trim();
    const parsedPartitions = Number.parseInt(partitions, 10) || 1;

    if (!trimmedName) {
      setError("topic name is required");
      setSuccess("");
      return;
    }

    if (parsedPartitions <= 0) {
      setError("partitions must be >= 1");
      setSuccess("");
      return;
    }

    setSubmitting(true);
    setError("");
    setSuccess("");

    try {
      await postJSON("/v1/topics", {
        name: trimmedName,
        partitions: parsedPartitions
      });

      setName("");
      setPartitions("1");
      setSuccess(`created topic ${trimmedName}`);

      await onTopicsChanged?.();
    } catch (err) {
      setError(err?.message || "failed to create topic");
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
    setPeekError("");

    try {
      const payload = await getJSON(`/debug/topics/peek?topic=${encodeURIComponent(topicName)}&limit=12`);
      const messages = Array.isArray(payload.messages) ? payload.messages : [];
      const normalized = messages.map((message) => ({
        id: `${topicName}:${message.partition}:${message.offset}`,
        topic: message.topic || topicName,
        partition: message.partition ?? 0,
        offset: message.offset ?? 0,
        attempts: message.attempts ?? 0,
        lastError: message.last_error || "",
        key: message.key || "",
        value: message.value || "",
        envelope: message.envelope || null,
        routing: message.routing || null
      }));

      setPeekMessages(normalized);
      setSelectedMessageID((prev) => (normalized.some((item) => item.id === prev) ? prev : normalized[0]?.id || ""));
    } catch (err) {
      setPeekMessages([]);
      setSelectedMessageID("");
      setPeekError(err?.message || "failed to load topic messages");
    } finally {
      setPeekLoading(false);
    }
  }

  return (
    <div className="dq-stack">
      <section className="dq-panel">
        <div className="row">
          <div>
            <strong>Create Topic</strong>
            <div className="dim">Provision a topic directly from the dashboard.</div>
          </div>
        </div>

        {error ? <div className="dq-error compact">{error}</div> : null}
        {success ? <div className="dq-success">{success}</div> : null}

        <form className="dq-form-grid" onSubmit={handleCreate}>
          <label className="dq-input-stack">
            <span>Name</span>
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="orders"
              autoComplete="off"
            />
          </label>

          <label className="dq-input-stack small">
            <span>Partitions</span>
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
              {submitting ? "Creating..." : "Create Topic"}
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
                    {selectedTopic === t.name ? "Refresh Peek" : "Inspect"}
                  </button>
                </div>
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

      {
        selectedTopic ? (
          <section className="dq-panel">
            <div className="row">
              <div>
                <strong>Topic Inspector</strong>
                <div className="dim">Recent messages for <code>{selectedTopic}</code>.</div>
              </div>
              <div className="dq-inline-actions">
                <button type="button" className="mini-btn" onClick={() => loadPeek(selectedTopic)} disabled={peekLoading}>
                  {peekLoading ? "Loading..." : "Refresh"}
                </button>
                <button
                  type="button"
                  className="mini-btn"
                  onClick={() => {
                    setSelectedTopic("");
                    setPeekMessages([]);
                    setPeekError("");
                    setSelectedMessageID("");
                  }}
                >
                  Close
                </button>
              </div>
            </div>

            {peekError ? <div className="dq-error compact">{peekError}</div> : null}

            <table>
              <thead>
                <tr>
                  <th>Offset</th>
                  <th>Partition</th>
                  <th>Key</th>
                  <th className="right">Attempts</th>
                  <th>Last Error</th>
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
                    <td className={message.lastError ? "amber" : "dim"}>{message.lastError || "-"}</td>
                    <td>
                      <button type="button" className="mini-btn" onClick={() => setSelectedMessageID(message.id)}>
                        {selectedMessageID === message.id ? "Selected" : "Inspect"}
                      </button>
                    </td>
                  </tr>
                ))}
                {
                  !peekLoading && peekMessages.length === 0 ? (
                    <tr>
                      <td colSpan={6}>no messages in this topic yet</td>
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
                    <span>partition {selectedMessage.partition}</span>
                    <span>offset {selectedMessage.offset}</span>
                    <span>attempts {selectedMessage.attempts}</span>
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
