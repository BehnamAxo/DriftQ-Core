import { fmt } from "../../../utils/number";
import { formatClock } from "../../../utils/time";
import { postJSON } from "../../../utils/http";
import { useEffect, useMemo, useState } from "react";

function parseMessageValue(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return { raw };
  }
}

function sanitizeEnvelope(envelope) {
  if (!envelope) {
    return undefined;
  }

  const next = { ...envelope };
  delete next.dlq;

  return Object.keys(next).length > 0 ? next : undefined;
}

export default function DeadLettersTab({
  dlqTopic,
  onDlqTopicChange,
  dlqMessages,
  selectedDLQ,
  onToggleInspect,
  topics,
  onRedrive
}) {
  const selectedMessage = selectedDLQ ? dlqMessages.find((m) => m.id === selectedDLQ) : null;
  const [targetTopic, setTargetTopic] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");

  const topicOptions = useMemo(() => topics.map((topic) => topic.name).sort(), [topics]);

  useEffect(() => {
    if (!selectedMessage) {
      setTargetTopic("");
      setError("");
      setSuccess("");
      return;
    }

    const preferred = selectedMessage.originalTopic || selectedMessage.topic.replace(/^dlq\./, "");
    setTargetTopic(preferred);
    setError("");
    setSuccess("");
  }, [selectedMessage]);

  async function handleRedrive() {
    if (!selectedMessage) {
      return;
    }

    const topic = targetTopic.trim();
    if (!topic) {
      setError("pick a target topic");
      setSuccess("");
      return;
    }

    setSubmitting(true);
    setError("");
    setSuccess("");

    try {
      const body = {
        topic,
        key: selectedMessage.key || "",
        value: selectedMessage.value || ""
      };

      const envelope = sanitizeEnvelope(selectedMessage.envelope);
      if (envelope) {
        body.envelope = envelope;
      }

      await postJSON("/v1/produce", body);
      setSuccess(`re-driven to ${topic}`);
      await onRedrive?.();
    } catch (err) {
      setError(err?.message || "failed to re-drive DLQ message");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <section className="dq-panel">
      <h3>DLQ Topic</h3>
      <div className="dq-controls inline">
        <input value={dlqTopic} onChange={(e) => onDlqTopicChange(e.target.value)} placeholder="dlq.my-topic" />
      </div>

      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>DLQ Topic</th>
            <th>Original Topic</th>
            <th>Reason</th>
            <th className="right">Retries</th>
            <th>Failed At</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {
            dlqMessages.map((m) => (
              <tr key={m.id}>
                <td>{m.id}</td>
                <td>{m.topic}</td>
                <td>{m.originalTopic || "-"}</td>
                <td className="amber">{m.reason}</td>
                <td className="right red">{fmt(m.retries)}</td>
                <td>{m.failedAt ? formatClock(m.failedAt) : "-"}</td>
                <td>
                  <button type="button" className="mini-btn" onClick={() => onToggleInspect(m.id)}>
                    {selectedDLQ === m.id ? "Close" : "Inspect"}
                  </button>
                </td>
              </tr>
            ))
          }
          {
            dlqMessages.length === 0 ? (
              <tr>
                <td colSpan={7}>no DLQ messages</td>
              </tr>
            ) : null
          }
        </tbody>
      </table>

      {
        selectedMessage ? (
          <div className="dq-stack top-gap">
            <p className="dq-note">
              Re-drive republishes this payload to another topic. It does not remove the original message from the DLQ because the broker does not expose delete semantics here.
            </p>

            {error ? <div className="dq-error compact">{error}</div> : null}
            {success ? <div className="dq-success">{success}</div> : null}

            <div className="tags">
              <span>{selectedMessage.topic}</span>
              <span>original {selectedMessage.originalTopic || "-"}</span>
              <span>partition {selectedMessage.originalPartition}</span>
              <span>offset {selectedMessage.originalOffset}</span>
              <span>attempts {selectedMessage.retries}</span>
            </div>

            <div className="dq-form-grid redrive">
              <label className="dq-input-stack">
                <span>Target Topic</span>
                <select className="dq-select" value={targetTopic} onChange={(e) => setTargetTopic(e.target.value)} disabled={!topicOptions.length || submitting}>
                  {!topicOptions.length ? <option value="">No topics available</option> : null}
                  {topicOptions.map((topic) => (
                    <option key={topic} value={topic}>
                      {topic}
                    </option>
                  ))}
                </select>
              </label>

              <div className="dq-form-actions">
                <button type="button" className="mini-btn" onClick={handleRedrive} disabled={submitting || !targetTopic}>
                  {submitting ? "Re-driving..." : "Re-drive Message"}
                </button>
              </div>
            </div>

            <pre className="dq-payload">{JSON.stringify(parseMessageValue(selectedMessage.value || ""), null, 2)}</pre>
            {selectedMessage.envelope ? <pre className="dq-payload">{JSON.stringify({ envelope: selectedMessage.envelope }, null, 2)}</pre> : null}
            {selectedMessage.routing ? <pre className="dq-payload">{JSON.stringify({ routing: selectedMessage.routing }, null, 2)}</pre> : null}
          </div>
        ) : null
      }
    </section>
  );
}
