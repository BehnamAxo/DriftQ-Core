import { API_PATHS, COMMON_TEXT, DEAD_LETTERS_COPY, TOPIC_PREFIX } from "../../../constants/ui";
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
  const [targetTopic, setTargetTopic] = useState(COMMON_TEXT.EMPTY);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState(COMMON_TEXT.EMPTY);
  const [success, setSuccess] = useState(COMMON_TEXT.EMPTY);

  const topicOptions = useMemo(() => topics.map((topic) => topic.name).sort(), [topics]);

  useEffect(() => {
    if (!selectedMessage) {
      setTargetTopic(COMMON_TEXT.EMPTY);
      setError(COMMON_TEXT.EMPTY);
      setSuccess(COMMON_TEXT.EMPTY);
      return;
    }

    const preferred = selectedMessage.originalTopic || selectedMessage.topic.replace(new RegExp(`^${TOPIC_PREFIX.DLQ.replace(".", "\\.")}`), COMMON_TEXT.EMPTY);
    setTargetTopic(preferred);
    setError(COMMON_TEXT.EMPTY);
    setSuccess(COMMON_TEXT.EMPTY);
  }, [selectedMessage]);

  async function handleRedrive() {
    if (!selectedMessage) {
      return;
    }

    const topic = targetTopic.trim();
    if (!topic) {
      setError(DEAD_LETTERS_COPY.PICK_TARGET_TOPIC);
      setSuccess(COMMON_TEXT.EMPTY);
      return;
    }

    setSubmitting(true);
    setError(COMMON_TEXT.EMPTY);
    setSuccess(COMMON_TEXT.EMPTY);

    try {
      const body = {
        topic,
        key: selectedMessage.key || COMMON_TEXT.EMPTY,
        value: selectedMessage.value || COMMON_TEXT.EMPTY
      };

      const envelope = sanitizeEnvelope(selectedMessage.envelope);
      if (envelope) {
        body.envelope = envelope;
      }

      await postJSON(API_PATHS.PRODUCE, body);
      setSuccess(`${DEAD_LETTERS_COPY.REDRIVE_PREFIX} ${topic}`);
      await onRedrive?.();
    } catch (err) {
      setError(err?.message || DEAD_LETTERS_COPY.REDRIVE_FAILED);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <section className="dq-panel">
      <h3>{DEAD_LETTERS_COPY.DLQ_TOPIC}</h3>
      <div className="dq-controls inline">
        <input value={dlqTopic} onChange={(e) => onDlqTopicChange(e.target.value)} placeholder={DEAD_LETTERS_COPY.INPUT_PLACEHOLDER} />
      </div>

      <table>
        <thead>
          <tr>
            <th>{DEAD_LETTERS_COPY.TABLE_HEADERS.ID}</th>
            <th>{DEAD_LETTERS_COPY.TABLE_HEADERS.DLQ_TOPIC}</th>
            <th>{DEAD_LETTERS_COPY.TABLE_HEADERS.ORIGINAL_TOPIC}</th>
            <th>{DEAD_LETTERS_COPY.TABLE_HEADERS.REASON}</th>
            <th className="right">{DEAD_LETTERS_COPY.TABLE_HEADERS.RETRIES}</th>
            <th>{DEAD_LETTERS_COPY.TABLE_HEADERS.FAILED_AT}</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {
            dlqMessages.map((m) => (
              <tr key={m.id}>
                <td>{m.id}</td>
                <td>{m.topic}</td>
                <td>{m.originalTopic || COMMON_TEXT.DASH}</td>
                <td className="amber">{m.reason}</td>
                <td className="right red">{fmt(m.retries)}</td>
                <td>{m.failedAt ? formatClock(m.failedAt) : COMMON_TEXT.DASH}</td>
                <td>
                  <button type="button" className="mini-btn" onClick={() => onToggleInspect(m.id)}>
                    {selectedDLQ === m.id ? DEAD_LETTERS_COPY.CLOSE : DEAD_LETTERS_COPY.INSPECT}
                  </button>
                </td>
              </tr>
            ))
          }
          {
            dlqMessages.length === 0 ? (
              <tr>
                <td colSpan={7}>{DEAD_LETTERS_COPY.NO_MESSAGES}</td>
              </tr>
            ) : null
          }
        </tbody>
      </table>

      {
        selectedMessage ? (
          <div className="dq-stack top-gap">
            <p className="dq-note">
              {DEAD_LETTERS_COPY.REDRIVE_NOTE}
            </p>

            {error ? <div className="dq-error compact">{error}</div> : null}
            {success ? <div className="dq-success">{success}</div> : null}

            <div className="tags">
              <span>{selectedMessage.topic}</span>
              <span>{DEAD_LETTERS_COPY.ORIGINAL_PREFIX} {selectedMessage.originalTopic || COMMON_TEXT.DASH}</span>
              <span>{DEAD_LETTERS_COPY.PARTITION_PREFIX} {selectedMessage.originalPartition}</span>
              <span>{DEAD_LETTERS_COPY.OFFSET_PREFIX} {selectedMessage.originalOffset}</span>
              <span>{DEAD_LETTERS_COPY.ATTEMPTS_PREFIX} {selectedMessage.retries}</span>
            </div>

            <div className="dq-form-grid redrive">
              <label className="dq-input-stack">
                <span>{DEAD_LETTERS_COPY.TARGET_TOPIC}</span>
                <select className="dq-select" value={targetTopic} onChange={(e) => setTargetTopic(e.target.value)} disabled={!topicOptions.length || submitting}>
                  {!topicOptions.length ? <option value={COMMON_TEXT.EMPTY}>{COMMON_TEXT.NO_TOPICS_AVAILABLE}</option> : null}
                  {topicOptions.map((topic) => (
                    <option key={topic} value={topic}>
                      {topic}
                    </option>
                  ))}
                </select>
              </label>

              <div className="dq-form-actions">
                <button type="button" className="mini-btn" onClick={handleRedrive} disabled={submitting || !targetTopic}>
                  {submitting ? DEAD_LETTERS_COPY.REDRIVING : DEAD_LETTERS_COPY.REDRIVE_BUTTON}
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
