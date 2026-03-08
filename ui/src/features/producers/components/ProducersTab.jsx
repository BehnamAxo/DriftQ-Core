import { API_PATHS, COMMON_TEXT, PRODUCERS_COPY } from "../../../constants/ui";
import { fmt } from "../../../utils/number";
import { postJSON } from "../../../utils/http";
import { useEffect, useState } from "react";

export default function ProducersTab({ producerReasons, topics, onProduced }) {
  const [topic, setTopic] = useState(COMMON_TEXT.EMPTY);
  const [key, setKey] = useState(COMMON_TEXT.EMPTY);
  const [value, setValue] = useState(COMMON_TEXT.EMPTY);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState(COMMON_TEXT.EMPTY);
  const [success, setSuccess] = useState(COMMON_TEXT.EMPTY);
  const hasTopics = topics.length > 0;

  useEffect(() => {
    if (!hasTopics) {
      setTopic(COMMON_TEXT.EMPTY);
      return;
    }

    if (!topic || !topics.some((item) => item.name === topic)) {
      setTopic(topics[0].name);
    }
  }, [hasTopics, topic, topics]);

  async function handleProduce(e) {
    e.preventDefault();

    const trimmedTopic = topic.trim();
    if (!trimmedTopic) {
      setError(PRODUCERS_COPY.TOPIC_REQUIRED);
      setSuccess(COMMON_TEXT.EMPTY);
      return;
    }

    if (!value) {
      setError(PRODUCERS_COPY.MESSAGE_VALUE_REQUIRED);
      setSuccess(COMMON_TEXT.EMPTY);
      return;
    }

    setSubmitting(true);
    setError(COMMON_TEXT.EMPTY);
    setSuccess(COMMON_TEXT.EMPTY);

    try {
      await postJSON(API_PATHS.PRODUCE, {
        topic: trimmedTopic,
        key: key.trim(),
        value
      });
      setSuccess(`${PRODUCERS_COPY.PRODUCED_TO_PREFIX} ${trimmedTopic}`);
      setValue(COMMON_TEXT.EMPTY);
      setKey(COMMON_TEXT.EMPTY);
      await onProduced?.();
    } catch (err) {
      setError(err?.message || PRODUCERS_COPY.PRODUCE_FAILED);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="dq-stack">
      <section className="dq-panel">
        <div className="row">
          <div>
            <strong>{PRODUCERS_COPY.PRODUCE_MESSAGE}</strong>
            <div className="dim">{PRODUCERS_COPY.PRODUCE_DESCRIPTION}</div>
          </div>
        </div>

        {error ? <div className="dq-error compact">{error}</div> : null}
        {success ? <div className="dq-success">{success}</div> : null}
        {!hasTopics ? <div className="dq-note">{PRODUCERS_COPY.CREATE_TOPIC_FIRST}</div> : null}

        <form className="dq-stack" onSubmit={handleProduce}>
          <div className="dq-form-grid producer">
            <label className="dq-input-stack">
              <span>{PRODUCERS_COPY.TOPIC}</span>
              <select
                className="dq-select"
                value={topic}
                onChange={(e) => {
                  setTopic(e.target.value);
                  setError(COMMON_TEXT.EMPTY);
                  setSuccess(COMMON_TEXT.EMPTY);
                }}
                disabled={!hasTopics || submitting}
              >
                {!hasTopics ? <option value={COMMON_TEXT.EMPTY}>{COMMON_TEXT.NO_TOPICS_AVAILABLE}</option> : null}
                {topics.map((item) => (
                  <option key={item.name} value={item.name}>
                    {item.name}
                  </option>
                ))}
              </select>
            </label>

            <label className="dq-input-stack">
              <span>{PRODUCERS_COPY.KEY}</span>
              <input
                value={key}
                onChange={(e) => setKey(e.target.value)}
                placeholder={PRODUCERS_COPY.KEY_PLACEHOLDER}
                autoComplete="off"
              />
            </label>

            <div className="dq-form-actions">
              <button type="submit" className="mini-btn" disabled={submitting || !hasTopics}>
                {submitting ? PRODUCERS_COPY.SENDING : PRODUCERS_COPY.SEND_MESSAGE}
              </button>
            </div>
          </div>

          <label className="dq-input-stack">
            <span>{PRODUCERS_COPY.VALUE}</span>
            <textarea
              className="dq-textarea"
              value={value}
              onChange={(e) => setValue(e.target.value)}
              placeholder={PRODUCERS_COPY.VALUE_PLACEHOLDER}
              rows={5}
            />
          </label>
        </form>
      </section>

      <section className="dq-panel">
        <p className="dq-note">
          {PRODUCERS_COPY.COUNTER_NOTE} <code>{API_PATHS.METRICS}</code>.
        </p>
        <h3>{PRODUCERS_COPY.REJECTIONS_BY_REASON}</h3>
        <table>
          <thead>
            <tr>
              <th>{PRODUCERS_COPY.REASON}</th>
              <th className="right">{PRODUCERS_COPY.COUNT}</th>
            </tr>
          </thead>
          <tbody>
            {
              producerReasons.map((r) => (
                <tr key={r.reason}>
                  <td>{r.reason}</td>
                  <td className="right amber">{fmt(r.value)}</td>
                </tr>
              ))
            }

            {
              producerReasons.length === 0 ? (
                <tr>
                  <td colSpan={2}>{PRODUCERS_COPY.NO_REJECTION_METRICS}</td>
                </tr>
              ) : null
            }
          </tbody>
        </table>
      </section>
    </div>
  );
}
