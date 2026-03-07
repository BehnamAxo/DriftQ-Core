import { fmt } from "../../utils/number";
import { postJSON } from "../../utils/http";
import { useEffect, useState } from "react";

export default function ProducersTab({ producerReasons, topics, onProduced }) {
  const [topic, setTopic] = useState("");
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const hasTopics = topics.length > 0;

  useEffect(() => {
    if (!hasTopics) {
      setTopic("");
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
      setError("topic is required");
      setSuccess("");
      return;
    }

    if (!value) {
      setError("message value is required");
      setSuccess("");
      return;
    }

    setSubmitting(true);
    setError("");
    setSuccess("");

    try {
      await postJSON("/v1/produce", {
        topic: trimmedTopic,
        key: key.trim(),
        value
      });
      setSuccess(`produced to ${trimmedTopic}`);
      setValue("");
      setKey("");
      await onProduced?.();
    } catch (err) {
      setError(err?.message || "failed to produce message");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="dq-stack">
      <section className="dq-panel">
        <div className="row">
          <div>
            <strong>Produce Message</strong>
            <div className="dim">Send a test message without leaving the dashboard.</div>
          </div>
        </div>

        {error ? <div className="dq-error compact">{error}</div> : null}
        {success ? <div className="dq-success">{success}</div> : null}
        {!hasTopics ? <div className="dq-note">Create a topic first in the Topics tab, then come back here to send a message.</div> : null}

        <form className="dq-stack" onSubmit={handleProduce}>
          <div className="dq-form-grid producer">
            <label className="dq-input-stack">
              <span>Topic</span>
              <select
                className="dq-select"
                value={topic}
                onChange={(e) => {
                  setTopic(e.target.value);
                  setError("");
                  setSuccess("");
                }}
                disabled={!hasTopics || submitting}
              >
                {!hasTopics ? <option value="">No topics available</option> : null}
                {topics.map((item) => (
                  <option key={item.name} value={item.name}>
                    {item.name}
                  </option>
                ))}
              </select>
            </label>

            <label className="dq-input-stack">
              <span>Key</span>
              <input
                value={key}
                onChange={(e) => setKey(e.target.value)}
                placeholder="optional-key"
                autoComplete="off"
              />
            </label>

            <div className="dq-form-actions">
              <button type="submit" className="mini-btn" disabled={submitting || !hasTopics}>
                {submitting ? "Sending..." : "Send Message"}
              </button>
            </div>
          </div>

          <label className="dq-input-stack">
            <span>Value</span>
            <textarea
              className="dq-textarea"
              value={value}
              onChange={(e) => setValue(e.target.value)}
              placeholder='{"hello":"driftq"}'
              rows={5}
            />
          </label>
        </form>
      </section>

      <section className="dq-panel">
        <p className="dq-note">
          Producer identity is not currently exposed by DriftQ API. This tab uses real broker counters from <code>/metrics</code>.
        </p>
        <h3>Produce Rejections by Reason</h3>
        <table>
          <thead>
            <tr>
              <th>Reason</th>
              <th className="right">Count</th>
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
                  <td colSpan={2}>no rejection metrics yet</td>
                </tr>
              ) : null
            }
          </tbody>
        </table>
      </section>
    </div>
  );
}
