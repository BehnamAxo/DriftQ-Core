import Sparkline from "../Sparkline";
import { fmt } from "../../utils/number";
import { postJSON } from "../../utils/http";
import { useState } from "react";

export default function TopicsTab({ topics, spark, onTopicsChanged }) {
  const [name, setName] = useState("");
  const [partitions, setPartitions] = useState("1");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");

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
                <span>{t.partitions}P</span>
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
    </div>
  );
}
