import { useEffect, useMemo, useState } from "react";
import { getJSON } from "../../../utils/http";
import { fmt } from "../../../utils/number";
import { formatClock } from "../../../utils/time";

function parseMessageValue(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return { raw };
  }
}

const STATUS_OPTIONS = [
  { value: "all", label: "All states" },
  { value: "queued", label: "Queued" },
  { value: "in_flight", label: "In-Flight" },
  { value: "acked", label: "Acked" },
  { value: "retried", label: "Retried" },
  { value: "dead_lettered", label: "Dead-Lettered" }
];

export default function MessagesTab({ group, topics }) {
  const [topicFilter, setTopicFilter] = useState("");
  const [ownerFilter, setOwnerFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [rows, setRows] = useState([]);
  const [selectedID, setSelectedID] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    const controller = new AbortController();
    async function load() {
      setLoading(true);
      setError("");
      try {
        const query = new URLSearchParams({
          group: group.trim() || "bench",
          limit: "100"
        });

        if (topicFilter) {
          query.set("topic", topicFilter);
        }

        if (ownerFilter.trim()) {
          query.set("owner", ownerFilter.trim());
        }

        if (statusFilter && statusFilter !== "all") {
          query.set("status", statusFilter);
        }

        const payload = await getJSON(`/debug/messages/state?${query.toString()}`, controller.signal);
        const nextRows = Array.isArray(payload.rows) ? payload.rows : [];
        setRows(nextRows);
        setSelectedID((prev) => {
          if (prev && nextRows.some((row) => `${row.topic}:${row.partition}:${row.offset}:${row.state}` === prev)) {
            return prev;
          }
          return nextRows[0] ? `${nextRows[0].topic}:${nextRows[0].partition}:${nextRows[0].offset}:${nextRows[0].state}` : "";
        });
      } catch (err) {
        setRows([]);
        setError(err?.message || "failed to load message state");
      } finally {
        setLoading(false);
      }
    }

    load();
    return () => controller.abort();
  }, [group, ownerFilter, statusFilter, topicFilter]);

  const selectedRow = useMemo(
    () => rows.find((row) => `${row.topic}:${row.partition}:${row.offset}:${row.state}` === selectedID) || null,
    [rows, selectedID]
  );

  const counts = useMemo(() => {
    const base = { queued: 0, in_flight: 0, acked: 0, retried: 0, dead_lettered: 0 };
    for (const row of rows) {
      base[row.state] = (base[row.state] || 0) + 1;
    }
    return base;
  }, [rows]);

  return (
    <div className="dq-stack">
      <section className="dq-panel">
        <div className="row">
          <div>
            <strong>Message State Browser</strong>
            <div className="dim">Filter broker-visible messages by topic, owner, and state for group {group || "bench"}.</div>
          </div>
        </div>

        <form className="dq-form-grid dq-form-grid.messages" onSubmit={(e) => e.preventDefault()}>
          <label className="dq-input-stack">
            <span>Topic</span>
            <select className="dq-select" value={topicFilter} onChange={(e) => setTopicFilter(e.target.value)}>
              <option value="">All topics</option>
              {
                topics.map((topic) => (
                  <option key={topic.name} value={topic.name}>
                    {topic.name}
                  </option>
                ))
              }
            </select>
          </label>

          <label className="dq-input-stack">
            <span>Status</span>
            <select className="dq-select" value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
              {
                STATUS_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))
              }
            </select>
          </label>

          <label className="dq-input-stack">
            <span>Owner</span>
            <input value={ownerFilter} onChange={(e) => setOwnerFilter(e.target.value)} placeholder="ownerA" autoComplete="off" />
          </label>
        </form>

        {error ? <div className="dq-error compact">{error}</div> : null}

        <div className="tags top-gap">
          <span>queued {fmt(counts.queued)}</span>
          <span>in-flight {fmt(counts.in_flight)}</span>
          <span>acked {fmt(counts.acked)}</span>
          <span>retried {fmt(counts.retried)}</span>
          <span>dead-lettered {fmt(counts.dead_lettered)}</span>
          <span>{loading ? "refreshing" : `${fmt(rows.length)} rows`}</span>
        </div>
      </section>

      <section className="dq-panel">
        <table>
          <thead>
            <tr>
              <th>Topic</th>
              <th className="right">Partition</th>
              <th className="right">Offset</th>
              <th>State</th>
              <th>Owner</th>
              <th className="right">Attempts</th>
              <th>Last Delivery</th>
              <th className="right">Lease Age</th>
              <th>Error</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {
              rows.map((row) => {
                const id = `${row.topic}:${row.partition}:${row.offset}:${row.state}`;
                return (
                  <tr key={id}>
                    <td>{row.topic}</td>
                    <td className="right">{fmt(row.partition)}</td>
                    <td className="right">{fmt(row.offset)}</td>
                    <td>
                      <span className={
                        row.state === "dead_lettered" ? "red" :
                        row.state === "retried" ? "amber" :
                        row.state === "in_flight" ? "green" :
                        row.state === "acked" ? "blue" :
                        "dim"
                      }>
                        {row.state}
                      </span>
                    </td>
                    <td>{row.owner || "-"}</td>
                    <td className="right">{fmt(row.attempts)}</td>
                    <td>{row.last_delivered_at_ms ? formatClock(row.last_delivered_at_ms) : "-"}</td>
                    <td className={`right ${row.stalled ? "red" : row.lease_age_ms > 0 ? "amber" : "dim"}`}>
                      {row.lease_age_ms > 0 ? `${fmt(row.lease_age_ms)}ms` : "-"}
                    </td>
                    <td className="dim">{row.last_error || "-"}</td>
                    <td className="right">
                      <button type="button" className="mini-btn" onClick={() => setSelectedID(id)}>
                        {selectedID === id ? "Inspecting" : "Inspect"}
                      </button>
                    </td>
                  </tr>
                );
              })
            }
            {
              !rows.length ? (
                <tr>
                  <td colSpan={10}>{loading ? "loading message state..." : "no messages matched the current filters"}</td>
                </tr>
              ) : null
            }
          </tbody>
        </table>
      </section>

      <section className="dq-panel">
        <div className="row">
          <div>
            <strong>Message Detail</strong>
            <div className="dim">{selectedRow ? `${selectedRow.topic}:${selectedRow.partition}:${selectedRow.offset}` : "Select a message row to inspect it."}</div>
          </div>
        </div>

        {
          selectedRow ? (
            <div className="dq-stack">
              <div className="tags">
                <span>{selectedRow.state}</span>
                <span>owner {selectedRow.owner || "-"}</span>
                <span>attempts {selectedRow.attempts}</span>
                <span>lease {selectedRow.lease_duration_ms > 0 ? `${fmt(selectedRow.lease_duration_ms)}ms` : "-"}</span>
                <span>expires {selectedRow.lease_expires_at_ms ? formatClock(selectedRow.lease_expires_at_ms) : "-"}</span>
              </div>
              <pre className="dq-payload">{JSON.stringify(parseMessageValue(selectedRow.value || ""), null, 2)}</pre>
              {selectedRow.envelope ? <pre className="dq-payload">{JSON.stringify({ envelope: selectedRow.envelope }, null, 2)}</pre> : null}
              {selectedRow.routing ? <pre className="dq-payload">{JSON.stringify({ routing: selectedRow.routing }, null, 2)}</pre> : null}
            </div>
          ) : (
            <p className="dq-note">No message selected.</p>
          )
        }
      </section>
    </div>
  );
}
