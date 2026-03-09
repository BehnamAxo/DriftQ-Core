import { API_PATHS, COMMON_TEXT, DEFAULTS, MESSAGE_STATE, MESSAGE_STATE_OPTIONS, MESSAGES_COPY, UI_LIMITS } from "../../../constants/ui";
import { fmt } from "../../../utils/number";
import { formatClock } from "../../../utils/time";
import { getJSON } from "../../../utils/http";
import { useEffect, useMemo, useState } from "react";

function parseMessageValue(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return { raw };
  }
}

export default function MessagesTab({ group, topics }) {
  const [topicFilter, setTopicFilter] = useState(COMMON_TEXT.EMPTY);
  const [ownerFilter, setOwnerFilter] = useState(COMMON_TEXT.EMPTY);
  const [statusFilter, setStatusFilter] = useState(MESSAGE_STATE.ALL);
  const [rows, setRows] = useState([]);
  const [selectedID, setSelectedID] = useState(COMMON_TEXT.EMPTY);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(COMMON_TEXT.EMPTY);

  useEffect(() => {
    const controller = new AbortController();
    async function load() {
      setLoading(true);
      setError(COMMON_TEXT.EMPTY);
      try {
        const query = new URLSearchParams({
          group: group.trim() || DEFAULTS.GROUP,
          limit: String(UI_LIMITS.MESSAGE_STATE_LIMIT)
        });

        if (topicFilter) {
          query.set("topic", topicFilter);
        }

        if (ownerFilter.trim()) {
          query.set("owner", ownerFilter.trim());
        }

        if (statusFilter && statusFilter !== MESSAGE_STATE.ALL) {
          query.set("status", statusFilter);
        }

        const payload = await getJSON(API_PATHS.messageState(query), controller.signal);
        const nextRows = Array.isArray(payload.rows) ? payload.rows : [];
        setRows(nextRows);
        setSelectedID((prev) => {
          if (prev && nextRows.some((row) => `${row.topic}:${row.partition}:${row.offset}:${row.state}` === prev)) {
            return prev;
          }
          return nextRows[0] ? `${nextRows[0].topic}:${nextRows[0].partition}:${nextRows[0].offset}:${nextRows[0].state}` : COMMON_TEXT.EMPTY;
        });
      } catch (err) {
        setRows([]);
        setError(err?.message || MESSAGES_COPY.LOAD_STATE_FAILED);
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
    const base = {
      [MESSAGE_STATE.QUEUED]: 0,
      [MESSAGE_STATE.IN_FLIGHT]: 0,
      [MESSAGE_STATE.ACKED]: 0,
      [MESSAGE_STATE.RETRIED]: 0,
      [MESSAGE_STATE.DEAD_LETTERED]: 0
    };
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
            <strong>{MESSAGES_COPY.BROWSER_TITLE}</strong>
            <div className="dim">{MESSAGES_COPY.BROWSER_DESCRIPTION_PREFIX} {group || DEFAULTS.GROUP}.</div>
          </div>
        </div>

        <form className="dq-form-grid dq-form-grid.messages" onSubmit={(e) => e.preventDefault()}>
          <label className="dq-input-stack">
            <span>{MESSAGES_COPY.TOPIC}</span>
            <select className="dq-select" value={topicFilter} onChange={(e) => setTopicFilter(e.target.value)}>
              <option value={COMMON_TEXT.EMPTY}>{MESSAGES_COPY.ALL_TOPICS}</option>
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
            <span>{MESSAGES_COPY.STATUS}</span>
            <select className="dq-select" value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
              {
                MESSAGE_STATE_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))
              }
            </select>
          </label>

          <label className="dq-input-stack">
            <span>{MESSAGES_COPY.OWNER}</span>
            <input value={ownerFilter} onChange={(e) => setOwnerFilter(e.target.value)} placeholder={MESSAGES_COPY.OWNER_PLACEHOLDER} autoComplete="off" />
          </label>
        </form>

        {error ? <div className="dq-error compact">{error}</div> : null}

        <div className="tags top-gap">
          <span>{MESSAGES_COPY.COUNTS.QUEUED} {fmt(counts[MESSAGE_STATE.QUEUED])}</span>
          <span>{MESSAGES_COPY.COUNTS.IN_FLIGHT} {fmt(counts[MESSAGE_STATE.IN_FLIGHT])}</span>
          <span>{MESSAGES_COPY.COUNTS.ACKED} {fmt(counts[MESSAGE_STATE.ACKED])}</span>
          <span>{MESSAGES_COPY.COUNTS.RETRIED} {fmt(counts[MESSAGE_STATE.RETRIED])}</span>
          <span>{MESSAGES_COPY.COUNTS.DEAD_LETTERED} {fmt(counts[MESSAGE_STATE.DEAD_LETTERED])}</span>
          <span>{loading ? COMMON_TEXT.REFRESHING : `${fmt(rows.length)} ${MESSAGES_COPY.COUNTS.ROWS}`}</span>
        </div>
      </section>

      <section className="dq-panel">
        <div className="dq-messages-table-scroll">
          <table>
            <thead>
              <tr>
                <th>{MESSAGES_COPY.TABLE_HEADERS.TOPIC}</th>
                <th className="right">{MESSAGES_COPY.TABLE_HEADERS.PARTITION}</th>
                <th className="right">{MESSAGES_COPY.TABLE_HEADERS.OFFSET}</th>
                <th>{MESSAGES_COPY.TABLE_HEADERS.STATE}</th>
                <th>{MESSAGES_COPY.TABLE_HEADERS.OWNER}</th>
                <th className="right">{MESSAGES_COPY.TABLE_HEADERS.ATTEMPTS}</th>
                <th>{MESSAGES_COPY.TABLE_HEADERS.LAST_DELIVERY}</th>
                <th className="right">{MESSAGES_COPY.TABLE_HEADERS.LEASE_AGE}</th>
                <th>{MESSAGES_COPY.TABLE_HEADERS.ERROR}</th>
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
                          row.state === MESSAGE_STATE.DEAD_LETTERED ? "red" :
                          row.state === MESSAGE_STATE.RETRIED ? "amber" :
                          row.state === MESSAGE_STATE.IN_FLIGHT ? "green" :
                          row.state === MESSAGE_STATE.ACKED ? "blue" :
                          "dim"
                        }>
                          {row.state}
                        </span>
                      </td>
                      <td>{row.owner || COMMON_TEXT.DASH}</td>
                      <td className="right">{fmt(row.attempts)}</td>
                      <td>{row.last_delivered_at_ms ? formatClock(row.last_delivered_at_ms) : COMMON_TEXT.DASH}</td>
                      <td className={`right ${row.stalled ? "red" : row.lease_age_ms > 0 ? "amber" : "dim"}`}>
                        {row.lease_age_ms > 0 ? `${fmt(row.lease_age_ms)}ms` : COMMON_TEXT.DASH}
                      </td>
                      <td className="dim">{row.last_error || COMMON_TEXT.DASH}</td>
                      <td className="right">
                        <button type="button" className="mini-btn" onClick={() => setSelectedID(id)}>
                          {selectedID === id ? MESSAGES_COPY.INSPECTING : MESSAGES_COPY.INSPECT}
                        </button>
                      </td>
                    </tr>
                  );
                })
              }
              {
                !rows.length ? (
                  <tr>
                    <td colSpan={10}>{loading ? MESSAGES_COPY.LOADING_STATE : MESSAGES_COPY.NO_MATCHING_MESSAGES}</td>
                  </tr>
                ) : null
              }
            </tbody>
          </table>
        </div>
      </section>

      <section className="dq-panel">
        <div className="row">
          <div>
            <strong>{MESSAGES_COPY.DETAIL_TITLE}</strong>
            <div className="dim">{selectedRow ? `${selectedRow.topic}:${selectedRow.partition}:${selectedRow.offset}` : MESSAGES_COPY.DETAIL_EMPTY}</div>
          </div>
        </div>

        {
          selectedRow ? (
            <div className="dq-stack">
              <div className="tags">
                <span>{selectedRow.state}</span>
                <span>{MESSAGES_COPY.OWNER.toLowerCase()} {selectedRow.owner || COMMON_TEXT.DASH}</span>
                <span>{MESSAGES_COPY.TABLE_HEADERS.ATTEMPTS.toLowerCase()} {selectedRow.attempts}</span>
                <span>{MESSAGES_COPY.LEASE_PREFIX} {selectedRow.lease_duration_ms > 0 ? `${fmt(selectedRow.lease_duration_ms)}ms` : COMMON_TEXT.DASH}</span>
                <span>{MESSAGES_COPY.EXPIRES_PREFIX} {selectedRow.lease_expires_at_ms ? formatClock(selectedRow.lease_expires_at_ms) : COMMON_TEXT.DASH}</span>
              </div>
              <pre className="dq-payload">{JSON.stringify(parseMessageValue(selectedRow.value || ""), null, 2)}</pre>
              {selectedRow.envelope ? <pre className="dq-payload">{JSON.stringify({ envelope: selectedRow.envelope }, null, 2)}</pre> : null}
              {selectedRow.routing ? <pre className="dq-payload">{JSON.stringify({ routing: selectedRow.routing }, null, 2)}</pre> : null}
            </div>
          ) : (
            <p className="dq-note">{MESSAGES_COPY.NO_MESSAGE_SELECTED}</p>
          )
        }
      </section>
    </div>
  );
}
