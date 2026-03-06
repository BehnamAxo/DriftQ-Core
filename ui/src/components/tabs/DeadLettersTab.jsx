import { fmt } from "../../utils/number";
import { formatClock } from "../../utils/time";

function parseMessageValue(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return { raw };
  }
}

export default function DeadLettersTab({ dlqTopic, onDlqTopicChange, dlqMessages, selectedDLQ, onToggleInspect }) {
  const selectedMessage = selectedDLQ ? dlqMessages.find((m) => m.id === selectedDLQ) : null;

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
            <th>Topic</th>
            <th>Reason</th>
            <th className="right">Retries</th>
            <th>Failed At</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {dlqMessages.map((m) => (
            <tr key={m.id}>
              <td>{m.id}</td>
              <td>{m.topic}</td>
              <td className="amber">{m.reason}</td>
              <td className="right red">{fmt(m.retries)}</td>
              <td>{m.failedAt ? formatClock(m.failedAt) : "-"}</td>
              <td>
                <button type="button" className="mini-btn" onClick={() => onToggleInspect(m.id)}>
                  {selectedDLQ === m.id ? "Close" : "Inspect"}
                </button>
              </td>
            </tr>
          ))}
          {
            dlqMessages.length === 0 ? (
              <tr>
                <td colSpan={6}>no DLQ messages</td>
              </tr>
            ) : null
          }
        </tbody>
      </table>
      {selectedMessage ? <pre className="dq-payload">{JSON.stringify(parseMessageValue(selectedMessage.value || ""), null, 2)}</pre> : null}
    </section>
  );
}
