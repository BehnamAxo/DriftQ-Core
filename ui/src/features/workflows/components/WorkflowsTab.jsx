import { useState } from "react";
import { postJSON } from "../../../utils/http";
import { formatClock } from "../../../utils/time";

export default function WorkflowsTab({ runs, selectedRun, onSelectRun, onRunChanged }) {
  const [cancelReason, setCancelReason] = useState("dashboard cancel");
  const [cancelingRunID, setCancelingRunID] = useState("");
  const [actionError, setActionError] = useState("");
  const [actionSuccess, setActionSuccess] = useState("");

  async function handleCancel(runID) {
    setCancelingRunID(runID);
    setActionError("");
    setActionSuccess("");

    try {
      await postJSON("/debug/run-cancel", {
        run_id: runID,
        reason: cancelReason.trim()
      });
      setActionSuccess(`canceled run ${runID}`);
      await onRunChanged?.();
    } catch (err) {
      setActionError(err?.message || "failed to cancel run");
    } finally {
      setCancelingRunID("");
    }
  }

  return (
    <section className="dq-stack">
      {actionError ? <div className="dq-error compact">{actionError}</div> : null}
      {actionSuccess ? <div className="dq-success">{actionSuccess}</div> : null}

      {
        runs.map((r) => (
          <div className="dq-panel run" key={r.id}>
            <div className="row">
              <strong>{r.id}</strong>
              <span className={r.status === "completed" ? "green" : r.status === "failed" ? "red" : r.status === "canceled" ? "amber" : "blue"}>{r.status}</span>
            </div>
            <div className="dim small">started {formatClock(r.startedAt)}</div>
            <div className="steps">
              {
                r.steps.map((s) => (
                  <button key={`${r.id}-${s.name}`} type="button" className={`step ${s.status}`} onClick={() => onSelectRun(r.id)}>
                    <span>{s.name}</span>
                    <small>{s.duration ? `${Math.round(s.duration)}ms` : "-"}</small>
                  </button>
                ))
              }
            </div>
            {
              selectedRun === r.id ? (
                <div className="timeline">
                  {
                    r.steps.map((s) => (
                      <div key={`${r.id}-tl-${s.name}`}>
                        <span>{s.name}</span>
                        <span>{s.duration ? `${Math.round(s.duration)}ms` : "-"}</span>
                      </div>
                    ))
                  }

                  <div className="dq-workflow-actions">
                    <label className="dq-input-stack">
                      <span>Cancel Reason</span>
                      <input
                        value={cancelReason}
                        onChange={(e) => setCancelReason(e.target.value)}
                        placeholder="dashboard cancel"
                        disabled={!r.cancelable || cancelingRunID === r.id}
                      />
                    </label>

                    <div className="dq-form-actions">
                      <button
                        type="button"
                        className="mini-btn danger"
                        onClick={() => handleCancel(r.id)}
                        disabled={!r.cancelable || cancelingRunID === r.id}
                      >
                        {cancelingRunID === r.id ? "Canceling..." : "Cancel Run"}
                      </button>
                    </div>
                  </div>
                </div>
              ) : null
            }
          </div>
        ))
      }

      {
        runs.length === 0 ? (
          <div className="dq-panel">
            No workflow runs yet. Start one with <code>POST /debug/run-demo</code>.
          </div>
        ) : null
      }
    </section>
  );
}
