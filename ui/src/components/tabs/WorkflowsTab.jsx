import { formatClock } from "../../utils/time";

export default function WorkflowsTab({ runs, selectedRun, onSelectRun }) {
  return (
    <section className="dq-stack">
      {
        runs.map((r) => (
          <div className="dq-panel run" key={r.id}>
            <div className="row">
              <strong>{r.id}</strong>
              <span className={r.status === "completed" ? "green" : r.status === "failed" ? "red" : "blue"}>{r.status}</span>
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
