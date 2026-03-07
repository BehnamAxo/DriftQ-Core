import { useCallback, useEffect, useRef, useState } from "react";
import { getJSON, postJSON } from "../../../utils/http";
import { formatClock } from "../../../utils/time";

export default function WorkflowsTab({ runs, selectedRun, onSelectRun, onRunChanged }) {
  const [cancelReason, setCancelReason] = useState("dashboard cancel");
  const [replayFromStep, setReplayFromStep] = useState({});
  const [replayMode, setReplayMode] = useState({});
  const [selectedStepByRun, setSelectedStepByRun] = useState({});
  const [cancelingRunID, setCancelingRunID] = useState("");
  const [replayingRunID, setReplayingRunID] = useState("");
  const [startingDemoRun, setStartingDemoRun] = useState(false);
  const [actionError, setActionError] = useState("");
  const [actionSuccess, setActionSuccess] = useState("");
  const [artifactsByRun, setArtifactsByRun] = useState({});
  const [artifactErrors, setArtifactErrors] = useState({});
  const [artifactStatusByRun, setArtifactStatusByRun] = useState({});
  const [preview, setPreview] = useState({
    runID: "",
    artifactID: "",
    loading: false,
    error: "",
    contentType: "",
    body: ""
  });
  const artifactControllersRef = useRef({});

  const loadArtifacts = useCallback((runID, { force = false } = {}) => {
    if (!runID) {
      return () => {};
    }

    const artifactStatus = artifactStatusByRun[runID];
    if (!force && (artifactStatus === "loading" || artifactStatus === "loaded" || artifactStatus === "error")) {
      return () => {};
    }

    artifactControllersRef.current[runID]?.abort();

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(new Error("artifact list timed out")), 5000);
    artifactControllersRef.current[runID] = controller;
    setArtifactStatusByRun((prev) => ({ ...prev, [runID]: "loading" }));
    setArtifactErrors((prev) => ({ ...prev, [runID]: "" }));

    getJSON(`/debug/run-artifacts?run_id=${encodeURIComponent(runID)}&limit=50`, controller.signal)
      .then((payload) => {
        if (controller.signal.aborted) {
          return;
        }

        setArtifactsByRun((prev) => ({
          ...prev,
          [runID]: Array.isArray(payload.artifacts) ? payload.artifacts : []
        }));
        setArtifactStatusByRun((prev) => ({ ...prev, [runID]: "loaded" }));
      })
      .catch((err) => {
        if (controller.signal.aborted) {
          setArtifactErrors((prev) => ({
            ...prev,
            [runID]: err?.message || "failed to load artifacts"
          }));
          setArtifactStatusByRun((prev) => ({ ...prev, [runID]: "error" }));
          return;
        }

        setArtifactErrors((prev) => ({
          ...prev,
          [runID]: err?.message || "failed to load artifacts"
        }));
        setArtifactStatusByRun((prev) => ({ ...prev, [runID]: "error" }));
      })
      .finally(() => {
        clearTimeout(timeout);
        if (artifactControllersRef.current[runID] === controller) {
          delete artifactControllersRef.current[runID];
        }
      });

    return () => {
      clearTimeout(timeout);
      controller.abort();
    };
  }, [artifactStatusByRun]);

  useEffect(() => {
    if (!selectedRun) {
      return undefined;
    }

    return loadArtifacts(selectedRun);
  }, [loadArtifacts, selectedRun]);

  useEffect(() => () => {
    Object.values(artifactControllersRef.current).forEach((controller) => controller.abort());
  }, []);

  useEffect(() => {
    if (!selectedRun) {
      return;
    }

    const run = runs.find((item) => item.id === selectedRun);
    if (!run || !run.steps.length) {
      return;
    }

    setSelectedStepByRun((prev) => {
      if (prev[selectedRun]) {
        return prev;
      }

      return {
        ...prev,
        [selectedRun]: run.steps[0].name
      };
    });
  }, [runs, selectedRun]);

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

  async function handleReplay(runID) {
    setReplayingRunID(runID);
    setActionError("");
    setActionSuccess("");

    try {
      const fromStep = replayFromStep[runID]?.trim() || "";
      const mode = replayMode[runID] || "time_travel";
      await postJSON("/debug/run-replay", {
        run_id: runID,
        from_step: fromStep,
        mode
      });
      setArtifactsByRun((prev) => {
        const next = { ...prev };
        delete next[runID];
        return next;
      });
      setArtifactStatusByRun((prev) => {
        const next = { ...prev };
        delete next[runID];
        return next;
      });
      setArtifactErrors((prev) => {
        const next = { ...prev };
        delete next[runID];
        return next;
      });
      setActionSuccess(
        fromStep
          ? `replayed run ${runID} from ${fromStep} (${mode})`
          : `replayed run ${runID} (${mode})`
      );
      await onRunChanged?.();
      onSelectRun(runID, { toggle: false });
      loadArtifacts(runID, { force: true });
    } catch (err) {
      setActionError(err?.message || "failed to replay run");
    } finally {
      setReplayingRunID("");
    }
  }

  async function handleStartDemoRun() {
    setStartingDemoRun(true);
    setActionError("");
    setActionSuccess("");

    try {
      const payload = await postJSON("/debug/run-demo", { x: 1 });
      setActionSuccess(`started demo run ${payload?.run_id || ""}`.trim());
      await onRunChanged?.();
    } catch (err) {
      setActionError(err?.message || "failed to start demo run");
    } finally {
      setStartingDemoRun(false);
    }
  }

  async function handlePreviewArtifact(runID, artifact) {
    setPreview({
      runID,
      artifactID: artifact.artifact_id,
      loading: true,
      error: "",
      contentType: artifact.content_type || "",
      body: ""
    });

    try {
      const res = await fetch(`/debug/artifact-get?artifact_id=${encodeURIComponent(artifact.artifact_id)}`);
      if (!res.ok) {
        throw new Error(`artifact preview failed: ${res.status}`);
      }

      const contentType = res.headers.get("Content-Type") || artifact.content_type || "";
      const lowerType = contentType.toLowerCase();
      if (
        lowerType.includes("json") ||
        lowerType.startsWith("text/") ||
        lowerType.includes("javascript") ||
        lowerType.includes("xml")
      ) {
        const body = await res.text();
        setPreview({
          runID,
          artifactID: artifact.artifact_id,
          loading: false,
          error: "",
          contentType,
          body
        });
        return;
      }

      setPreview({
        runID,
        artifactID: artifact.artifact_id,
        loading: false,
        error: "binary artifact preview is not supported inline",
        contentType,
        body: ""
      });
    } catch (err) {
      setPreview({
        runID,
        artifactID: artifact.artifact_id,
        loading: false,
        error: err?.message || "failed to preview artifact",
        contentType: artifact.content_type || "",
        body: ""
      });
    }
  }

  function renderStatus(value) {
    const status = String(value || "").toLowerCase();
    const className =
      status === "completed"
        ? "green"
        : status === "failed"
          ? "red"
          : status === "canceled"
            ? "amber"
            : status === "waiting"
              ? "amber"
              : "blue";

    return <span className={className}>{status || "unknown"}</span>;
  }

  function handleStepSelect(runID, stepName) {
    setSelectedStepByRun((prev) => ({
      ...prev,
      [runID]: stepName
    }));
    setReplayFromStep((prev) => ({
      ...prev,
      [runID]: stepName
    }));
    onSelectRun(runID, { toggle: false });
    loadArtifacts(runID);
  }

  return (
    <section className="dq-stack">
      {actionError ? <div className="dq-error compact">{actionError}</div> : null}
      {actionSuccess ? <div className="dq-success">{actionSuccess}</div> : null}

      <div className="dq-panel dq-workflow-toolbar">
        <div>
          <strong>Workflow Controls</strong>
          <p className="dq-note">Inspect runs, replay from a step, cancel active work, and inspect artifacts.</p>
        </div>
        <button type="button" className="mini-btn" onClick={handleStartDemoRun} disabled={startingDemoRun}>
          {startingDemoRun ? "Starting..." : "Run Demo"}
        </button>
      </div>

      {
        runs.map((r) => {
          const selectedStepName = selectedStepByRun[r.id] || r.steps[0]?.name || "";
          const selectedStep = r.steps.find((step) => step.name === selectedStepName) || null;
          const artifactItems = artifactsByRun[r.id] || [];
          const artifactStatus = artifactStatusByRun[r.id] || "idle";
          const artifactsLoading = artifactStatus === "loading";
          const replayFrom = replayFromStep[r.id] || "";
          const selectedReplayStep = r.steps.find((step) => step.name === replayFrom) || null;
          const replayHint =
            replayMode[r.id] === "live"
              ? "live replay forces the selected step and downstream steps to run again."
              : selectedReplayStep
                ? "time_travel reuses succeeded outputs. If this step already succeeded and there is nothing downstream to rerun, the replay will be a no-op."
                : "time_travel reuses succeeded outputs when possible.";

          return (
          <div className="dq-panel run" key={r.id}>
            <div className="row">
              <strong>{r.id}</strong>
              <div className="dq-workflow-header-actions">
                {renderStatus(r.status)}
                <button type="button" className="mini-btn" onClick={() => onSelectRun(r.id)}>
                  {selectedRun === r.id ? "Hide" : "Inspect"}
                </button>
              </div>
            </div>
            <div className="dim small">
              workflow {r.workflowId || "-"} | started {formatClock(r.startedAt)}
            </div>
            <div className="steps">
              {
                r.steps.map((s) => (
                  <button
                    key={`${r.id}-${s.name}`}
                    type="button"
                    className={`step ${s.status} ${selectedStepName === s.name ? "active" : ""}`}
                    onClick={() => handleStepSelect(r.id, s.name)}
                  >
                    <span>{s.name}</span>
                    <small>{s.duration ? `${Math.round(s.duration)}ms` : `attempt ${s.attempts || 1}`}</small>
                  </button>
                ))
              }
            </div>
            {
              selectedRun === r.id ? (
                <div className="timeline">
                  <div className="dq-workflow-summary dq-kv-grid">
                    <div className="dq-kv-row">
                      <span className="dq-kv-label">Status</span>
                      <strong className="dq-kv-value">{r.status}</strong>
                    </div>
                    <div className="dq-kv-row">
                      <span className="dq-kv-label">Workflow</span>
                      <strong className="dq-kv-value">{r.workflowId || "-"}</strong>
                    </div>
                    <div className="dq-kv-row">
                      <span className="dq-kv-label">Started</span>
                      <strong className="dq-kv-value">{formatClock(r.startedAt)}</strong>
                    </div>
                    <div className="dq-kv-row">
                      <span className="dq-kv-label">Ended</span>
                      <strong className="dq-kv-value">{formatClock(r.endedAt)}</strong>
                    </div>
                    <div className="dq-kv-row">
                      <span className="dq-kv-label">Duration</span>
                      <strong className="dq-kv-value">{r.duration ? `${Math.round(r.duration)}ms` : "-"}</strong>
                    </div>
                    <div className="dq-kv-row">
                      <span className="dq-kv-label">Terminal Reason</span>
                      <strong className="dq-kv-value">{r.terminalReason || "-"}</strong>
                    </div>
                  </div>

                  <div className="dq-workflow-counts">
                    <span className="green">done {r.counts.completed || 0}</span>
                    <span className="blue">running {r.counts.running || 0}</span>
                    <span className="amber">waiting {r.counts.waiting || 0}</span>
                    <span className="red">failed {r.counts.failed || 0}</span>
                    <span className="amber">canceled {r.counts.canceled || 0}</span>
                  </div>

                  <div className="dq-workflow-step-table">
                    <div className="dq-table-row dq-table-head">
                      <span>Step</span>
                      <span>Status</span>
                      <span>Attempts</span>
                      <span>Duration</span>
                      <span>Bytes</span>
                      <span>Error</span>
                    </div>
                    {
                      r.steps.map((s) => (
                        <div key={`${r.id}-tl-${s.name}`} className="dq-table-row">
                          <span>{s.name}</span>
                          <span>{s.status}</span>
                          <span>{s.attempts || 1}</span>
                          <span>{s.duration ? `${Math.round(s.duration)}ms` : "-"}</span>
                          <span>{`${s.inputBytes}/${s.outputBytes}`}</span>
                          <span className="dim">{s.error || "-"}</span>
                        </div>
                      ))
                    }
                  </div>

                  {
                    selectedStep ? (
                      <div className="dq-panel dq-workflow-step-focus">
                        <div className="row">
                          <strong>Step Detail</strong>
                          <span className="dim small">{selectedStep.name}</span>
                        </div>
                        <div className="dq-workflow-summary dq-kv-grid">
                          <div className="dq-kv-row">
                            <span className="dq-kv-label">Status</span>
                            <strong className="dq-kv-value">{selectedStep.status}</strong>
                          </div>
                          <div className="dq-kv-row">
                            <span className="dq-kv-label">Attempts</span>
                            <strong className="dq-kv-value">{selectedStep.attempts || 1}</strong>
                          </div>
                          <div className="dq-kv-row">
                            <span className="dq-kv-label">Duration</span>
                            <strong className="dq-kv-value">{selectedStep.duration ? `${Math.round(selectedStep.duration)}ms` : "-"}</strong>
                          </div>
                          <div className="dq-kv-row">
                            <span className="dq-kv-label">Started</span>
                            <strong className="dq-kv-value">{formatClock(selectedStep.startedAt)}</strong>
                          </div>
                          <div className="dq-kv-row">
                            <span className="dq-kv-label">Ended</span>
                            <strong className="dq-kv-value">{formatClock(selectedStep.endedAt)}</strong>
                          </div>
                          <div className="dq-kv-row">
                            <span className="dq-kv-label">I/O Bytes</span>
                            <strong className="dq-kv-value">{`${selectedStep.inputBytes}/${selectedStep.outputBytes}`}</strong>
                          </div>
                        </div>
                        <div className="dq-note">
                          {selectedStep.error ? `error: ${selectedStep.error}` : "No step error recorded."}
                        </div>
                      </div>
                    ) : null
                  }

                  <div className="dq-workflow-actions">
                    <label className="dq-input-stack small">
                      <span>Replay From</span>
                      <select
                        value={replayFrom}
                        onChange={(e) => {
                          const nextStep = e.target.value;
                          setReplayFromStep((prev) => ({ ...prev, [r.id]: nextStep }));
                          if (nextStep) {
                            setSelectedStepByRun((prev) => ({ ...prev, [r.id]: nextStep }));
                          }
                        }}
                        disabled={replayingRunID === r.id}
                      >
                        <option value="">entire run</option>
                        {r.steps.map((step) => <option key={`${r.id}-replay-${step.name}`} value={step.name}>{step.name}</option>)}
                      </select>
                    </label>

                    <label className="dq-input-stack small">
                      <span>Replay Mode</span>
                      <select
                        value={replayMode[r.id] || "time_travel"}
                        onChange={(e) => setReplayMode((prev) => ({ ...prev, [r.id]: e.target.value }))}
                        disabled={replayingRunID === r.id}
                      >
                        <option value="time_travel">time_travel</option>
                        <option value="live">live</option>
                      </select>
                    </label>

                    <label className="dq-input-stack">
                      <span>Cancel Reason</span>
                      <input
                        value={cancelReason}
                        onChange={(e) => setCancelReason(e.target.value)}
                        placeholder="dashboard cancel"
                        disabled={!r.cancelable || cancelingRunID === r.id}
                      />
                    </label>

                    <div className="dq-form-actions dq-workflow-action-buttons">
                      <button
                        type="button"
                        className="mini-btn"
                        onClick={() => handleReplay(r.id)}
                        disabled={replayingRunID === r.id}
                      >
                        {replayingRunID === r.id ? "Replaying..." : "Replay Run"}
                      </button>
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

                  <div className="dq-note">{replayHint}</div>

                  <div className="dq-workflow-artifacts">
                    <div className="row">
                      <strong>Artifacts</strong>
                      <span className="dim small">
                        {artifactsLoading ? "loading..." : `${artifactItems.length} items`}
                      </span>
                    </div>

                    {artifactErrors[r.id] ? <div className="dq-error compact">{artifactErrors[r.id]}</div> : null}

                    {
                      artifactItems.length > 0 ? (
                        <div className="dq-workflow-step-table">
                          <div className="dq-table-row dq-table-head">
                            <span>Artifact</span>
                            <span>Node</span>
                            <span>Type</span>
                            <span>Size</span>
                            <span>Created</span>
                            <span>Actions</span>
                          </div>
                          {
                            artifactItems.map((artifact) => (
                              <div key={artifact.artifact_id} className="dq-table-row">
                                <span>{artifact.original_name || artifact.artifact_id}</span>
                                <span>{artifact.node_id || "-"}</span>
                                <span>{artifact.content_type || "-"}</span>
                                <span>{artifact.size || 0}</span>
                                <span>{formatClock(artifact.created_at)}</span>
                                <span className="dq-inline-actions">
                                  <button
                                    type="button"
                                    className="mini-btn"
                                    onClick={() => handlePreviewArtifact(r.id, artifact)}
                                  >
                                    Preview
                                  </button>
                                  <a
                                    className="mini-btn"
                                    href={`/debug/artifact-get?artifact_id=${encodeURIComponent(artifact.artifact_id)}`}
                                    target="_blank"
                                    rel="noreferrer"
                                  >
                                    Open
                                  </a>
                                </span>
                              </div>
                            ))
                          }
                        </div>
                      ) : !artifactsLoading ? (
                        <div className="dim small">No artifacts recorded for this run yet.</div>
                      ) : null
                    }

                    {
                      preview.runID === r.id && preview.artifactID ? (
                        <div className="dq-panel dq-artifact-preview">
                          <div className="row">
                            <strong>Artifact Preview</strong>
                            <span className="dim small">{preview.contentType || "unknown type"}</span>
                          </div>
                          {preview.loading ? <div className="dim small">loading preview...</div> : null}
                          {!preview.loading && preview.error ? <div className="dq-error compact">{preview.error}</div> : null}
                          {!preview.loading && !preview.error && preview.body ? (
                            <pre className="dq-payload">{preview.body}</pre>
                          ) : null}
                        </div>
                      ) : null
                    }
                  </div>
                </div>
              ) : null
            }
          </div>
        );
        })
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
