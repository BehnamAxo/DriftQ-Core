import { API_PATHS, COMMON_TEXT, DEFAULTS, WORKFLOW_REPLAY_MODE, WORKFLOW_STATUS, WORKFLOWS_COPY, UI_LIMITS } from "../../../constants/ui";
import { formatClock } from "../../../utils/time";
import { getJSON, postJSON } from "../../../utils/http";
import { useCallback, useEffect, useRef, useState } from "react";

export default function WorkflowsTab({ runs, selectedRun, onSelectRun, onRunChanged }) {
  const [cancelReason, setCancelReason] = useState(DEFAULTS.WORKFLOW_CANCEL_REASON);
  const [replayFromStep, setReplayFromStep] = useState({});
  const [replayMode, setReplayMode] = useState({});
  const [selectedStepByRun, setSelectedStepByRun] = useState({});
  const [cancelingRunID, setCancelingRunID] = useState(COMMON_TEXT.EMPTY);
  const [replayingRunID, setReplayingRunID] = useState(COMMON_TEXT.EMPTY);
  const [startingDemoRun, setStartingDemoRun] = useState(false);
  const [actionError, setActionError] = useState(COMMON_TEXT.EMPTY);
  const [actionSuccess, setActionSuccess] = useState(COMMON_TEXT.EMPTY);
  const [artifactsByRun, setArtifactsByRun] = useState({});
  const [artifactErrors, setArtifactErrors] = useState({});
  const [artifactStatusByRun, setArtifactStatusByRun] = useState({});
  const [preview, setPreview] = useState({
    runID: COMMON_TEXT.EMPTY,
    artifactID: COMMON_TEXT.EMPTY,
    loading: false,
    error: COMMON_TEXT.EMPTY,
    contentType: COMMON_TEXT.EMPTY,
    body: COMMON_TEXT.EMPTY
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
    const timeout = setTimeout(() => controller.abort(new Error(WORKFLOWS_COPY.ARTIFACT_LIST_TIMED_OUT)), UI_LIMITS.WORKFLOW_ARTIFACT_TIMEOUT_MS);
    artifactControllersRef.current[runID] = controller;
    setArtifactStatusByRun((prev) => ({ ...prev, [runID]: WORKFLOW_STATUS.LOADING }));
    setArtifactErrors((prev) => ({ ...prev, [runID]: COMMON_TEXT.EMPTY }));

    getJSON(API_PATHS.runArtifacts(runID, UI_LIMITS.WORKFLOW_ARTIFACT_LIMIT), controller.signal)
      .then((payload) => {
        if (controller.signal.aborted) {
          return;
        }

        setArtifactsByRun((prev) => ({
          ...prev,
          [runID]: Array.isArray(payload.artifacts) ? payload.artifacts : []
        }));
        setArtifactStatusByRun((prev) => ({ ...prev, [runID]: WORKFLOW_STATUS.LOADED }));
      })
      .catch((err) => {
        if (controller.signal.aborted) {
          setArtifactErrors((prev) => ({
            ...prev,
            [runID]: err?.message || WORKFLOWS_COPY.LOAD_ARTIFACTS_FAILED
          }));
          setArtifactStatusByRun((prev) => ({ ...prev, [runID]: WORKFLOW_STATUS.ERROR }));
          return;
        }

        setArtifactErrors((prev) => ({
          ...prev,
          [runID]: err?.message || WORKFLOWS_COPY.LOAD_ARTIFACTS_FAILED
        }));
        setArtifactStatusByRun((prev) => ({ ...prev, [runID]: WORKFLOW_STATUS.ERROR }));
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
    setActionError(COMMON_TEXT.EMPTY);
    setActionSuccess(COMMON_TEXT.EMPTY);

    try {
      await postJSON(API_PATHS.RUN_CANCEL, {
        run_id: runID,
        reason: cancelReason.trim()
      });
      setActionSuccess(`${WORKFLOWS_COPY.CANCELED_RUN_PREFIX} ${runID}`);
      await onRunChanged?.();
    } catch (err) {
      setActionError(err?.message || WORKFLOWS_COPY.CANCEL_FAILED);
    } finally {
      setCancelingRunID(COMMON_TEXT.EMPTY);
    }
  }

  async function handleReplay(runID) {
    setReplayingRunID(runID);
    setActionError(COMMON_TEXT.EMPTY);
    setActionSuccess(COMMON_TEXT.EMPTY);

    try {
      const fromStep = replayFromStep[runID]?.trim() || COMMON_TEXT.EMPTY;
      const mode = replayMode[runID] || WORKFLOW_REPLAY_MODE.TIME_TRAVEL;
      await postJSON(API_PATHS.RUN_REPLAY, {
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
          ? `${WORKFLOWS_COPY.REPLAYED_RUN_PREFIX} ${runID} ${WORKFLOWS_COPY.FROM} ${fromStep} (${mode})`
          : `${WORKFLOWS_COPY.REPLAYED_RUN_PREFIX} ${runID} (${mode})`
      );
      await onRunChanged?.();
      onSelectRun(runID, { toggle: false });
      loadArtifacts(runID, { force: true });
    } catch (err) {
      setActionError(err?.message || WORKFLOWS_COPY.REPLAY_FAILED);
    } finally {
      setReplayingRunID(COMMON_TEXT.EMPTY);
    }
  }

  async function handleStartDemoRun() {
    setStartingDemoRun(true);
    setActionError(COMMON_TEXT.EMPTY);
    setActionSuccess(COMMON_TEXT.EMPTY);

    try {
      const payload = await postJSON(API_PATHS.RUN_DEMO, { x: 1 });
      setActionSuccess(`${WORKFLOWS_COPY.STARTED_DEMO_RUN_PREFIX} ${payload?.run_id || COMMON_TEXT.EMPTY}`.trim());
      await onRunChanged?.();
    } catch (err) {
      setActionError(err?.message || WORKFLOWS_COPY.START_DEMO_FAILED);
    } finally {
      setStartingDemoRun(false);
    }
  }

  async function handlePreviewArtifact(runID, artifact) {
    setPreview({
      runID,
      artifactID: artifact.artifact_id,
      loading: true,
      error: COMMON_TEXT.EMPTY,
      contentType: artifact.content_type || COMMON_TEXT.EMPTY,
      body: COMMON_TEXT.EMPTY
    });

    try {
      const res = await fetch(API_PATHS.artifactGet(artifact.artifact_id));
      if (!res.ok) {
        throw new Error(`${WORKFLOWS_COPY.ARTIFACT_PREVIEW_FAILED_PREFIX} ${res.status}`);
      }

      const contentType = res.headers.get("Content-Type") || artifact.content_type || COMMON_TEXT.EMPTY;
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
          error: COMMON_TEXT.EMPTY,
          contentType,
          body
        });
        return;
      }

      setPreview({
        runID,
        artifactID: artifact.artifact_id,
        loading: false,
        error: WORKFLOWS_COPY.BINARY_PREVIEW_UNSUPPORTED,
        contentType,
        body: COMMON_TEXT.EMPTY
      });
    } catch (err) {
      setPreview({
        runID,
        artifactID: artifact.artifact_id,
        loading: false,
        error: err?.message || WORKFLOWS_COPY.PREVIEW_FAILED,
        contentType: artifact.content_type || COMMON_TEXT.EMPTY,
        body: COMMON_TEXT.EMPTY
      });
    }
  }

  function renderStatus(value) {
    const status = String(value || COMMON_TEXT.EMPTY).toLowerCase();
    const className =
      status === WORKFLOW_STATUS.COMPLETED
        ? "green"
        : status === WORKFLOW_STATUS.FAILED
          ? "red"
          : status === WORKFLOW_STATUS.CANCELED
            ? "amber"
            : status === WORKFLOW_STATUS.WAITING
              ? "amber"
              : "blue";

    return <span className={className}>{status || WORKFLOWS_COPY.STEP_STATUS_EMPTY}</span>;
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
          <strong>{WORKFLOWS_COPY.CONTROLS_TITLE}</strong>
          <p className="dq-note">{WORKFLOWS_COPY.CONTROLS_DESCRIPTION}</p>
        </div>
        <button type="button" className="mini-btn" onClick={handleStartDemoRun} disabled={startingDemoRun}>
          {startingDemoRun ? WORKFLOWS_COPY.STARTING : WORKFLOWS_COPY.RUN_DEMO}
        </button>
      </div>

      {
        runs.map((r) => {
          const selectedStepName = selectedStepByRun[r.id] || r.steps[0]?.name || "";
          const selectedStep = r.steps.find((step) => step.name === selectedStepName) || null;
          const artifactItems = artifactsByRun[r.id] || [];
          const artifactStatus = artifactStatusByRun[r.id] || WORKFLOW_STATUS.IDLE;
          const artifactsLoading = artifactStatus === WORKFLOW_STATUS.LOADING;
          const replayFrom = replayFromStep[r.id] || COMMON_TEXT.EMPTY;
          const selectedReplayStep = r.steps.find((step) => step.name === replayFrom) || null;
          const replayHint =
            replayMode[r.id] === WORKFLOW_REPLAY_MODE.LIVE
              ? WORKFLOWS_COPY.LIVE_REPLAY_HINT
              : selectedReplayStep
                ? WORKFLOWS_COPY.TIME_TRAVEL_SELECTED_HINT
                : WORKFLOWS_COPY.TIME_TRAVEL_HINT;

          return (
          <div className="dq-panel run" key={r.id}>
            <div className="row">
              <strong>{r.id}</strong>
              <div className="dq-workflow-header-actions">
                {renderStatus(r.status)}
                <button type="button" className="mini-btn" onClick={() => onSelectRun(r.id)}>
                  {selectedRun === r.id ? WORKFLOWS_COPY.HIDE : WORKFLOWS_COPY.INSPECT}
                </button>
              </div>
            </div>
            <div className="dim small">
              {WORKFLOWS_COPY.WORKFLOW_PREFIX} {r.workflowId || COMMON_TEXT.DASH} | {WORKFLOWS_COPY.STARTED_PREFIX} {formatClock(r.startedAt)}
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
                    <small>{s.duration ? `${Math.round(s.duration)}ms` : `${WORKFLOWS_COPY.STEP_ACTION_ATTEMPT_PREFIX} ${s.attempts || 1}`}</small>
                  </button>
                ))
              }
            </div>
            {
              selectedRun === r.id ? (
                <div className="timeline">
                  <div className="dq-workflow-summary dq-kv-grid">
                    <div className="dq-kv-row">
                      <span className="dq-kv-label">{WORKFLOWS_COPY.SUMMARY_LABELS.STATUS}</span>
                      <strong className="dq-kv-value">{r.status}</strong>
                    </div>
                    <div className="dq-kv-row">
                      <span className="dq-kv-label">{WORKFLOWS_COPY.SUMMARY_LABELS.WORKFLOW}</span>
                      <strong className="dq-kv-value">{r.workflowId || COMMON_TEXT.DASH}</strong>
                    </div>
                    <div className="dq-kv-row">
                      <span className="dq-kv-label">{WORKFLOWS_COPY.SUMMARY_LABELS.STARTED}</span>
                      <strong className="dq-kv-value">{formatClock(r.startedAt)}</strong>
                    </div>
                    <div className="dq-kv-row">
                      <span className="dq-kv-label">{WORKFLOWS_COPY.SUMMARY_LABELS.ENDED}</span>
                      <strong className="dq-kv-value">{formatClock(r.endedAt)}</strong>
                    </div>
                    <div className="dq-kv-row">
                      <span className="dq-kv-label">{WORKFLOWS_COPY.SUMMARY_LABELS.DURATION}</span>
                      <strong className="dq-kv-value">{r.duration ? `${Math.round(r.duration)}ms` : COMMON_TEXT.DASH}</strong>
                    </div>
                    <div className="dq-kv-row">
                      <span className="dq-kv-label">{WORKFLOWS_COPY.SUMMARY_LABELS.TERMINAL_REASON}</span>
                      <strong className="dq-kv-value">{r.terminalReason || COMMON_TEXT.DASH}</strong>
                    </div>
                  </div>

                  <div className="dq-workflow-counts">
                    <span className="green">{WORKFLOWS_COPY.COUNTS.DONE} {r.counts.completed || 0}</span>
                    <span className="blue">{WORKFLOWS_COPY.COUNTS.RUNNING} {r.counts.running || 0}</span>
                    <span className="amber">{WORKFLOWS_COPY.COUNTS.WAITING} {r.counts.waiting || 0}</span>
                    <span className="red">{WORKFLOWS_COPY.COUNTS.FAILED} {r.counts.failed || 0}</span>
                    <span className="amber">{WORKFLOWS_COPY.COUNTS.CANCELED} {r.counts.canceled || 0}</span>
                  </div>

                  <div className="dq-workflow-step-table">
                    <div className="dq-table-row dq-table-head">
                      <span>{WORKFLOWS_COPY.STEP_TABLE_HEADERS.STEP}</span>
                      <span>{WORKFLOWS_COPY.STEP_TABLE_HEADERS.STATUS}</span>
                      <span>{WORKFLOWS_COPY.STEP_TABLE_HEADERS.ATTEMPTS}</span>
                      <span>{WORKFLOWS_COPY.STEP_TABLE_HEADERS.DURATION}</span>
                      <span>{WORKFLOWS_COPY.STEP_TABLE_HEADERS.BYTES}</span>
                      <span>{WORKFLOWS_COPY.STEP_TABLE_HEADERS.ERROR}</span>
                    </div>
                    {
                      r.steps.map((s) => (
                        <div key={`${r.id}-tl-${s.name}`} className="dq-table-row">
                          <span>{s.name}</span>
                          <span>{s.status}</span>
                          <span>{s.attempts || 1}</span>
                          <span>{s.duration ? `${Math.round(s.duration)}ms` : COMMON_TEXT.DASH}</span>
                          <span>{`${s.inputBytes}/${s.outputBytes}`}</span>
                          <span className="dim">{s.error || COMMON_TEXT.DASH}</span>
                        </div>
                      ))
                    }
                  </div>

                  {
                    selectedStep ? (
                      <div className="dq-panel dq-workflow-step-focus">
                        <div className="row">
                          <strong>{WORKFLOWS_COPY.STEP_DETAIL_TITLE}</strong>
                          <span className="dim small">{selectedStep.name}</span>
                        </div>
                        <div className="dq-workflow-summary dq-kv-grid">
                          <div className="dq-kv-row">
                            <span className="dq-kv-label">{WORKFLOWS_COPY.SUMMARY_LABELS.STATUS}</span>
                            <strong className="dq-kv-value">{selectedStep.status}</strong>
                          </div>
                          <div className="dq-kv-row">
                            <span className="dq-kv-label">{WORKFLOWS_COPY.STEP_TABLE_HEADERS.ATTEMPTS}</span>
                            <strong className="dq-kv-value">{selectedStep.attempts || 1}</strong>
                          </div>
                          <div className="dq-kv-row">
                            <span className="dq-kv-label">{WORKFLOWS_COPY.SUMMARY_LABELS.DURATION}</span>
                            <strong className="dq-kv-value">{selectedStep.duration ? `${Math.round(selectedStep.duration)}ms` : COMMON_TEXT.DASH}</strong>
                          </div>
                          <div className="dq-kv-row">
                            <span className="dq-kv-label">{WORKFLOWS_COPY.SUMMARY_LABELS.STARTED}</span>
                            <strong className="dq-kv-value">{formatClock(selectedStep.startedAt)}</strong>
                          </div>
                          <div className="dq-kv-row">
                            <span className="dq-kv-label">{WORKFLOWS_COPY.SUMMARY_LABELS.ENDED}</span>
                            <strong className="dq-kv-value">{formatClock(selectedStep.endedAt)}</strong>
                          </div>
                          <div className="dq-kv-row">
                            <span className="dq-kv-label">{WORKFLOWS_COPY.IO_BYTES}</span>
                            <strong className="dq-kv-value">{`${selectedStep.inputBytes}/${selectedStep.outputBytes}`}</strong>
                          </div>
                        </div>
                        <div className="dq-note">
                          {selectedStep.error ? `${WORKFLOWS_COPY.STEP_ERROR_PREFIX} ${selectedStep.error}` : WORKFLOWS_COPY.NO_STEP_ERROR}
                        </div>
                      </div>
                    ) : null
                  }

                  <div className="dq-workflow-actions">
                    <label className="dq-input-stack small">
                      <span>{WORKFLOWS_COPY.REPLAY_FROM}</span>
                      <select
                        className="dq-select"
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
                        <option value={COMMON_TEXT.EMPTY}>{WORKFLOWS_COPY.ENTIRE_RUN}</option>
                        {r.steps.map((step) => <option key={`${r.id}-replay-${step.name}`} value={step.name}>{step.name}</option>)}
                      </select>
                    </label>

                    <label className="dq-input-stack small">
                      <span>{WORKFLOWS_COPY.REPLAY_MODE}</span>
                      <select
                        className="dq-select"
                        value={replayMode[r.id] || WORKFLOW_REPLAY_MODE.TIME_TRAVEL}
                        onChange={(e) => setReplayMode((prev) => ({ ...prev, [r.id]: e.target.value }))}
                        disabled={replayingRunID === r.id}
                      >
                        <option value={WORKFLOW_REPLAY_MODE.TIME_TRAVEL}>{WORKFLOW_REPLAY_MODE.TIME_TRAVEL}</option>
                        <option value={WORKFLOW_REPLAY_MODE.LIVE}>{WORKFLOW_REPLAY_MODE.LIVE}</option>
                      </select>
                    </label>

                    <label className="dq-input-stack">
                      <span>{WORKFLOWS_COPY.CANCEL_REASON}</span>
                      <input
                        value={cancelReason}
                        onChange={(e) => setCancelReason(e.target.value)}
                        placeholder={WORKFLOWS_COPY.CANCEL_REASON_PLACEHOLDER}
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
                        {replayingRunID === r.id ? WORKFLOWS_COPY.REPLAYING : WORKFLOWS_COPY.REPLAY_RUN}
                      </button>
                      <button
                        type="button"
                        className="mini-btn danger"
                        onClick={() => handleCancel(r.id)}
                        disabled={!r.cancelable || cancelingRunID === r.id}
                      >
                        {cancelingRunID === r.id ? WORKFLOWS_COPY.CANCELING : WORKFLOWS_COPY.CANCEL_RUN}
                      </button>
                    </div>
                  </div>

                  <div className="dq-note">{replayHint}</div>

                  <div className="dq-workflow-artifacts">
                    <div className="row">
                      <strong>{WORKFLOWS_COPY.ARTIFACTS}</strong>
                      <span className="dim small">
                        {artifactsLoading ? WORKFLOWS_COPY.LOADING : `${artifactItems.length} ${WORKFLOWS_COPY.ITEMS}`}
                      </span>
                    </div>

                    {artifactErrors[r.id] ? <div className="dq-error compact">{artifactErrors[r.id]}</div> : null}

                    {
                      artifactItems.length > 0 ? (
                        <div className="dq-workflow-step-table">
                          <div className="dq-table-row dq-table-head">
                            <span>{WORKFLOWS_COPY.ARTIFACT_HEADERS.ARTIFACT}</span>
                            <span>{WORKFLOWS_COPY.ARTIFACT_HEADERS.NODE}</span>
                            <span>{WORKFLOWS_COPY.ARTIFACT_HEADERS.TYPE}</span>
                            <span>{WORKFLOWS_COPY.ARTIFACT_HEADERS.SIZE}</span>
                            <span>{WORKFLOWS_COPY.ARTIFACT_HEADERS.CREATED}</span>
                            <span>{WORKFLOWS_COPY.ARTIFACT_HEADERS.ACTIONS}</span>
                          </div>
                          {
                            artifactItems.map((artifact) => (
                              <div key={artifact.artifact_id} className="dq-table-row">
                                <span>{artifact.original_name || artifact.artifact_id}</span>
                                <span>{artifact.node_id || COMMON_TEXT.DASH}</span>
                                <span>{artifact.content_type || COMMON_TEXT.DASH}</span>
                                <span>{artifact.size || 0}</span>
                                <span>{formatClock(artifact.created_at)}</span>
                                <span className="dq-inline-actions">
                                  <button
                                    type="button"
                                    className="mini-btn"
                                    onClick={() => handlePreviewArtifact(r.id, artifact)}
                                  >
                                    {WORKFLOWS_COPY.PREVIEW}
                                  </button>
                                  <a
                                    className="mini-btn"
                                    href={API_PATHS.artifactGet(artifact.artifact_id)}
                                    target="_blank"
                                    rel="noreferrer"
                                  >
                                    {WORKFLOWS_COPY.OPEN}
                                  </a>
                                </span>
                              </div>
                            ))
                          }
                        </div>
                      ) : !artifactsLoading ? (
                        <div className="dim small">{WORKFLOWS_COPY.NO_ARTIFACTS}</div>
                      ) : null
                    }

                    {
                      preview.runID === r.id && preview.artifactID ? (
                        <div className="dq-panel dq-artifact-preview">
                          <div className="row">
                            <strong>{WORKFLOWS_COPY.ARTIFACT_PREVIEW}</strong>
                            <span className="dim small">{preview.contentType || WORKFLOWS_COPY.UNKNOWN_TYPE}</span>
                          </div>
                          {preview.loading ? <div className="dim small">{WORKFLOWS_COPY.LOADING_PREVIEW}</div> : null}
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
            {WORKFLOWS_COPY.NO_RUNS} <code>POST {API_PATHS.RUN_DEMO}</code>.
          </div>
        ) : null
      }
    </section>
  );
}
