package engine

import (
	"context"
	"encoding/json"
	"strings"
	"time"
)

func (r *Runner) CancelRun(ctx context.Context, runID string, reason string) error {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return ErrRunNotFound
	}
	reason = strings.TrimSpace(reason)

	traceID := TraceIDFrom(ctx)
	if traceID == "" {
		traceID = NewTraceID()
		ctx = WithTraceID(ctx, traceID)
	}

	run, ok := r.store.GetRun(runID)
	if !ok {
		return ErrRunNotFound
	}

	if err := r.ensureRunTenantAccess(ctx, run, "run.cancel"); err != nil {
		return err
	}

	if run.Status == RunStatusSucceeded || run.Status == RunStatusFailed {
		return nil
	}

	// Idempotent cancel: still try to interrupt any in-flight work
	if run.Status == RunStatusCanceled {
		r.cancelRunContext(runID)
		return nil
	}

	now := time.Now().UTC()
	run.Status = RunStatusCanceled
	run.EndedAt = &now

	if err := r.store.UpdateRun(run); err != nil {
		return err
	}

	// IMPORTANT: this is what actually interrupts an in-flight node.Run(...) quickly...
	r.cancelRunContext(runID)

	p, _ := json.Marshal(map[string]any{
		"status": "canceled",
		"reason": reason,
	})

	_, _ = r.store.AppendEvent(RunEvent{
		RunID:      runID,
		Type:       EventRunFinished,
		WorkflowID: run.WorkflowID,
		Payload:    p,
	})

	r.logger.Info("run canceled", "trace_id", traceID, "run_id", runID, "reason", reason)
	return nil
}
