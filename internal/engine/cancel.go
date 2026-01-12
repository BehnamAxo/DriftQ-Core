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

	run, ok := r.store.GetRun(runID)
	if !ok {
		return ErrRunNotFound
	}

	if run.Status == RunStatusSucceeded || run.Status == RunStatusFailed || run.Status == RunStatusCanceled {
		return nil
	}

	now := time.Now().UTC()
	run.Status = RunStatusCanceled
	run.EndedAt = &now

	if err := r.store.UpdateRun(run); err != nil {
		return err
	}

	p, _ := json.Marshal(map[string]any{
		"status": "canceled",
		"reason": strings.TrimSpace(reason),
	})

	_, _ = r.store.AppendEvent(RunEvent{
		RunID:   runID,
		Type:    EventRunFinished,
		Payload: p,
	})

	r.logger.Info("run canceled", "run_id", runID, "reason", strings.TrimSpace(reason), "trace_id", TraceIDFrom(ctx))
	return nil
}
