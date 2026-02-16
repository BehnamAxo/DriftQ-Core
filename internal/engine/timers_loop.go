package engine

import (
	"context"
	"encoding/json"
	"time"
)

// FireDueTimers finds all timers with status=scheduled and fire_at <= now, then marks them fired,
// and appends a TimerFired event for each and returns how many timers were fired.
func (r *Runner) FireDueTimers(now time.Time) (int, error) {
	now = now.UTC()

	due := r.store.ListDueTimers(now)
	if len(due) == 0 {
		return 0, nil
	}

	firedCount := 0

	for _, t := range due {
		// Put this here just for extra safety (ListDueTimers should already filter this but still)
		if t.Status != TimerScheduled {
			continue
		}

		// mark fired
		t.Status = TimerFired
		tm := now
		t.FiredAt = &tm

		if err := r.store.UpsertTimer(t); err != nil {
			r.logger.Error("timer upsert (fire) failed",
				"run_id", t.RunID, "step_id", t.NodeID, "attempt", t.Attempt, "err", err,
			)
			return firedCount, err
		}

		p, _ := json.Marshal(map[string]any{
			"status":   "fired",
			"fire_at":  t.FireAt,
			"fired_at": tm,
			"reason":   t.Reason,
		})

		_, _ = r.store.AppendEvent(RunEvent{
			RunID:      t.RunID,
			Type:       EventTimerFired,
			WorkflowID: t.WorkflowID,
			NodeID:     t.NodeID,
			Attempt:    t.Attempt,
			Payload:    p,
		})

		firedCount++
	}

	return firedCount, nil
}

// StartTimerLoop polls for due timers and fires them.
// After firing, it best-effort resumes runs that are in "waiting" state
// (using remembered graphs)
func (r *Runner) StartTimerLoop(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	go func() {
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := time.Now().UTC()

				// capture which runs are due BEFORE we fire (FireDueTimers only returns count)
				due := r.store.ListDueTimers(now)

				fired, err := r.FireDueTimers(now)
				if err != nil || fired == 0 {
					continue
				}

				// Resume waiting runs (best-effort)
				seen := map[string]struct{}{}
				for _, tm := range due {
					if tm.RunID == "" {
						continue
					}
					if _, ok := seen[tm.RunID]; ok {
						continue
					}
					seen[tm.RunID] = struct{}{}

					run, ok := r.store.GetRun(tm.RunID)
					if !ok {
						continue
					}
					if run.Status != RunStatusWaiting {
						continue
					}

					r.mu.RLock()
					g, ok := r.graphs[run.WorkflowID]
					r.mu.RUnlock()
					if !ok {
						// tests call runner.rememberGraph(...), so this should exist there
						continue
					}

					go func(runID string, graph WorkflowGraph) {
						_ = r.RunDAG(context.Background(), runID, graph, json.RawMessage(`{}`))
					}(tm.RunID, g)
				}
			}
		}
	}()
}
