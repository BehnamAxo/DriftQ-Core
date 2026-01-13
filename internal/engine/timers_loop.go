package engine

import (
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
