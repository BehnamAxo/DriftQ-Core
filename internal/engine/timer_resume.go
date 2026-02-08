package engine

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// FireDueTimersAndResume:
//  1) finds all due timers (scheduled && fire_at <= now),
//  2) fires them (marks fired + appends TimerFired events),
//  3) resumes any WAITING runs impacted by those timers.
//
// This turns Delay() + timers into a real "backoff retry" mechanism: timers fire -> runs continue.
func (r *Runner) FireDueTimersAndResume(ctx context.Context, now time.Time) (fired int, resumed int, err error) {
	now = now.UTC()

	// Capture which runs are impacted BEFORE firing timers (since FireDueTimers mutates timer state).
	due := r.store.ListDueTimers(now)
	if len(due) == 0 {
		return 0, 0, nil
	}

	runIDs := make(map[string]struct{}, len(due))
	for _, t := range due {
		runIDs[t.RunID] = struct{}{}
	}

	fired, err = r.FireDueTimers(now)
	if err != nil {
		return fired, 0, err
	}
	if fired == 0 {
		return 0, 0, nil
	}

	// Resume each waiting run once.
	for runID := range runIDs {
		ok, rerr := r.resumeWaitingRun(ctx, runID)
		if rerr != nil {
			// Don't fail the whole loop for one bad run; log + continue.
			r.logger.Error("timers: resume failed", "run_id", runID, "err", rerr)
			continue
		}
		if ok {
			resumed++
		}
	}

	return fired, resumed, nil
}

// resumeWaitingRun resumes a run if it exists and is currently WAITING.
// Returns (resumed=true) iff it attempted a resume.
// It is safe to call this repeatedly; non-waiting runs are ignored.
func (r *Runner) resumeWaitingRun(ctx context.Context, runID string) (bool, error) {
	run, ok := r.store.GetRun(runID)
	if !ok {
		return false, nil
	}

	// Only resume runs that are waiting (i.e., paused on timers/backoff).
	if run.Status != RunStatusWaiting {
		return false, nil
	}

	// We need the workflow spec to rebuild the graph/executable.
	if len(run.Spec) == 0 {
		return false, errors.New("run has no spec stored; cannot resume")
	}

	reg := r.HandlerRegistry()
	if reg == nil {
		return false, errors.New("no handler registry configured on runner")
	}

	// Re-parse and compile from the stored spec.
	g, spec, err := ParseWorkflowSpecJSON(run.Spec)
	if err != nil {
		return false, err
	}

	exec, err := CompileSpecToExecutable(spec, g, reg)
	if err != nil {
		return false, err
	}

	// Note: runDAG will:
	// - set status RUNNING
	// - continue where it left off based on persisted node executions
	// - possibly return to WAITING if another Delay() occurs
	return true, r.runDAG(ctx, runID, exec, run.InitialInput, json.RawMessage(run.Spec))
}
