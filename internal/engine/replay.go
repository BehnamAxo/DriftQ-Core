package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type ReplayMode string

const (
	ReplayLive       ReplayMode = "live"
	ReplayTimeTravel ReplayMode = "time_travel"
)

func (r *Runner) Replay(ctx context.Context, srcRunID string, mode ReplayMode) (string, error) {
	src, ok := r.store.GetRun(srcRunID)
	if !ok {
		return "", ErrRunNotFound
	}

	// 1) Prefer cached executable graph (fast path)
	exec, ok := r.getGraph(src.WorkflowID)

	// 2) If cache miss, fall back to stored spec (replay-after-restart)
	if !ok {
		if len(src.Spec) == 0 {
			return "", fmt.Errorf(
				"replay: workflow graph not found in memory and no stored spec for workflow_id=%q (run_id=%q)",
				src.WorkflowID, srcRunID,
			)
		}
		if r.registry == nil {
			return "", fmt.Errorf(
				"replay: no cached graph; runner has no handler registry to compile stored spec (workflow_id=%q run_id=%q)",
				src.WorkflowID, srcRunID,
			)
		}

		g, spec, err := ParseWorkflowSpecJSON(src.Spec)
		if err != nil {
			return "", fmt.Errorf("replay: parse stored spec failed: %w", err)
		}

		exec2, err := CompileSpecToExecutable(spec, g, r.registry)
		if err != nil {
			return "", fmt.Errorf("replay: compile stored spec failed: %w", err)
		}

		// keep IDs aligned so logs + caches are sane
		if exec2.ID == "" || exec2.ID != src.WorkflowID {
			exec2.ID = src.WorkflowID
		}

		r.rememberGraph(src.WorkflowID, exec2)
		exec = exec2
	}

	// Prefer stored initial input; fallback to root node's recorded input
	initial := json.RawMessage(nil)
	if len(src.InitialInput) > 0 {
		initial = cloneRaw(src.InitialInput)
	} else {
		initial = r.initialInputFromRun(srcRunID, exec)
	}

	switch mode {
	case ReplayTimeTravel:
		// New run id for the replay so we don't clobber the original run
		replayRunID := fmt.Sprintf("%s-replay-%s", srcRunID, NewTraceID())

		// Create the new run
		newRun := Run{
			RunID:        replayRunID,
			WorkflowID:   src.WorkflowID,
			Status:       RunStatusQueued,
			Spec:         cloneRaw(src.Spec),
			InitialInput: cloneRaw(initial),
		}
		if err := r.store.CreateRun(newRun); err != nil {
			return "", err
		}
		_, _ = r.store.AppendEvent(RunEvent{RunID: replayRunID, Type: EventRunCreated})

		// Build replay cache from the source run
		cache := r.buildReplayCacheFromRun(srcRunID)

		// Run new DAG, but reuse cached outputs instead of calling handlers
		if err := r.runDAGWithCache(ctx, replayRunID, exec, initial, src.Spec, cache); err != nil {
			return replayRunID, err
		}
		return replayRunID, nil

	case ReplayLive:
		return "", errors.New("replay live not implemented yet")

	default:
		return "", fmt.Errorf("unknown replay mode: %q", mode)
	}
}

func (r *Runner) initialInputFromRun(runID string, g WorkflowGraph) json.RawMessage {
	incoming := map[string]int{}
	for _, n := range g.Nodes {
		incoming[n.NodeID] = 0
	}
	for _, e := range g.Edges {
		incoming[e.To]++
	}

	rootID := ""
	for _, n := range g.Nodes {
		if incoming[n.NodeID] == 0 {
			rootID = n.NodeID
			break
		}
	}
	if rootID == "" {
		return json.RawMessage(`{}`)
	}

	prev := r.store.ListNodeExecutions(runID)
	bestAttempt := int(^uint(0) >> 1) // max int
	var best json.RawMessage

	for _, ne := range prev {
		if ne.NodeID != rootID {
			continue
		}
		if len(ne.Input) == 0 {
			continue
		}
		if ne.Attempt < bestAttempt {
			bestAttempt = ne.Attempt
			best = cloneRaw(ne.Input)
		}
	}

	if best == nil {
		return json.RawMessage(`{}`)
	}
	return best
}
