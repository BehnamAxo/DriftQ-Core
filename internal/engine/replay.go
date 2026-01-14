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

func (r *Runner) Replay(ctx context.Context, runID string, mode ReplayMode) error {
	run, ok := r.store.GetRun(runID)
	if !ok {
		return ErrRunNotFound
	}

	// 1) Prefer cached executable graph (fast path)
	exec, ok := r.getGraph(run.WorkflowID)

	// 2) If cache miss, fall back to stored spec (replay-after-restart)
	if !ok {
		exec2, err := r.compileExecutableFromStoredSpec(runID, run)
		if err != nil {
			return err
		}
		exec = exec2
	}

	// Prefer stored initial input; fallback to the first root node's recorded input
	var initial json.RawMessage
	if len(run.InitialInput) > 0 {
		initial = cloneRaw(run.InitialInput)
	} else {
		initial = r.initialInputFromRun(runID, exec)
	}

	switch mode {
	case ReplayTimeTravel:
		// Build a cache from the run history so we can short-circuit node execution
		cache := r.buildReplayCacheFromRun(runID)

		// IMPORTANT: use the cache-aware runner; plain runDAG() would discard the cache
		return r.runDAGWithReplayCache(ctx, runID, exec, initial, run.Spec, cache)

	case ReplayLive:
		return errors.New("replay live not implemented yet")

	default:
		return fmt.Errorf("unknown replay mode: %q", mode)
	}
}

func (r *Runner) compileExecutableFromStoredSpec(runID string, run Run) (WorkflowGraph, error) {
	if len(run.Spec) == 0 {
		return WorkflowGraph{}, fmt.Errorf(
			"replay: no cached graph and no stored spec (workflow_id=%q run_id=%q)",
			run.WorkflowID, runID,
		)
	}
	if r.registry == nil {
		return WorkflowGraph{}, fmt.Errorf(
			"replay: no cached graph; runner has no handler registry to compile stored spec (workflow_id=%q run_id=%q)",
			run.WorkflowID, runID,
		)
	}

	g, spec, err := ParseWorkflowSpecJSON(run.Spec)
	if err != nil {
		return WorkflowGraph{}, fmt.Errorf("replay: parse stored spec failed: %w", err)
	}

	exec2, err := CompileSpecToExecutable(spec, g, r.registry)
	if err != nil {
		return WorkflowGraph{}, fmt.Errorf("replay: compile stored spec failed: %w", err)
	}

	// Keep IDs aligned so logs + caches are sane
	if exec2.ID == "" || exec2.ID != run.WorkflowID {
		exec2.ID = run.WorkflowID
	}

	r.rememberGraph(run.WorkflowID, exec2)
	return exec2, nil
}

// If Run.InitialInput wasn't stored, infer it from the earliest attempt of the root node's recorded Input.
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
