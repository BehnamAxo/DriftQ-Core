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

	g, ok := r.getGraph(run.WorkflowID)
	if !ok {
		return fmt.Errorf("replay: workflow graph not found for workflow_id=%q (run_id=%q)", run.WorkflowID, runID)
	}

	initial := r.initialInputFromRun(runID, g)

	switch mode {
	case ReplayTimeTravel:
		return r.RunDAG(ctx, runID, g, initial)

	case ReplayLive:
		return errors.New("replay live not implemented yet")

	default:
		return fmt.Errorf("unknown replay mode: %q", mode)
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
