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

// some updates for this section logic, replay now is backwards-compatible:
//   - If the workflow graph is still in memory, it works like before
//   - But if the process restarted and the graph cache is empty, we must use ReplayWithRegistry
//     so we can rebuild the executable graph from the stored run.Spec
func (r *Runner) Replay(ctx context.Context, runID string, mode ReplayMode) error {
	run, ok := r.store.GetRun(runID)
	if !ok {
		return ErrRunNotFound
	}

	// We need an executable graph (NodeDef.Run funcs). For now we fetch it from the in-memory graph cache
	// If this fails after a process restart, we will need to recompile from run.Spec using a registry
	g, ok := r.getGraph(run.WorkflowID)
	if !ok {
		if len(run.Spec) == 0 {
			return fmt.Errorf("replay: workflow graph not found for workflow_id=%q (run_id=%q) and run has no stored spec", run.WorkflowID, runID)
		}
		return fmt.Errorf("replay: workflow graph not found for workflow_id=%q (run_id=%q); compiled graph cache missing (restart?)", run.WorkflowID, runID)
	}

	// Prefer stored initial input (new runs). Fall back to reconstructing from the root node input
	initial := cloneRaw(run.InitialInput)
	if len(initial) == 0 {
		initial = r.initialInputFromRun(runID, g)
	}

	switch mode {
	case ReplayTimeTravel:
		return r.runDAG(ctx, runID, g, initial, cloneRaw(run.Spec))

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
