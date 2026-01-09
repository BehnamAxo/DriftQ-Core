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
//   - If the workflow graph is still in memory, it works like before.
//   - But if the process restarted and the graph cache is empty, we must use ReplayWithRegistry
//     so we can rebuild the executable graph from the stored run.Spec.
func (r *Runner) Replay(ctx context.Context, runID string, mode ReplayMode) error {
	return r.ReplayWithRegistry(ctx, runID, mode, nil)
}

// ReplayWithRegistry is now restart-safe meaning if the workflow graph is not in memory, it rebuilds it from run.Spec using the provided registry woot woot
func (r *Runner) ReplayWithRegistry(ctx context.Context, runID string, mode ReplayMode, reg *HandlerRegistry) error {
	run, ok := r.store.GetRun(runID)
	if !ok {
		return ErrRunNotFound
	}

	// Fast path: graph already remembered in-memory
	exec, ok := r.getGraph(run.WorkflowID)

	// Restart-safe path: rebuild executable graph from stored spec.
	if !ok {
		if len(run.Spec) == 0 {
			return fmt.Errorf("replay: workflow graph not found in memory and run has no stored spec (workflow_id=%q run_id=%q)", run.WorkflowID, runID)
		}

		if reg == nil {
			return errors.New("replay: handler registry is required to rebuild executable graph from stored spec (use ReplayWithRegistry)")
		}

		g, spec, err := ParseWorkflowSpecJSON(run.Spec)
		if err != nil {
			return err
		}

		exec, err = CompileSpecToExecutable(spec, g, reg)
		if err != nil {
			return err
		}

		// Keep ID stable and cache it for this process lifetime
		if exec.ID == "" {
			exec.ID = run.WorkflowID
		}
		r.rememberGraph(run.WorkflowID, exec)
	}

	initial := cloneRaw(run.InitialInput)
	if len(initial) == 0 {
		initial = r.initialInputFromRun(runID, exec)
	}

	switch mode {
	case ReplayTimeTravel:
		return r.runDAG(ctx, runID, exec, initial, cloneRaw(run.Spec))

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
