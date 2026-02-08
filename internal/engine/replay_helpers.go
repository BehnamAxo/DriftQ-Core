package engine

import "time"

func graphHasNode(g WorkflowGraph, nodeID string) bool {
	for _, n := range g.Nodes {
		if n.NodeID == nodeID {
			return true
		}
	}
	return false
}

// downstreamNodes returns the transitive closure of nodes reachable from fromStep
// (including fromStep itself).
func downstreamNodes(g WorkflowGraph, fromStep string) map[string]struct{} {
	children := map[string][]string{}
	for _, e := range g.Edges {
		children[e.From] = append(children[e.From], e.To)
	}

	out := map[string]struct{}{}
	q := []string{fromStep}
	for len(q) > 0 {
		n := q[0]
		q = q[1:]
		if _, seen := out[n]; seen {
			continue
		}
		out[n] = struct{}{}
		for _, c := range children[n] {
			q = append(q, c)
		}
	}

	return out
}

func nodeHasSucceededAttempt(nodes []NodeExecution, nodeID string) bool {
	for _, ne := range nodes {
		if ne.NodeID == nodeID && ne.Status == NodeStatusSucceeded {
			return true
		}
	}
	return false
}

// invalidateNodesAndTimers marks prior work for the provided set of node IDs as canceled.
// This allows a replay/redrive to create new attempts while preserving history.
func (r *Runner) invalidateNodesAndTimers(runID string, nodeIDs map[string]struct{}) error {
	if len(nodeIDs) == 0 {
		return nil
	}

	// 1) Cancel any existing node executions for these nodes (all attempts)
	prev := r.store.ListNodeExecutions(runID)
	for _, ne := range prev {
		if _, ok := nodeIDs[ne.NodeID]; !ok {
			continue
		}
		if ne.Status == NodeStatusCanceled {
			continue
		}

		// Mark as canceled so runDAG won't treat it as done (Succeeded) or waiting.
		ne.Status = NodeStatusCanceled
		now := time.Now().UTC()
		if ne.EndedAt == nil {
			ne.EndedAt = &now
		}
		if err := r.store.UpsertNodeExecution(ne); err != nil {
			return err
		}
	}

	// 2) Cancel any scheduled timers for these nodes
	for _, t := range r.store.ListTimers(runID) {
		if _, ok := nodeIDs[t.NodeID]; !ok {
			continue
		}
		if t.Status != TimerScheduled {
			continue
		}
		now := time.Now().UTC()
		t.Status = TimerCanceled
		t.FiredAt = &now
		if err := r.store.UpsertTimer(t); err != nil {
			return err
		}

		_, _ = r.store.AppendEvent(RunEvent{
			RunID:      runID,
			Type:       EventTimerCanceled,
			WorkflowID: t.WorkflowID,
			NodeID:     t.NodeID,
			Attempt:    t.Attempt,
		})
	}

	return nil
}
