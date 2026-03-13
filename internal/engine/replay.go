package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type ReplayMode string

const (
	ReplayLive       ReplayMode = "live"
	ReplayTimeTravel ReplayMode = "time_travel"
)

func (r *Runner) Replay(ctx context.Context, runID string, mode ReplayMode) error {
	return r.ReplayFrom(ctx, runID, "", mode)
}

// ReplayFrom replays (or redrives) a run starting at a specific step.
//
// Semantics:
//   - mode=time_travel: reuse any already-succeeded outputs when possible.
//     If fromStep has already succeeded, we keep it and replay downstream only.
//   - mode=live: force re-execution starting at fromStep.
//
// Implementation note: we keep history by marking prior node executions/timers as
// canceled instead of deleting them. New attempts will have higher attempt numbers.
func (r *Runner) ReplayFrom(ctx context.Context, runID, fromStep string, mode ReplayMode) error {
	run, ok := r.store.GetRun(runID)
	if !ok {
		return ErrRunNotFound
	}

	if err := r.ensureRunTenantAccess(ctx, run, "run.replay"); err != nil {
		return err
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

	// Optional: replay from a step by invalidating the downstream suffix.
	if strings.TrimSpace(fromStep) != "" {
		fromStep = strings.TrimSpace(fromStep)
		if !graphHasNode(exec, fromStep) {
			return fmt.Errorf("replay: unknown from-step %q", fromStep)
		}

		suffix := downstreamNodes(exec, fromStep)

		// In time-travel mode, if the from-step already has a succeeded attempt,
		// we keep it and only invalidate downstream nodes.
		if mode == ReplayTimeTravel && nodeHasSucceededAttempt(r.store.ListNodeExecutions(runID), fromStep) {
			delete(suffix, fromStep)
		}

		if err := r.invalidateNodesAndTimers(runID, suffix); err != nil {
			return err
		}

		// allow re-running a previously succeeded run
		if run.Status == RunStatusSucceeded {
			run.Status = RunStatusQueued
			run.EndedAt = nil
			run.TerminalReason = ""
			run.TerminalMeta = nil
			_ = r.store.UpdateRun(run)
		}
	}

	switch mode {
	case ReplayTimeTravel:
		cache := r.buildReplayCacheFromRun(runID)
		return r.runDAGWithReplayCache(ctx, runID, exec, initial, run.Spec, cache)

	case ReplayLive:
		// Live replay forces execution. If no fromStep is provided, force a full rerun.
		if strings.TrimSpace(fromStep) == "" {
			suffix := map[string]struct{}{}
			for _, n := range exec.Nodes {
				suffix[n.NodeID] = struct{}{}
			}
			if err := r.invalidateNodesAndTimers(runID, suffix); err != nil {
				return err
			}
			if run.Status == RunStatusSucceeded {
				run.Status = RunStatusQueued
				run.EndedAt = nil
				run.TerminalReason = ""
				run.TerminalMeta = nil
				_ = r.store.UpdateRun(run)
			}
		}
		return r.runDAGWithReplayCache(ctx, runID, exec, initial, run.Spec, nil)

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

	reg := r.HandlerRegistryForTenant(run.TenantID)
	if reg == nil {
		return WorkflowGraph{}, fmt.Errorf(
			"replay: no cached graph; runner has no handler registry to compile stored spec (workflow_id=%q run_id=%q)",
			run.WorkflowID, runID,
		)
	}

	g, spec, err := ParseWorkflowSpecJSON(run.Spec)
	if err != nil {
		return WorkflowGraph{}, fmt.Errorf("replay: parse stored spec failed: %w", err)
	}

	exec2, err := CompileSpecToExecutable(spec, g, reg)
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
