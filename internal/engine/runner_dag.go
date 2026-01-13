package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"
)

var ErrGraphInvalid = errors.New("invalid workflow graph")
var ErrRunCanceled = errors.New("run canceled")

func (r *Runner) RunDAG(ctx context.Context, runID string, g WorkflowGraph, initialInput json.RawMessage) error {
	return r.runDAG(ctx, runID, g, initialInput, nil)
}

func (r *Runner) runDAG(ctx context.Context, runID string, g WorkflowGraph, initialInput json.RawMessage, spec json.RawMessage) error {
	if err := g.Validate(); err != nil {
		return err
	}

	traceID := TraceIDFrom(ctx)
	if traceID == "" {
		traceID = NewTraceID()
		ctx = WithTraceID(ctx, traceID)
	}

	// helper: external cancel (via CancelRun) should stop scheduling ASAP
	isCanceled := func() bool {
		cur, ok := r.store.GetRun(runID)
		return ok && cur.Status == RunStatusCanceled
	}

	nodeByID := map[string]NodeDef{}
	nodeIndex := map[string]int{}
	for i, n := range g.Nodes {
		nodeByID[n.NodeID] = n
		nodeIndex[n.NodeID] = i
	}

	wfID := g.ID
	if wfID == "" {
		wfID = "graph_" + runID
	}

	var run Run
	existingRun, ok := r.store.GetRun(runID)
	done := map[string]NodeExecution{}
	outputs := map[string]json.RawMessage{}
	maxAttempt := map[string]int{}

	if ok {
		run = existingRun

		// prefer stored workflow id if present
		if run.WorkflowID != "" {
			wfID = run.WorkflowID
		} else {
			run.WorkflowID = wfID
		}

		if run.Status == RunStatusSucceeded {
			r.logger.Info("run already succeeded; skipping",
				"trace_id", traceID,
				"run_id", runID,
			)
			return nil
		}

		if run.Status == RunStatusCanceled {
			r.logger.Info("run already canceled; skipping",
				"trace_id", traceID,
				"run_id", runID,
			)
			return ErrRunCanceled
		}

		r.logger.Info("resuming existing run", "trace_id", traceID, "run_id", runID)

		prevNodes := r.store.ListNodeExecutions(runID)
		for _, ne := range prevNodes {
			// track max attempt per node (even for failed/running/waiting records)
			if ne.Attempt > maxAttempt[ne.NodeID] {
				maxAttempt[ne.NodeID] = ne.Attempt
			}

			if ne.Status == NodeStatusSucceeded {
				done[ne.NodeID] = ne
				if len(ne.Output) > 0 {
					outputs[ne.NodeID] = cloneRaw(ne.Output)
				}
			}
		}
	} else {
		run = Run{
			RunID:        runID,
			WorkflowID:   wfID,
			Status:       RunStatusQueued,
			Spec:         cloneRaw(spec),
			InitialInput: cloneRaw(initialInput),
		}

		if err := r.store.CreateRun(run); err != nil {
			r.logger.Error("run create failed", "trace_id", traceID, "run_id", runID, "err", err)
			return err
		}

		r.logger.Info("run created", "trace_id", traceID, "run_id", runID)

		_, _ = r.store.AppendEvent(RunEvent{
			RunID: runID,
			Type:  EventRunCreated,
		})
	}

	r.rememberGraph(wfID, g)

	parents := map[string][]string{}
	children := map[string][]string{}
	inDegree := map[string]int{}

	for _, n := range g.Nodes {
		inDegree[n.NodeID] = 0
	}

	for _, e := range g.Edges {
		children[e.From] = append(children[e.From], e.To)
		parents[e.To] = append(parents[e.To], e.From)

		// Only count this dependency if the parent is NOT already done
		if _, parentDone := done[e.From]; !parentDone {
			inDegree[e.To]++
		}
	}

	// Start / resume run
	now := time.Now().UTC()

	startedJustNow := false
	if run.StartedAt == nil {
		run.StartedAt = &now
		startedJustNow = true
	}

	// If we are resuming a failed/waiting run, clear EndedAt
	if run.Status != RunStatusRunning {
		run.EndedAt = nil
	}

	// If someone canceled via store before we begin, do not flip to RUNNING.
	if isCanceled() {
		r.logger.Info("run already canceled; skipping start",
			"trace_id", traceID,
			"run_id", runID,
		)
		return ErrRunCanceled
	}

	run.Status = RunStatusRunning
	if err := r.store.UpdateRun(run); err != nil {
		r.logger.Error("run start update failed", "trace_id", traceID, "run_id", runID, "err", err)
		return err
	}

	if startedJustNow {
		_, _ = r.store.AppendEvent(RunEvent{
			RunID: runID,
			Type:  EventRunStarted,
		})
	}

	r.logger.Info("run started", "trace_id", traceID, "run_id", runID)

	// ---- per-run cancelable context so /run-cancel can interrupt in-flight node.Run ----
	runCtx, runCancel := context.WithCancel(ctx)
	r.setRunCancel(runID, runCancel)
	defer func() {
		r.clearRunCancel(runID)
		runCancel()
	}()
	// -------------------------------------------------------------------------------

	// Ready queue = nodes with indegree 0 AND not already succeeded
	var ready []string
	for id, deg := range inDegree {
		if deg == 0 {
			if _, isDone := done[id]; !isDone {
				ready = append(ready, id)
			}
		}
	}

	sort.Slice(ready, func(i, j int) bool { return nodeIndex[ready[i]] < nodeIndex[ready[j]] })

	for len(ready) > 0 {
		// stop scheduling new work if externally canceled
		if isCanceled() {
			r.logger.Info("run canceled; stopping scheduling",
				"trace_id", traceID,
				"run_id", runID,
			)
			return ErrRunCanceled
		}

		select {
		case <-runCtx.Done():
			end := time.Now().UTC()
			run.Status = RunStatusCanceled
			run.EndedAt = &end

			if run.StartedAt != nil {
				r.metrics.ObserveRun(run.Status, end.Sub(*run.StartedAt))
			}

			r.logger.Info("run finished",
				"trace_id", traceID,
				"run_id", runID,
				"status", run.Status,
				"duration_ms", func() int64 {
					if run.StartedAt == nil {
						return 0
					}
					return end.Sub(*run.StartedAt).Milliseconds()
				}(),
				"err", runCtx.Err(),
			)

			_ = r.store.UpdateRun(run)
			_, _ = r.store.AppendEvent(RunEvent{
				RunID:   runID,
				Type:    EventRunFinished,
				Payload: json.RawMessage(`{"status":"canceled"}`),
			})
			return runCtx.Err()
		default:
		}

		// pop next ready node
		nodeID := ready[0]
		ready = ready[1:]

		node, ok := nodeByID[nodeID]
		if !ok {
			return ErrGraphInvalid
		}

		// Build node input
		var nodeInput json.RawMessage
		deps := parents[nodeID]
		if len(deps) == 0 {
			nodeInput = cloneRaw(initialInput)
		} else {
			sort.Slice(deps, func(i, j int) bool { return nodeIndex[deps[i]] < nodeIndex[deps[j]] })
			agg := make(map[string]json.RawMessage, len(deps))
			for _, depID := range deps {
				agg[depID] = cloneRaw(outputs[depID])
			}
			b, err := json.Marshal(agg)
			if err != nil {
				return err
			}
			nodeInput = json.RawMessage(b)
		}

		// increment attempt on replay
		attempt := 1
		if prev := maxAttempt[node.NodeID]; prev > 0 {
			attempt = prev + 1
		}
		maxAttempt[node.NodeID] = attempt

		nodeStart := time.Now().UTC()

		r.logger.Info("node started",
			"trace_id", traceID,
			"run_id", runID,
			"step_id", node.NodeID,
			"attempt", attempt,
		)

		ne := NodeExecution{
			RunID:      runID,
			WorkflowID: wfID,
			NodeID:     node.NodeID,
			Attempt:    attempt,
			Status:     NodeStatusRunning,
			StartedAt:  &nodeStart,
			Input:      cloneRaw(nodeInput),
		}

		if err := r.store.UpsertNodeExecution(ne); err != nil {
			r.logger.Error("node upsert start failed",
				"trace_id", traceID, "run_id", runID, "step_id", node.NodeID, "attempt", attempt, "err", err)
			return err
		}

		_, _ = r.store.AppendEvent(RunEvent{
			RunID:      runID,
			Type:       EventNodeStarted,
			WorkflowID: wfID,
			NodeID:     node.NodeID,
			Attempt:    attempt,
		})

		// IMPORTANT: base execution context COMES from runCtx so external cancel can interrupt the node
		stepCtx := WithAttempt(runCtx, attempt)

		execCtx := stepCtx
		cancelFn := func() {}
		if node.TimeoutMS > 0 {
			execCtx, cancelFn = context.WithTimeout(stepCtx, time.Duration(node.TimeoutMS)*time.Millisecond)
		}

		out, err := node.Run(execCtx, cloneRaw(nodeInput))

		// Always release timer resources immediately (NOT defer inside loop)
		cancelFn()

		// If the context timed out, treat it as the failure reason.
		if execCtx.Err() == context.DeadlineExceeded {
			err = context.DeadlineExceeded
		}

		nodeEnd := time.Now().UTC()
		nodeDur := nodeEnd.Sub(nodeStart)

		// ---- Section durable delay -> WAITING + TimerScheduled ----
		var de *DelayError
		if err != nil && errors.As(err, &de) {
			// if canceled while node was running, do not schedule timers / overwrite
			if isCanceled() {
				return ErrRunCanceled
			}

			now := time.Now().UTC()
			fireAt := now.Add(de.After)

			// mark node waiting
			ne.Status = NodeStatusWaiting
			ne.EndedAt = &now
			ne.Error = ""
			_ = r.store.UpsertNodeExecution(ne)

			// schedule timer (durable)
			_ = r.store.UpsertTimer(Timer{
				RunID:      runID,
				WorkflowID: wfID,
				NodeID:     node.NodeID,
				Attempt:    attempt,
				Status:     TimerScheduled,
				FireAt:     fireAt,
				CreatedAt:  now,
				Reason:     de.Reason,
			})

			p, _ := json.Marshal(map[string]any{
				"fire_at": fireAt.Format(time.RFC3339Nano),
				"reason":  de.Reason,
			})
			_, _ = r.store.AppendEvent(RunEvent{
				RunID:      runID,
				Type:       EventTimerScheduled,
				WorkflowID: wfID,
				NodeID:     node.NodeID,
				Attempt:    attempt,
				Payload:    p,
			})

			// mark run waiting
			run.Status = RunStatusWaiting
			run.EndedAt = nil
			_ = r.store.UpdateRun(run)

			r.logger.Info("node waiting (timer scheduled)",
				"trace_id", traceID,
				"run_id", runID,
				"step_id", node.NodeID,
				"attempt", attempt,
				"fire_at", fireAt.Format(time.RFC3339Nano),
				"reason", de.Reason,
			)

			// stop scheduling; resume will happen when timer fires
			return nil
		}
		// --------------------------------------------------------

		if err != nil {
			// if canceled while this node was running, do NOT overwrite the run as FAILED
			if isCanceled() {
				return ErrRunCanceled
			}

			r.metrics.ObserveNode(node.NodeID, false, nodeDur)

			r.logger.Error("node failed",
				"trace_id", traceID,
				"run_id", runID,
				"step_id", node.NodeID,
				"attempt", attempt,
				"duration_ms", nodeDur.Milliseconds(),
				"err", err,
			)

			ne.Status = NodeStatusFailed
			ne.EndedAt = &nodeEnd
			ne.Error = err.Error()
			_ = r.store.UpsertNodeExecution(ne)

			p, _ := json.Marshal(map[string]any{"error": err.Error()})
			_, _ = r.store.AppendEvent(RunEvent{
				RunID:   runID,
				Type:    EventNodeFailed,
				NodeID:  node.NodeID,
				Attempt: attempt,
				Payload: p,
			})

			run.Status = RunStatusFailed
			run.EndedAt = &nodeEnd

			if run.StartedAt != nil {
				r.metrics.ObserveRun(run.Status, nodeEnd.Sub(*run.StartedAt))
			}

			r.logger.Info("run finished",
				"trace_id", traceID,
				"run_id", runID,
				"status", run.Status,
				"duration_ms", func() int64 {
					if run.StartedAt == nil {
						return 0
					}
					return nodeEnd.Sub(*run.StartedAt).Milliseconds()
				}(),
			)

			_ = r.store.UpdateRun(run)
			p2, _ := json.Marshal(map[string]any{"status": "failed", "failed_node": node.NodeID})
			_, _ = r.store.AppendEvent(RunEvent{
				RunID:   runID,
				Type:    EventRunFinished,
				Payload: p2,
			})

			return ErrNodeFailed
		}

		// success
		r.metrics.ObserveNode(node.NodeID, true, nodeDur)

		r.logger.Info("node finished",
			"trace_id", traceID,
			"run_id", runID,
			"step_id", node.NodeID,
			"attempt", attempt,
			"duration_ms", nodeDur.Milliseconds(),
		)

		ne.Status = NodeStatusSucceeded
		ne.EndedAt = &nodeEnd
		ne.Output = cloneRaw(out)
		if err := r.store.UpsertNodeExecution(ne); err != nil {
			r.logger.Error("node upsert finish failed",
				"trace_id", traceID, "run_id", runID, "step_id", node.NodeID, "attempt", attempt, "err", err)
			return err
		}

		p, _ := json.Marshal(map[string]any{"output": json.RawMessage(out)})
		_, _ = r.store.AppendEvent(RunEvent{
			RunID:   runID,
			Type:    EventNodeFinished,
			NodeID:  node.NodeID,
			Attempt: attempt,
			Payload: p,
		})

		// store output for downstream deps
		outputs[nodeID] = cloneRaw(out)

		// unlock children
		for _, child := range children[nodeID] {
			if _, isDone := done[child]; isDone {
				continue
			}
			inDegree[child]--
			if inDegree[child] == 0 {
				ready = append(ready, child)
			}
		}

		sort.Slice(ready, func(i, j int) bool { return nodeIndex[ready[i]] < nodeIndex[ready[j]] })
	}

	// if canceled while last node was running, do NOT overwrite as SUCCEEDED
	if isCanceled() {
		return ErrRunCanceled
	}

	// finish run success
	end := time.Now().UTC()
	run.Status = RunStatusSucceeded
	run.EndedAt = &end

	if run.StartedAt != nil {
		r.metrics.ObserveRun(run.Status, end.Sub(*run.StartedAt))
	}

	if err := r.store.UpdateRun(run); err != nil {
		r.logger.Error("run finish update failed", "trace_id", traceID, "run_id", runID, "err", err)
		return err
	}

	r.logger.Info("run finished",
		"trace_id", traceID,
		"run_id", runID,
		"status", run.Status,
		"duration_ms", func() int64 {
			if run.StartedAt == nil {
				return 0
			}
			return end.Sub(*run.StartedAt).Milliseconds()
		}(),
	)

	p3, _ := json.Marshal(map[string]any{"status": "succeeded"})
	_, _ = r.store.AppendEvent(RunEvent{
		RunID:   runID,
		Type:    EventRunFinished,
		Payload: p3,
	})

	return nil
}
