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

// Public wrapper (existing API)
func (r *Runner) RunDAG(ctx context.Context, runID string, g WorkflowGraph, initialInput json.RawMessage) error {
	return r.runDAG(ctx, runID, g, initialInput, nil)
}

// Internal wrapper (old behavior, no replay cache)
func (r *Runner) runDAG(ctx context.Context, runID string, g WorkflowGraph, initialInput json.RawMessage, spec json.RawMessage) error {
	return r.runDAGWithReplayCache(ctx, runID, g, initialInput, spec, nil)
}

// Internal wrapper (time-travel replay uses this)
func (r *Runner) runDAGWithReplayCache(ctx context.Context, runID string, g WorkflowGraph, initialInput json.RawMessage, spec json.RawMessage, cache map[string]replayCacheEntry) error {
	return r.runDAGWithCache(ctx, runID, g, initialInput, spec, cache)
}

func (r *Runner) runDAGWithCache(ctx context.Context, runID string, g WorkflowGraph, initialInput json.RawMessage, spec json.RawMessage, cache map[string]replayCacheEntry) error {
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

	// FAN-OUT / FAN-IN section: run up to r.maxParallel ready nodes concurrently, and "join" via indegree unlocks
	type nodeResult struct {
		nodeID  string
		attempt int
		started time.Time
		ended   time.Time
		input   json.RawMessage
		output  json.RawMessage
		err     error
	}

	maxPar := r.maxParallel
	if maxPar < 1 {
		maxPar = 1
	}

	resCh := make(chan nodeResult, maxPar)
	running := 0

	stopScheduling := false
	externalCanceled := false // true if CancelRun already set run.Status=canceled in store
	failed := false
	waiting := false
	var failErr error

	for len(ready) > 0 || running > 0 {
		// External cancel (CancelRun) should interrupt inflight and stop scheduling
		if !stopScheduling && isCanceled() {
			externalCanceled = true
			stopScheduling = true
			// interrupt inflight node.Run(ctx)
			runCancel()
		}

		// Start as many ready nodes as we can
		for !stopScheduling && running < maxPar && len(ready) > 0 {
			// pop next ready node
			nodeID := ready[0]
			ready = ready[1:]

			node, ok := nodeByID[nodeID]
			if !ok {
				return ErrGraphInvalid
			}

			// Build input
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

			// ---------------- REPLAY CACHE SHORT-CIRCUIT ----------------
			if cache != nil {
				if entry, ok := cache[node.NodeID]; ok {
					// synthetic "instant" completion
					nodeEnd := nodeStart
					nodeDur := nodeEnd.Sub(nodeStart)

					r.logger.Info("node replay-cache hit",
						"trace_id", traceID,
						"run_id", runID,
						"step_id", node.NodeID,
						"attempt", attempt,
						"cached_attempt", entry.Attempt,
					)

					// Emit started for consistent event stream
					_, _ = r.store.AppendEvent(RunEvent{
						RunID:      runID,
						Type:       EventNodeStarted,
						WorkflowID: wfID,
						NodeID:     node.NodeID,
						Attempt:    attempt,
					})

					out := cloneRaw(entry.Output)

					// Persist succeeded immediately (no goroutine)
					ne := NodeExecution{
						RunID:      runID,
						WorkflowID: wfID,
						NodeID:     node.NodeID,
						Attempt:    attempt,
						Status:     NodeStatusSucceeded,
						StartedAt:  &nodeStart,
						EndedAt:    &nodeEnd,
						Input:      cloneRaw(nodeInput),
						Output:     out,
					}
					if err := r.store.UpsertNodeExecution(ne); err != nil {
						return err
					}

					// IMPORTANT: mark as done so skip/indegree logic stays correct
					done[node.NodeID] = ne

					// Emit finished event (inline or artifact)
					payload, err := r.buildNodeFinishedPayload(runCtx, runID, wfID, node.NodeID, attempt, out)
					if err != nil {
						return err
					}
					_, _ = r.store.AppendEvent(RunEvent{
						RunID:      runID,
						Type:       EventNodeFinished,
						WorkflowID: wfID,
						NodeID:     node.NodeID,
						Attempt:    attempt,
						Payload:    payload,
					})

					r.metrics.ObserveNode(node.NodeID, true, nodeDur)

					// Make output available to downstream nodes
					outputs[node.NodeID] = out

					// unlock children
					for _, child := range children[node.NodeID] {
						if _, isDone := done[child]; isDone {
							continue
						}

						inDegree[child]--
						if inDegree[child] == 0 {
							ready = append(ready, child)
						}
					}
					sort.Slice(ready, func(i, j int) bool { return nodeIndex[ready[i]] < nodeIndex[ready[j]] })

					continue
				}
			}
			// ------------------------------------------------------------

			// Normal execution path (no cache hit)
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

			// capture locals for goroutine
			n := node
			inp := cloneRaw(nodeInput)
			started := nodeStart
			att := attempt

			running++

			go func() {
				stepCtx := WithAttempt(runCtx, att)

				execCtx := stepCtx
				cancelFn := func() {}
				if n.TimeoutMS > 0 {
					execCtx, cancelFn = context.WithTimeout(stepCtx, time.Duration(n.TimeoutMS)*time.Millisecond)
				}

				out, err := n.Run(execCtx, cloneRaw(inp))

				cancelFn()

				if execCtx.Err() == context.DeadlineExceeded {
					err = context.DeadlineExceeded
				}

				resCh <- nodeResult{
					nodeID:  n.NodeID,
					attempt: att,
					started: started,
					ended:   time.Now().UTC(),
					input:   cloneRaw(inp),
					output:  cloneRaw(out),
					err:     err,
				}
			}()
		}

		// If nothing inflight, loop back to schedule (or exit if ready empty)
		if running == 0 {
			continue
		}

		// Wait for either a node result or cancel
		select {
		case <-runCtx.Done():
			// If CancelRun already updated store, don't overwrite; just stop scheduling and drain
			if isCanceled() {
				externalCanceled = true
			}
			stopScheduling = true
			// ensure inflight sees cancel
			runCancel()

		case res := <-resCh:
			running--

			node := nodeByID[res.nodeID]
			nodeEnd := res.ended
			nodeDur := nodeEnd.Sub(res.started)

			// If externally canceled, mark node as canceled (don’t fail run)
			if externalCanceled || isCanceled() {
				ne := NodeExecution{
					RunID:      runID,
					WorkflowID: wfID,
					NodeID:     node.NodeID,
					Attempt:    res.attempt,
					Status:     NodeStatusCanceled,
					StartedAt:  &res.started,
					EndedAt:    &nodeEnd,
					Input:      cloneRaw(res.input),
				}
				_ = r.store.UpsertNodeExecution(ne)
				stopScheduling = true
				continue
			}

			// ---- durable delay => WAITING + TimerScheduled ----
			var de *DelayError
			if res.err != nil && errors.As(res.err, &de) {
				now := time.Now().UTC()
				fireAt := now.Add(de.After)

				ne := NodeExecution{
					RunID:      runID,
					WorkflowID: wfID,
					NodeID:     node.NodeID,
					Attempt:    res.attempt,
					Status:     NodeStatusWaiting,
					StartedAt:  &res.started,
					EndedAt:    &now,
					Input:      cloneRaw(res.input),
				}
				_ = r.store.UpsertNodeExecution(ne)

				_ = r.store.UpsertTimer(Timer{
					RunID:      runID,
					WorkflowID: wfID,
					NodeID:     node.NodeID,
					Attempt:    res.attempt,
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
					Attempt:    res.attempt,
					Payload:    p,
				})

				run.Status = RunStatusWaiting
				run.EndedAt = nil
				_ = r.store.UpdateRun(run)

				r.logger.Info("node waiting (timer scheduled)",
					"trace_id", traceID,
					"run_id", runID,
					"step_id", node.NodeID,
					"attempt", res.attempt,
					"fire_at", fireAt.Format(time.RFC3339Nano),
					"reason", de.Reason,
				)

				// stop scheduling new work, but still drain inflight cleanly
				waiting = true
				stopScheduling = true
				continue
			}
			// ---------------------------------------------------

			if res.err != nil {
				r.metrics.ObserveNode(node.NodeID, false, nodeDur)

				r.logger.Error("node failed",
					"trace_id", traceID,
					"run_id", runID,
					"step_id", node.NodeID,
					"attempt", res.attempt,
					"duration_ms", nodeDur.Milliseconds(),
					"err", res.err,
				)

				ne := NodeExecution{
					RunID:      runID,
					WorkflowID: wfID,
					NodeID:     node.NodeID,
					Attempt:    res.attempt,
					Status:     NodeStatusFailed,
					StartedAt:  &res.started,
					EndedAt:    &nodeEnd,
					Input:      cloneRaw(res.input),
					Error:      res.err.Error(),
				}
				_ = r.store.UpsertNodeExecution(ne)

				p, _ := json.Marshal(map[string]any{"error": res.err.Error()})
				_, _ = r.store.AppendEvent(RunEvent{
					RunID:      runID,
					Type:       EventNodeFailed,
					WorkflowID: wfID,
					NodeID:     node.NodeID,
					Attempt:    res.attempt,
					Payload:    p,
				})

				run.Status = RunStatusFailed
				run.EndedAt = &nodeEnd

				if run.StartedAt != nil {
					r.metrics.ObserveRun(run.Status, nodeEnd.Sub(*run.StartedAt))
				}

				_ = r.store.UpdateRun(run)
				p2, _ := json.Marshal(map[string]any{"status": "failed", "failed_node": node.NodeID})
				_, _ = r.store.AppendEvent(RunEvent{
					RunID:   runID,
					Type:    EventRunFinished,
					Payload: p2,
				})

				// fail fast: interrupt inflight
				failed = true
				failErr = ErrNodeFailed
				stopScheduling = true
				runCancel()
				continue
			}

			// success
			r.metrics.ObserveNode(node.NodeID, true, nodeDur)

			r.logger.Info("node finished",
				"trace_id", traceID,
				"run_id", runID,
				"step_id", node.NodeID,
				"attempt", res.attempt,
				"duration_ms", nodeDur.Milliseconds(),
			)

			ne := NodeExecution{
				RunID:      runID,
				WorkflowID: wfID,
				NodeID:     node.NodeID,
				Attempt:    res.attempt,
				Status:     NodeStatusSucceeded,
				StartedAt:  &res.started,
				EndedAt:    &nodeEnd,
				Input:      cloneRaw(res.input),
				Output:     cloneRaw(res.output),
			}

			if err := r.store.UpsertNodeExecution(ne); err != nil {
				return err
			}

			// Build payload that either inlines output or stores it as an artifact
			payload, err := r.buildNodeFinishedPayload(runCtx, runID, wfID, node.NodeID, res.attempt, res.output)
			if err != nil {
				return err
			}

			_, _ = r.store.AppendEvent(RunEvent{
				RunID:      runID,
				Type:       EventNodeFinished,
				WorkflowID: wfID,
				NodeID:     node.NodeID,
				Attempt:    res.attempt,
				Payload:    payload,
			})

			outputs[res.nodeID] = cloneRaw(res.output)

			// unlock children
			for _, child := range children[res.nodeID] {
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
	}

	// If we failed, return the failure
	if failed {
		return failErr
	}

	// If CancelRun already set status in store, just return canceled (do not overwrite anything)
	if externalCanceled || isCanceled() {
		return ErrRunCanceled
	}

	// If we entered WAITING (timer scheduled), return nil (resume happens on timer fire)
	if waiting || run.Status == RunStatusWaiting {
		return nil
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
