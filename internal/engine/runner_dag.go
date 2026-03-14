package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otrace "go.opentelemetry.io/otel/trace"
)

var ErrGraphInvalid = errors.New("invalid workflow graph")
var ErrRunCanceled = errors.New("run canceled")

type nodeResult struct {
	nodeID  string
	attempt int
	started time.Time
	ended   time.Time
	input   json.RawMessage
	output  json.RawMessage
	err     error
}

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

func (r *Runner) runDAGWithCache(ctx context.Context, runID string, g WorkflowGraph, initialInput json.RawMessage, spec json.RawMessage, cache map[string]replayCacheEntry) (err error) {
	if err := g.Validate(); err != nil {
		return err
	}

	traceID := TraceIDFrom(ctx)
	if traceID == "" {
		traceID = NewTraceID()
		ctx = WithTraceID(ctx, traceID)
	}

	ctx, runSpan := r.startSpan(ctx, "driftq.workflow.run", workflowSpanAttributes(runID, g.ID, effectiveTenantFromContext(ctx))...)
	defer func() {
		run, ok := r.store.GetRun(runID)
		status := ""
		if ok {
			status = string(run.Status)
		}
		r.finishSpan(runSpan, err, attribute.String("driftq.run_status", status))
	}()

	if _, err := r.authorizeWorkflow(ctx, runID, g); err != nil {
		return err
	}

	riskReport, ctx, err := r.evaluateAndEnforceRisk(ctx, runID, g, initialInput)
	if err != nil {
		var riskErr *RiskError
		if errors.As(err, &riskErr) && riskErr.Report.Action == RiskActionRequireApproval {
			task, stageErr := r.stageRiskApproval(ctx, runID, g, spec, initialInput, riskErr.Report)
			if stageErr != nil {
				return stageErr
			}
			return &HumanApprovalPendingError{Task: task}
		}
		return err
	}

	requestedTenantID := effectiveTenantFromContext(ctx)

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
	attemptsUsed := 0

	if ok {
		run = existingRun
		if err := r.ensureRunTenantAccess(ctx, run, "run.resume"); err != nil {
			return err
		}

		if run.TenantID == "" && requestedTenantID != "" {
			run.TenantID = requestedTenantID
		}

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
		attemptsUsed = len(prevNodes)

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
		if err := r.enforceTenantRunQuota(ctx, requestedTenantID, runID, wfID); err != nil {
			return err
		}
		run = Run{
			RunID:        runID,
			WorkflowID:   wfID,
			Status:       RunStatusQueued,
			Spec:         cloneRaw(spec),
			InitialInput: cloneRaw(initialInput),
			TenantID:     requestedTenantID,
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

		if riskPayload, err := json.Marshal(riskReport); err == nil {
			_, _ = r.store.AppendEvent(RunEvent{
				RunID:      runID,
				Type:       EventRiskAssessed,
				WorkflowID: wfID,
				Payload:    riskPayload,
			})
		}
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

	// v2.7: tenant + effective budget snapshot
	tenantID := run.TenantID
	if tenantID == "" {
		tenantID = requestedTenantID
		if tenantID != "" {
			run.TenantID = tenantID
		}
	}

	defBudget := r.getDefaultRunBudget()
	tenantBudget, _ := r.getTenantBudget(tenantID)
	effBudget := effectiveBudget(defBudget, tenantBudget, run.RunBudget)

	// snapshot effective policy + usage onto the run so debugger/CLI can show it
	run.RunBudget = effBudget
	run.BudgetUsage.Attempts = attemptsUsed
	if run.StartedAt != nil {
		run.BudgetUsage.WallClock = time.Now().UTC().Sub(*run.StartedAt).Milliseconds()
	}
	_ = r.store.UpdateRun(run)

	// make tenant available to handlers
	ctx = WithTenantID(ctx, tenantID)
	ctx = WithAgentStateContext(ctx, r)
	ctx = WithAgentMemoryContext(ctx, r)

	// per-run cancelable context so /run-cancel can interrupt in-flight node.Run
	runCtx, runCancel := context.WithCancel(ctx)

	r.setRunCancel(runID, runCancel)
	defer func() {
		r.clearRunCancel(runID)
		runCancel()
	}()

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

	// FAN-OUT / FAN-IN
	type usageEvent struct {
		nodeID  string
		attempt int
		delta   UsageDelta
	}

	maxPar := r.maxParallel
	if maxPar < 1 {
		maxPar = 1
	}

	resCh := make(chan nodeResult, maxPar)
	usageCh := make(chan usageEvent, maxPar*16) // best-effort
	running := 0

	// ✅ NEW: periodic tick so we don’t block forever while nodes run (wall-clock budget checks, etc.)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()

	inflightKey := map[string]string{}     // nodeID -> capKey
	lastThrottle := map[string]time.Time{} // capKey -> last event time (local suppression)
	throttleRetry := 25 * time.Millisecond

	stopScheduling := false
	externalCanceled := false
	failed := false
	waiting := false
	var failErr error
	budgetExceeded := false

	triggerBudgetExceeded := func(p BudgetExceededPayload) {
		if budgetExceeded {
			return
		}

		budgetExceeded = true

		end := time.Now().UTC()
		run.Status = RunStatusFailed
		run.EndedAt = &end
		run.TerminalReason = "budget_exceeded"

		run.BudgetUsage.Attempts = attemptsUsed
		if run.StartedAt != nil {
			run.BudgetUsage.WallClock = end.Sub(*run.StartedAt).Milliseconds()
		}

		meta, _ := json.Marshal(p)
		run.TerminalMeta = meta
		_ = r.store.UpdateRun(run)

		_, _ = r.store.AppendEvent(RunEvent{
			RunID:      runID,
			Type:       EventBudgetExceeded,
			WorkflowID: wfID,
			Payload:    meta,
		})

		fin, _ := json.Marshal(map[string]any{
			"status":          "failed",
			"terminal_reason": "budget_exceeded",
		})

		_, _ = r.store.AppendEvent(RunEvent{
			RunID:      runID,
			Type:       EventRunFinished,
			WorkflowID: wfID,
			Payload:    fin,
		})

		stopScheduling = true
		runCancel()
	}

	for len(ready) > 0 || running > 0 {

		// wall-clock timeout budget
		if !budgetExceeded && effBudget.WallClockTimeoutMS > 0 && run.StartedAt != nil {
			used := time.Now().UTC().Sub(*run.StartedAt).Milliseconds()
			if used > effBudget.WallClockTimeoutMS {
				triggerBudgetExceeded(BudgetExceededPayload{
					Scope:  BudgetScopeRun,
					Reason: BudgetReasonWallClock,
					Limit:  map[string]any{"wall_clock_timeout_ms": effBudget.WallClockTimeoutMS},
					Used:   map[string]any{"wall_clock_ms": used},
				})
			}
		}

		// external cancel
		if !stopScheduling && isCanceled() {
			externalCanceled = true
			stopScheduling = true
			runCancel()
		}

		// schedule
		maxSkips := len(ready)
		skips := 0

		for !stopScheduling && running < maxPar && len(ready) > 0 {
			// If everything left in ready is throttled, bail out so we can wait for inflight results
			// (otherwise we spin forever and never drain resCh to release caps).
			if skips >= maxSkips {
				break
			}

			// pop ONCE
			nodeID := ready[0]
			ready = ready[1:]

			node, ok := nodeByID[nodeID]
			if !ok {
				return ErrGraphInvalid
			}

			// max attempts budget (check BEFORE caps)
			if !budgetExceeded && effBudget.MaxAttempts > 0 && (attemptsUsed+1) > effBudget.MaxAttempts {
				triggerBudgetExceeded(BudgetExceededPayload{
					Scope:  BudgetScopeRun,
					Reason: BudgetReasonMaxAttempts,
					Limit:  map[string]any{"max_attempts": effBudget.MaxAttempts},
					Used:   map[string]any{"attempts": attemptsUsed},
				})
				break
			}

			// concurrency cap by topic (skip during replay cache runs)
			if cache == nil && node.Topic != "" {
				cap, scope, key := r.concurrencyCapFor(tenantID, node.Topic)
				if cap > 0 {
					acqOK, inflight := r.tryAcquireCap(key, cap)
					if !acqOK {
						ready = append(ready, nodeID)
						skips++

						now := time.Now().UTC()
						if shouldEmitThrottle(lastThrottle, key, now, 250*time.Millisecond) {
							p, _ := json.Marshal(ThrottledPayload{
								Reason:       "concurrency_cap",
								Scope:        scope,
								TenantID:     tenantID,
								Topic:        node.Topic,
								Limit:        cap,
								Inflight:     inflight,
								RetryAfterMS: throttleRetry.Milliseconds(),
							})
							_, _ = r.store.AppendEvent(RunEvent{
								RunID:      runID,
								Type:       EventThrottled,
								WorkflowID: wfID,
								NodeID:     node.NodeID,
								Payload:    p,
							})
						}
						continue
					}
					inflightKey[nodeID] = key
				}
			}

			skips = 0

			// count this attempt as "spent"
			attemptsUsed++
			run.BudgetUsage.Attempts = attemptsUsed
			if run.StartedAt != nil {
				run.BudgetUsage.WallClock = time.Now().UTC().Sub(*run.StartedAt).Milliseconds()
			}
			_ = r.store.UpdateRun(run)

			// build input
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
					if key := inflightKey[nodeID]; key != "" {
						r.releaseCap(key)
						delete(inflightKey, nodeID)
					}
					return err
				}
				nodeInput = json.RawMessage(b)
			}

			// attempt number per node
			attempt := 1
			if prev := maxAttempt[node.NodeID]; prev > 0 {
				attempt = prev + 1
			}
			maxAttempt[node.NodeID] = attempt

			nodeStart := time.Now().UTC()

			if handled, humanRes, err := r.maybeHandleHumanNode(ctx, runID, wfID, node, attempt, nodeInput, nodeStart); err != nil {
				if key := inflightKey[nodeID]; key != "" {
					r.releaseCap(key)
					delete(inflightKey, nodeID)
				}
				return err
			} else if handled {
				if errors.Is(humanRes.err, ErrHumanApprovalPending) {
					run.Status = RunStatusWaiting
					run.EndedAt = nil
					_ = r.store.UpdateRun(run)
					waiting = true
					stopScheduling = true
					if key := inflightKey[nodeID]; key != "" {
						r.releaseCap(key)
						delete(inflightKey, nodeID)
					}
					continue
				}

				running++
				resCh <- humanRes
				continue
			}

			// replay-cache short-circuit
			if cache != nil {
				if entry, ok := cache[node.NodeID]; ok {
					nodeEnd := nodeStart
					nodeDur := nodeEnd.Sub(nodeStart)

					r.logger.Info("node replay-cache hit",
						"trace_id", traceID,
						"run_id", runID,
						"step_id", node.NodeID,
						"attempt", attempt,
						"cached_attempt", entry.Attempt,
					)

					_, _ = r.store.AppendEvent(RunEvent{
						RunID:      runID,
						Type:       EventNodeStarted,
						WorkflowID: wfID,
						NodeID:     node.NodeID,
						Attempt:    attempt,
					})

					out := cloneRaw(entry.Output)

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

					done[node.NodeID] = ne

					payload, err := r.buildNodeFinishedPayload(runCtx, runID, wfID, node.NodeID, attempt, out)
					if err != nil {
						return err
					}
					// Inject proof-grade fields: this node was short-circuited from replay-cache (no worker call).
					payload, err = InjectNodeFinishedProofFields(payload, NodeFinishedProofFields{
						UsedCachedOutput: true,
						CachedAttempt:    entry.Attempt,
						// Queueing isn't tracked yet; treat queued_at as started_at for now (queue_ms=0).
						QueuedAt:  &nodeStart,
						StartedAt: &nodeStart,
						EndedAt:   &nodeEnd,
						QueueMS:   0,
						WorkerMS:  nodeEnd.Sub(nodeStart).Milliseconds(),
					})
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

					r.observeNodeMetric(wfID, node.NodeID, true, nodeDur)
					outputs[node.NodeID] = out

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

			// normal execution
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
				r.logger.Error("node upsert start failed", "trace_id", traceID, "run_id", runID, "step_id", node.NodeID, "attempt", attempt, "err", err)
				if key := inflightKey[nodeID]; key != "" {
					r.releaseCap(key)
					delete(inflightKey, nodeID)
				}
				return err
			}

			_, _ = r.store.AppendEvent(RunEvent{
				RunID:      runID,
				Type:       EventNodeStarted,
				WorkflowID: wfID,
				NodeID:     node.NodeID,
				Attempt:    attempt,
			})

			n := node
			inp := cloneRaw(nodeInput)
			started := nodeStart
			att := attempt

			running++

			go func() {
				stepCtx := WithAttempt(runCtx, att)
				stepCtx = WithExecutionRef(stepCtx, ExecutionRef{
					RunID:      runID,
					WorkflowID: wfID,
					NodeID:     n.NodeID,
					Attempt:    att,
				})

				// Make the runner's artifact store available to node handlers. This is what ArtifactStoreFrom(ctx) reads
				if r.artifacts != nil {
					stepCtx = WithArtifactStoreContext(stepCtx, r.artifacts)
				}

				if rl := r.wrapRateLimiter(runID, wfID, n.NodeID, att, tenantID, n.Topic); rl != nil {
					stepCtx = WithRateLimiter(stepCtx, rl)
				}

				stepCtx = WithUsageSink(stepCtx, func(d UsageDelta) {
					select {
					case usageCh <- usageEvent{nodeID: n.NodeID, attempt: att, delta: d}:
					default:
						// drop
					}
				})

				execCtx := stepCtx
				cancelFn := func() {}
				if n.TimeoutMS > 0 {
					execCtx, cancelFn = context.WithTimeout(stepCtx, time.Duration(n.TimeoutMS)*time.Millisecond)
				}

				execCtx, nodeSpan := r.startSpan(execCtx, "driftq.node.execute", nodeSpanAttributes(runID, wfID, tenantID, n.NodeID, n.Topic, att)...)
				toolCtx := execCtx
				toolSpanStarted := false
				if strings.TrimSpace(n.Topic) != "" {
					toolCtx, _ = r.startSpan(execCtx, "driftq.tool.execute", nodeSpanAttributes(runID, wfID, tenantID, n.NodeID, n.Topic, att)...)
					toolSpanStarted = true
				}

				out, err := func() (out json.RawMessage, err error) {
					defer func() {
						if rec := recover(); rec != nil {
							r.logger.Error("node panicked",
								"trace_id", traceID,
								"run_id", runID,
								"step_id", n.NodeID,
								"attempt", att,
								"panic", rec,
								"stack", string(debug.Stack()),
							)
							err = fmt.Errorf("handler panic: %v", rec)
							out = nil
						}
					}()

					if strings.TrimSpace(n.Topic) != "" {
						return r.invokeTool(toolCtx, toolInvocation{
							RunID:              runID,
							WorkflowID:         wfID,
							NodeID:             n.NodeID,
							Attempt:            att,
							Tool:               n.Topic,
							RequiredCapability: n.RequiredCapability,
							InputSchema:        cloneRaw(n.InputSchema),
							OutputSchema:       cloneRaw(n.OutputSchema),
							Handler:            n.Run,
						}, cloneRaw(inp))
					}
					return n.Run(toolCtx, cloneRaw(inp))
				}()

				cancelFn()
				if toolSpanStarted {
					r.finishSpan(otrace.SpanFromContext(toolCtx), err)
				}

				r.finishSpan(nodeSpan, err)
				if toolSpanStarted && r.obs != nil {
					r.obs.observeTool(wfID, n.NodeID, n.Topic, err == nil, time.Since(started))
				}

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

		// If nothing inflight:
		if running == 0 {
			if stopScheduling {
				break
			}

			if len(ready) > 0 {
				select {
				case <-runCtx.Done():
					if isCanceled() {
						externalCanceled = true
					}
					stopScheduling = true
					runCancel()
				case <-time.After(throttleRetry):
				}
			}
			continue
		}

		// Wait for either: usage, result, cancel, or tick (so we don’t block forever)
		select {
		case <-tick.C:
			// ✅ NEW: wake up periodically so the loop can re-check budgets/cancel/etc.
			continue

		case <-runCtx.Done():
			if isCanceled() {
				externalCanceled = true
			}
			stopScheduling = true
			runCancel()

		case ue := <-usageCh:
			// token/$ accounting + budget enforcement (best-effort)
			if budgetExceeded {
				continue
			}

			if ue.delta.Tokens != 0 {
				run.BudgetUsage.Tokens += ue.delta.Tokens
			}

			if ue.delta.Dollars != 0 {
				run.BudgetUsage.Dollars += ue.delta.Dollars
			}

			if run.StartedAt != nil {
				run.BudgetUsage.WallClock = time.Now().UTC().Sub(*run.StartedAt).Milliseconds()
			}
			_ = r.store.UpdateRun(run)

			if effBudget.MaxTokens > 0 && run.BudgetUsage.Tokens > effBudget.MaxTokens {
				triggerBudgetExceeded(BudgetExceededPayload{
					Scope:  BudgetScopeRun,
					Reason: BudgetReasonTokens,
					Limit:  map[string]any{"max_tokens": effBudget.MaxTokens},
					Used:   map[string]any{"tokens": run.BudgetUsage.Tokens},
				})
				continue
			}

			if effBudget.MaxDollars > 0 && run.BudgetUsage.Dollars > effBudget.MaxDollars {
				triggerBudgetExceeded(BudgetExceededPayload{
					Scope:  BudgetScopeRun,
					Reason: BudgetReasonDollars,
					Limit:  map[string]any{"max_dollars": effBudget.MaxDollars},
					Used:   map[string]any{"dollars": run.BudgetUsage.Dollars},
				})
				continue
			}

		case res := <-resCh:
			running--

			// release any acquired cap slot
			if key := inflightKey[res.nodeID]; key != "" {
				r.releaseCap(key)
				delete(inflightKey, res.nodeID)
			}

			node := nodeByID[res.nodeID]
			nodeEnd := res.ended
			nodeDur := nodeEnd.Sub(res.started)

			// budget exceeded => treat inflight results as canceled
			if budgetExceeded {
				ne := NodeExecution{
					RunID:      runID,
					WorkflowID: wfID,
					NodeID:     res.nodeID,
					Attempt:    res.attempt,
					Status:     NodeStatusCanceled,
					StartedAt:  &res.started,
					EndedAt:    &nodeEnd,
					Input:      cloneRaw(res.input),
					Error:      "budget_exceeded",
				}
				_ = r.store.UpsertNodeExecution(ne)
				continue
			}

			// external cancel
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

				waiting = true
				stopScheduling = true
				continue
			}
			// ---------------------------------------------------

			// failure
			if res.err != nil {
				r.observeNodeMetric(wfID, node.NodeID, false, nodeDur)

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
					r.observeRunMetric(wfID, run.Status, nodeEnd.Sub(*run.StartedAt))
				}

				_ = r.store.UpdateRun(run)
				p2, _ := json.Marshal(map[string]any{"status": "failed", "failed_node": node.NodeID})
				_, _ = r.store.AppendEvent(RunEvent{
					RunID:   runID,
					Type:    EventRunFinished,
					Payload: p2,
				})

				failed = true
				failErr = ErrNodeFailed
				stopScheduling = true
				runCancel()
				continue
			}

			// success
			r.observeNodeMetric(wfID, node.NodeID, true, nodeDur)

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

			// ✅ mark done on normal success
			done[res.nodeID] = ne

			payload, err := r.buildNodeFinishedPayload(runCtx, runID, wfID, node.NodeID, res.attempt, res.output)
			if err != nil {
				return err
			}
			// Inject proof-grade timing fields for finished node.
			start := res.started
			payload, err = InjectNodeFinishedProofFields(payload, NodeFinishedProofFields{
				UsedCachedOutput: false,
				QueuedAt:         &start, // queueing not tracked yet; queue_ms=0
				StartedAt:        &start,
				EndedAt:          &nodeEnd,
				QueueMS:          0,
				WorkerMS:         nodeEnd.Sub(start).Milliseconds(),
			})
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

	if failed {
		return failErr
	}

	if externalCanceled || isCanceled() {
		return ErrRunCanceled
	}

	if waiting || run.Status == RunStatusWaiting {
		return nil
	}

	if isCanceled() {
		return ErrRunCanceled
	}

	if budgetExceeded {
		return ErrBudgetExceeded
	}

	// finish run success
	end := time.Now().UTC()
	run.Status = RunStatusSucceeded
	run.EndedAt = &end

	if run.StartedAt != nil {
		r.observeRunMetric(wfID, run.Status, end.Sub(*run.StartedAt))
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
