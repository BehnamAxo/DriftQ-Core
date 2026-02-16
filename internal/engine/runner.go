package engine

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"
)

var (
	ErrNodeFailed = errors.New("node failed")
)

const DefaultArtifactInlineLimit = 8 * 1024 // TODO: need to tweak it later

type NodeFunc func(ctx context.Context, input json.RawMessage) (json.RawMessage, error)

type Workflow struct {
	WorkflowID string
	Nodes      []NodeDef // ordered for v2.0 (DAG comes later)
}

type NodeDef struct {
	NodeID    string
	Topic     string
	Run       NodeFunc
	TimeoutMS int
}

type Runner struct {
	store   Store
	metrics *EngineMetrics
	logger  *slog.Logger

	mu       sync.RWMutex
	graphs   map[string]WorkflowGraph // workflow_id -> graph
	registry *HandlerRegistry

	maxParallel int // for join/fan out later
	cancels     map[string]context.CancelFunc

	// v2.5 artifacts
	artifacts           ArtifactStore // optional (nil = inline only)
	artifactInlineLimit int           // bytes, if output > limit => store as artifact + emit ref

	// v2.7 budgets/throttles
	defaultRunBudget BudgetPolicy
	tenantBudgets    map[string]BudgetPolicy
	rateLimiter      RateLimiter

	// (in-memory for now)
	throttleMu      sync.Mutex
	topicCaps       map[string]int            // topic -> cap
	tenantTopicCaps map[string]map[string]int // tenant -> (topic -> cap)
	inflightCaps    map[string]int            // capKey -> inflight count
}

type RunnerOption func(*Runner)

func WithArtifactStore(s ArtifactStore) RunnerOption {
	return func(r *Runner) {
		r.artifacts = s
	}
}

func WithArtifactInlineLimit(n int) RunnerOption {
	return func(r *Runner) {
		if n < 0 {
			n = 0
		}
		r.artifactInlineLimit = n
	}
}

func NewRunner(store Store, opts ...RunnerOption) *Runner {
	r := &Runner{
		store:               store,
		metrics:             NewEngineMetrics(),
		logger:              slog.Default(),
		graphs:              make(map[string]WorkflowGraph),
		maxParallel:         1,
		cancels:             make(map[string]context.CancelFunc),
		artifactInlineLimit: DefaultArtifactInlineLimit,
		tenantBudgets:       make(map[string]BudgetPolicy),
		topicCaps:           make(map[string]int),
		tenantTopicCaps:     make(map[string]map[string]int),
		inflightCaps:        make(map[string]int),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}

	return r
}

func (r *Runner) RunWorkflow(ctx context.Context, runID string, wf Workflow, initialInput json.RawMessage) error {
	traceID := TraceIDFrom(ctx)
	if traceID == "" {
		traceID = NewTraceID()
		ctx = WithTraceID(ctx, traceID)
	}

	// 1) Create run (queued)
	run := Run{
		RunID:      runID,
		WorkflowID: wf.WorkflowID,
		Status:     RunStatusQueued,
	}

	if err := r.store.CreateRun(run); err != nil {
		r.logger.Error("run create failed",
			"trace_id", traceID,
			"run_id", runID,
			"workflow_id", wf.WorkflowID,
			"err", err,
		)
		return err
	}

	r.logger.Info("run created",
		"trace_id", traceID,
		"run_id", runID,
		"workflow_id", wf.WorkflowID,
	)

	_, _ = r.store.AppendEvent(RunEvent{
		RunID:      runID,
		Type:       EventRunCreated,
		WorkflowID: wf.WorkflowID,
	})

	// 2) Start run
	start := time.Now().UTC()
	run.Status = RunStatusRunning
	run.StartedAt = &start

	if err := r.store.UpdateRun(run); err != nil {
		r.logger.Error("run start update failed",
			"trace_id", traceID,
			"run_id", runID,
			"workflow_id", wf.WorkflowID,
			"err", err,
		)
		return err
	}

	r.logger.Info("run started",
		"trace_id", traceID,
		"run_id", runID,
		"workflow_id", wf.WorkflowID,
	)

	_, _ = r.store.AppendEvent(RunEvent{
		RunID:      runID,
		Type:       EventRunStarted,
		WorkflowID: wf.WorkflowID,
	})

	// 3) Execute nodes sequentially
	input := cloneRaw(initialInput)

	for _, node := range wf.Nodes {
		select {
		case <-ctx.Done():
			// mark canceled
			end := time.Now().UTC()
			run.Status = RunStatusCanceled
			run.EndedAt = &end

			// metrics: canceled run duration
			if run.StartedAt != nil {
				dur := end.Sub(*run.StartedAt)
				r.metrics.ObserveRun(run.Status, dur)
			}

			r.logger.Info("run finished",
				"trace_id", traceID,
				"run_id", runID,
				"workflow_id", wf.WorkflowID,
				"status", run.Status,
				"duration_ms", func() int64 {
					if run.StartedAt == nil {
						return 0
					}
					return end.Sub(*run.StartedAt).Milliseconds()
				}(),
				"err", ctx.Err(),
			)

			_ = r.store.UpdateRun(run)
			_, _ = r.store.AppendEvent(RunEvent{
				RunID:      runID,
				Type:       EventRunFinished,
				WorkflowID: wf.WorkflowID,
				Payload:    json.RawMessage(`{"status":"canceled"}`),
			})
			return ctx.Err()

		default:
		}

		attempt := 1
		nodeStart := time.Now().UTC()

		r.logger.Info("node started",
			"trace_id", traceID,
			"run_id", runID,
			"workflow_id", wf.WorkflowID,
			"step_id", node.NodeID,
			"attempt", attempt,
		)

		ne := NodeExecution{
			RunID:      runID,
			WorkflowID: wf.WorkflowID,
			NodeID:     node.NodeID,
			Attempt:    attempt,
			Status:     NodeStatusRunning,
			StartedAt:  &nodeStart,
			Input:      cloneRaw(input),
		}

		if err := r.store.UpsertNodeExecution(ne); err != nil {
			r.logger.Error("node upsert start failed",
				"trace_id", traceID,
				"run_id", runID,
				"workflow_id", wf.WorkflowID,
				"step_id", node.NodeID,
				"attempt", attempt,
				"err", err,
			)
			return err
		}

		_, _ = r.store.AppendEvent(RunEvent{
			RunID:      runID,
			Type:       EventNodeStarted,
			WorkflowID: wf.WorkflowID,
			NodeID:     node.NodeID,
			Attempt:    attempt,
		})

		output, err := node.Run(ctx, cloneRaw(input))
		nodeEnd := time.Now().UTC()
		nodeDur := nodeEnd.Sub(nodeStart)

		if err != nil {
			// metrics: node failed duration
			r.metrics.ObserveNode(node.NodeID, false, nodeDur)

			r.logger.Error("node failed",
				"trace_id", traceID,
				"run_id", runID,
				"workflow_id", wf.WorkflowID,
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
				RunID:      runID,
				Type:       EventNodeFailed,
				WorkflowID: wf.WorkflowID,
				NodeID:     node.NodeID,
				Attempt:    attempt,
				Payload:    p,
			})

			// mark run failed
			run.Status = RunStatusFailed
			run.EndedAt = &nodeEnd

			// metrics: failed run duration
			if run.StartedAt != nil {
				dur := nodeEnd.Sub(*run.StartedAt)
				r.metrics.ObserveRun(run.Status, dur)
			}

			r.logger.Info("run finished",
				"trace_id", traceID,
				"run_id", runID,
				"workflow_id", wf.WorkflowID,
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
				RunID:      runID,
				Type:       EventRunFinished,
				WorkflowID: wf.WorkflowID,
				Payload:    p2,
			})

			return ErrNodeFailed
		}

		// metrics: node succeeded duration
		r.metrics.ObserveNode(node.NodeID, true, nodeDur)

		r.logger.Info("node finished",
			"trace_id", traceID,
			"run_id", runID,
			"workflow_id", wf.WorkflowID,
			"step_id", node.NodeID,
			"attempt", attempt,
			"duration_ms", nodeDur.Milliseconds(),
		)

		ne.Status = NodeStatusSucceeded
		ne.EndedAt = &nodeEnd
		ne.Output = cloneRaw(output)
		if err := r.store.UpsertNodeExecution(ne); err != nil {
			r.logger.Error("node upsert finish failed",
				"trace_id", traceID,
				"run_id", runID,
				"workflow_id", wf.WorkflowID,
				"step_id", node.NodeID,
				"attempt", attempt,
				"err", err,
			)
			return err
		}

		p, _ := json.Marshal(map[string]any{"output": json.RawMessage(output)})
		_, _ = r.store.AppendEvent(RunEvent{
			RunID:      runID,
			Type:       EventNodeFinished,
			WorkflowID: wf.WorkflowID,
			NodeID:     node.NodeID,
			Attempt:    attempt,
			Payload:    p,
		})

		input = cloneRaw(output)
	}

	// 4) Finish run succeeded
	end := time.Now().UTC()
	run.Status = RunStatusSucceeded
	run.EndedAt = &end

	// metrics: succeeded run duration
	if run.StartedAt != nil {
		dur := end.Sub(*run.StartedAt)
		r.metrics.ObserveRun(run.Status, dur)
	}

	if err := r.store.UpdateRun(run); err != nil {
		r.logger.Error("run finish update failed",
			"trace_id", traceID,
			"run_id", runID,
			"workflow_id", wf.WorkflowID,
			"err", err,
		)
		return err
	}

	r.logger.Info("run finished",
		"trace_id", traceID,
		"run_id", runID,
		"workflow_id", wf.WorkflowID,
		"status", run.Status,
		"duration_ms", func() int64 {
			if run.StartedAt == nil {
				return 0
			}
			return end.Sub(*run.StartedAt).Milliseconds()
		}(),
	)

	p, _ := json.Marshal(map[string]any{"status": "succeeded"})
	_, _ = r.store.AppendEvent(RunEvent{
		RunID:      runID,
		Type:       EventRunFinished,
		WorkflowID: wf.WorkflowID,
		Payload:    p,
	})

	return nil
}

func (r *Runner) MetricsSnapshot() MetricsSnapshot {
	return r.metrics.Snapshot()
}

func (r *Runner) SetLogger(l *slog.Logger) {
	if l != nil {
		r.logger = l
	}
}

func (r *Runner) SetHandlerRegistry(reg *HandlerRegistry) {
	r.registry = reg
}

func (r *Runner) HandlerRegistry() *HandlerRegistry {
	return r.registry
}

func (r *Runner) SetMaxParallelism(n int) {
	if n < 1 {
		n = 1
	}
	r.maxParallel = n
}

func (r *Runner) MaxParallelism() int {
	if r.maxParallel < 1 {
		return 1
	}
	return r.maxParallel
}

func (r *Runner) setRunCancel(runID string, cancel context.CancelFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancels == nil {
		r.cancels = make(map[string]context.CancelFunc)
	}
	// If someone accidentally starts same run twice, kill the older one.
	if old, ok := r.cancels[runID]; ok && old != nil {
		old()
	}
	r.cancels[runID] = cancel
}

func (r *Runner) clearRunCancel(runID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.cancels, runID)
}

func (r *Runner) cancelRunContext(runID string) bool {
	r.mu.RLock()
	cancel := r.cancels[runID]
	r.mu.RUnlock()

	if cancel == nil {
		return false
	}
	cancel()
	return true
}

func (r *Runner) SetArtifactStore(s ArtifactStore) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.artifacts = s
}

func (r *Runner) SetArtifactInlineLimit(n int) {
	if n < 0 {
		n = 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.artifactInlineLimit = n
}
