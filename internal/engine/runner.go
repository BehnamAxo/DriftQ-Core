package engine

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNodeFailed = errors.New("node failed")
)

type NodeFunc func(ctx context.Context, input json.RawMessage) (json.RawMessage, error)

type Workflow struct {
	WorkflowID string
	Nodes      []NodeDef // ordered for v2.0 (DAG comes later)
}

type NodeDef struct {
	NodeID string
	Run    NodeFunc
}

type Runner struct {
	store Store
}

func NewRunner(store Store) *Runner {
	return &Runner{store: store}
}

func (r *Runner) RunWorkflow(ctx context.Context, runID string, wf Workflow, initialInput json.RawMessage) error {
	// 1) Create run (queued)
	run := Run{
		RunID:      runID,
		WorkflowID: wf.WorkflowID,
		Status:     RunStatusQueued,
	}

	if err := r.store.CreateRun(run); err != nil {
		return err
	}

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
		return err
	}
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

		if err != nil {
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

			run.Status = RunStatusFailed
			run.EndedAt = &nodeEnd
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

		ne.Status = NodeStatusSucceeded
		ne.EndedAt = &nodeEnd
		ne.Output = cloneRaw(output)
		if err := r.store.UpsertNodeExecution(ne); err != nil {
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

	if err := r.store.UpdateRun(run); err != nil {
		return err
	}

	p, _ := json.Marshal(map[string]any{"status": "succeeded"})
	_, _ = r.store.AppendEvent(RunEvent{
		RunID:      runID,
		Type:       EventRunFinished,
		WorkflowID: wf.WorkflowID,
		Payload:    p,
	})

	return nil
}
