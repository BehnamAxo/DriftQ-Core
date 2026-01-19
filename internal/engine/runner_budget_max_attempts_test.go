package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestRunDAG_MaxAttemptsBudgetExceeded(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)
	r.maxParallel = 4

	runID := "run_max_attempts"
	wfID := "wf_budget"

	// Pre-create run so we can set RunBudget before execution
	if err := store.CreateRun(Run{
		RunID:      runID,
		WorkflowID: wfID,
		Status:     RunStatusQueued,
		RunBudget: BudgetPolicy{
			MaxAttempts: 1,
		},
		InitialInput: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	ranB := false

	g := WorkflowGraph{
		ID: wfID,
		Nodes: []NodeDef{
			{
				NodeID: "A",
				Run: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					return json.RawMessage(`"ok"`), nil
				},
			},
			{
				NodeID: "B",
				Run: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					ranB = true
					return json.RawMessage(`"should_not_run"`), nil
				},
			},
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
		},
	}

	err := r.RunDAG(context.Background(), runID, g, json.RawMessage(`{}`))
	if !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded, got: %v", err)
	}

	if ranB {
		t.Fatalf("node B should not have run (max attempts = 1)")
	}

	run, ok := store.GetRun(runID)
	if !ok {
		t.Fatalf("run missing from store")
	}

	if run.Status != RunStatusFailed {
		t.Fatalf("expected run status FAILED, got %v", run.Status)
	}

	if run.TerminalReason != "budget_exceeded" {
		t.Fatalf("expected terminal_reason=budget_exceeded, got %q", run.TerminalReason)
	}

	// Node A should be succeeded, node B should not exist (never started)
	nodes := store.ListNodeExecutions(runID)

	var aOK bool
	for _, ne := range nodes {
		if ne.NodeID == "A" && ne.Status == NodeStatusSucceeded {
			aOK = true
		}

		if ne.NodeID == "B" {
			t.Fatalf("unexpected node execution for B (status=%v)", ne.Status)
		}
	}

	if !aOK {
		t.Fatalf("expected node A to succeed; nodes=%v", nodes)
	}

	// Events should include BudgetExceeded + RunFinished.
	evs := store.ListEvents(runID)
	var sawBudget, sawFinished bool
	for _, e := range evs {
		if e.Type == EventBudgetExceeded {
			sawBudget = true
		}

		if e.Type == EventRunFinished {
			sawFinished = true
		}
	}

	if !sawBudget {
		t.Fatalf("expected EventBudgetExceeded")
	}

	if !sawFinished {
		t.Fatalf("expected EventRunFinished")
	}
}
