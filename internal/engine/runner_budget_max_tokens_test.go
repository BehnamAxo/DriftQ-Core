package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestRunDAG_MaxTokensBudgetExceeded(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)
	r.maxParallel = 4

	runID := "run_max_tokens"
	wfID := "wf_budget_tokens"

	// Pre-create run so we can set budget before execution
	if err := store.CreateRun(Run{
		RunID:      runID,
		WorkflowID: wfID,
		Status:     RunStatusQueued,
		RunBudget: BudgetPolicy{
			MaxTokens: 1, // tiny limit so we exceed immediately
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
				Topic:  "llm",
				Run: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					// Emit usage, then block until the runner cancels us due to budget exceeded.
					if sink, ok := ctx.Value(usageSinkKey{}).(UsageSink); ok && sink != nil {
						sink(UsageDelta{Tokens: 2})
					}

					<-ctx.Done()
					return nil, ctx.Err()
				},
			},
			{
				NodeID: "B",
				Topic:  "llm",
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
		t.Fatalf("node B should not have run after budget exceeded")
	}

	run, ok := store.GetRun(runID)
	if !ok {
		t.Fatalf("expected run to exist in store")
	}

	if run.Status != RunStatusFailed {
		t.Fatalf("expected run.Status=failed, got: %s", run.Status)
	}

	if run.TerminalReason != "budget_exceeded" {
		t.Fatalf("expected run.TerminalReason=budget_exceeded, got: %q", run.TerminalReason)
	}

	if run.BudgetUsage.Tokens <= 1 {
		t.Fatalf("expected tokens usage to exceed limit; got: %d", run.BudgetUsage.Tokens)
	}
}
