package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestRunDAG_WallClockTimeoutBudgetExceeded(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)
	r.maxParallel = 1

	runID := "run_wallclock_timeout"
	wfID := "wf_budget_wallclock"

	// Pre-create run so we can set budget before execution
	if err := store.CreateRun(Run{
		RunID:        runID,
		WorkflowID:   wfID,
		Status:       RunStatusQueued,
		RunBudget:    BudgetPolicy{WallClockTimeoutMS: 10}, // 10ms timeout
		InitialInput: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	g := WorkflowGraph{
		ID: wfID,
		Nodes: []NodeDef{
			{
				NodeID: "A",
				Topic:  "llm",
				Run: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(250 * time.Millisecond):
						return json.RawMessage(`"too_slow"`), nil
					}
				},
			},
		},
	}

	// Guard against a dead hang
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := r.RunDAG(ctx, runID, g, json.RawMessage(`{}`))
	if !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded, got: %v", err)
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
}
