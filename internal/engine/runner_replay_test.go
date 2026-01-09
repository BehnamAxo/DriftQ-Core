package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
)

func TestRunner_ReplayFailedRun(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	r := NewRunner(store)

	var aCalls int32
	var bCalls int32

	nodeA := func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		atomic.AddInt32(&aCalls, 1)
		return json.RawMessage(`{"a":"ok"}`), nil
	}

	nodeBFail := func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		atomic.AddInt32(&bCalls, 1)
		return nil, errors.New("boom")
	}

	g1 := WorkflowGraph{
		ID: "wf_test",
		Nodes: []NodeDef{
			{NodeID: "A", Run: nodeA},
			{NodeID: "B", Run: nodeBFail},
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
		},
	}

	runID := "replay_fail_test_1"

	// First run should FAIL at B
	if err := r.RunDAG(ctx, runID, g1, json.RawMessage(`{"hello":"world"}`)); err == nil {
		t.Fatalf("expected failure, got nil")
	}

	if got := atomic.LoadInt32(&aCalls); got != 1 {
		t.Fatalf("expected A called 1 time, got %d", got)
	}

	if got := atomic.LoadInt32(&bCalls); got != 1 {
		t.Fatalf("expected B called 1 time, got %d", got)
	}

	// "Fix" B by swapping handler
	nodeBOk := func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		atomic.AddInt32(&bCalls, 1)
		return json.RawMessage(`{"b":"ok"}`), nil
	}

	g2 := WorkflowGraph{
		ID: "wf_test",
		Nodes: []NodeDef{
			{NodeID: "A", Run: nodeA},   // same A
			{NodeID: "B", Run: nodeBOk}, // fixed B
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
		},
	}

	// Second run should resume: A should be skipped, B reruns and succeeds
	if err := r.RunDAG(ctx, runID, g2, json.RawMessage(`{"hello":"world"}`)); err != nil {
		t.Fatalf("expected success on replay, got %v", err)
	}

	if got := atomic.LoadInt32(&aCalls); got != 1 {
		t.Fatalf("expected A still 1 call (skipped on replay), got %d", got)
	}

	if got := atomic.LoadInt32(&bCalls); got != 2 {
		t.Fatalf("expected B called 2 times (fail then replay), got %d", got)
	}
}
