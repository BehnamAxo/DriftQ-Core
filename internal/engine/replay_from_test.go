package engine

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
)

func TestReplayFrom_TimeTravel_DoesNotReexecuteSucceededFromStep(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)
	runner.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	aCalls := 0
	bCalls := 0

	nodeA := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		aCalls++
		return json.RawMessage(`{"a":1}`), nil
	}

	nodeB := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		bCalls++
		if bCalls == 1 {
			return nil, errors.New("boom")
		}
		return json.RawMessage(`{"b":2}`), nil
	}

	g := WorkflowGraph{
		ID: "wf",
		Nodes: []NodeDef{
			{NodeID: "A", Run: nodeA},
			{NodeID: "B", Run: nodeB},
		},
		Edges: []NodeEdge{{From: "A", To: "B"}},
	}

	// 1) initial run fails in B
	err := runner.RunDAG(context.Background(), "r1", g, json.RawMessage(`{}`))
	if err == nil {
		t.Fatalf("expected run to fail")
	}
	if !errors.Is(err, ErrNodeFailed) {
		// The runner wraps some failures; ErrNodeFailed is the stable sentinel.
		t.Fatalf("expected ErrNodeFailed, got: %v", err)
	}
	if aCalls != 1 {
		t.Fatalf("expected A to run once, got %d", aCalls)
	}
	if bCalls != 1 {
		t.Fatalf("expected B to run once, got %d", bCalls)
	}

	// 2) time-travel replay from A should *not* rerun A (it already succeeded)
	if err := runner.ReplayFrom(context.Background(), "r1", "A", ReplayTimeTravel); err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	if aCalls != 1 {
		t.Fatalf("expected A not to rerun in time-travel, got %d", aCalls)
	}
	if bCalls != 2 {
		t.Fatalf("expected B to rerun once, got %d", bCalls)
	}

	// Ensure we have B attempt 2 succeeded
	nodes := store.ListNodeExecutions("r1")
	var b2OK bool
	for _, ne := range nodes {
		if ne.NodeID == "B" && ne.Attempt == 2 && ne.Status == NodeStatusSucceeded {
			b2OK = true
		}
	}
	if !b2OK {
		t.Fatalf("expected B attempt 2 to succeed")
	}
}

func TestReplayFrom_Live_ReexecutesFromStep(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)
	runner.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	aCalls := 0
	bCalls := 0

	nodeA := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		aCalls++
		return json.RawMessage(`{"a":1}`), nil
	}

	nodeB := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		bCalls++
		if bCalls == 1 {
			return nil, errors.New("boom")
		}
		return json.RawMessage(`{"b":2}`), nil
	}

	g := WorkflowGraph{
		ID: "wf",
		Nodes: []NodeDef{
			{NodeID: "A", Run: nodeA},
			{NodeID: "B", Run: nodeB},
		},
		Edges: []NodeEdge{{From: "A", To: "B"}},
	}

	// initial run fails in B
	if err := runner.RunDAG(context.Background(), "r1", g, json.RawMessage(`{}`)); err == nil {
		t.Fatalf("expected run to fail")
	}
	if aCalls != 1 || bCalls != 1 {
		t.Fatalf("expected A=1,B=1; got A=%d B=%d", aCalls, bCalls)
	}

	// live replay from A should rerun A and B
	if err := runner.ReplayFrom(context.Background(), "r1", "A", ReplayLive); err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if aCalls != 2 {
		t.Fatalf("expected A to rerun in live mode, got %d", aCalls)
	}
	if bCalls != 2 {
		t.Fatalf("expected B to rerun in live mode, got %d", bCalls)
	}

	// Verify the original A attempt 1 got canceled (history preserved)
	nodes := store.ListNodeExecutions("r1")
	var a1Canceled bool
	for _, ne := range nodes {
		if ne.NodeID == "A" && ne.Attempt == 1 && ne.Status == NodeStatusCanceled {
			a1Canceled = true
		}
	}
	if !a1Canceled {
		t.Fatalf("expected A attempt 1 to be marked canceled on live replay")
	}
}
