package engine

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunDAG_CancelRun_StopsSchedulingMoreNodes(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	r := NewRunner(store)
	r.SetLogger(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))

	// Node A blocks so we can cancel mid-run deterministically.
	startedA := make(chan struct{})
	releaseA := make(chan struct{})

	nodeA := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		select {
		case <-startedA:
			// already closed
		default:
			close(startedA)
		}
		<-releaseA
		return json.RawMessage(`{"a":1}`), nil
	}

	var startedB atomic.Bool
	nodeB := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		startedB.Store(true)
		return json.RawMessage(`{"b":1}`), nil
	}

	g := WorkflowGraph{
		ID: "wf_cancel_test",
		Nodes: []NodeDef{
			{NodeID: "A", Run: nodeA},
			{NodeID: "B", Run: nodeB},
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
		},
	}

	runID := "run_cancel_test"
	initial := json.RawMessage(`{}`)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.RunDAG(ctx, runID, g, initial)
	}()

	// Wait until A is actually running.
	select {
	case <-startedA:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for node A to start: %v", ctx.Err())
	}

	// Cancel via store-based cancel (external cancel).
	if err := r.CancelRun(context.Background(), runID, "test cancel"); err != nil {
		t.Fatalf("CancelRun returned error: %v", err)
	}

	// Let A finish.
	close(releaseA)

	// Runner should stop before scheduling B.
	err := <-errCh
	if err == nil {
		t.Fatalf("expected RunDAG to return an error (ErrRunCanceled), got nil")
	}
	if !errors.Is(err, ErrRunCanceled) {
		t.Fatalf("expected ErrRunCanceled, got: %v", err)
	}

	// Assert run status is canceled (CancelRun writes this).
	run, ok := store.GetRun(runID)
	if !ok {
		t.Fatalf("expected run to exist in store")
	}
	if run.Status != RunStatusCanceled {
		t.Fatalf("expected run status %q, got %q", RunStatusCanceled, run.Status)
	}

	// Assert B never started.
	if startedB.Load() {
		t.Fatalf("node B started, but should not have been scheduled after cancel")
	}

	// Also assert no NodeExecution exists for B attempt 1.
	if _, ok := store.GetNodeExecution(runID, "B", 1); ok {
		t.Fatalf("found node execution for B attempt 1, but B should not have started")
	}

	// And no EventNodeStarted for B.
	for _, e := range store.ListEvents(runID) {
		if e.Type == EventNodeStarted && e.NodeID == "B" {
			t.Fatalf("found EventNodeStarted for B, but B should not have started")
		}
	}
}
