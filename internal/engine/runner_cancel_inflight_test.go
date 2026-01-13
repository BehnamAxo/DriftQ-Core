package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestRunnerCancelInterruptsInflightNode(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)

	// If your Runner uses maxParallel; otherwise harmless.
	r.maxParallel = 1

	// Signals from the running node
	aStarted := make(chan struct{})
	aReturned := make(chan error, 1)

	// Downstream must NOT start after cancel
	bStarted := make(chan struct{}, 1)

	// Node A: blocks ~forever unless ctx is canceled
	slow := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		select {
		case <-aStarted:
		default:
			close(aStarted)
		}

		select {
		case <-time.After(10 * time.Second):
			aReturned <- nil
			return input, nil
		case <-ctx.Done():
			aReturned <- ctx.Err()
			return nil, ctx.Err()
		}
	}

	// Node B: should never run
	after := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		bStarted <- struct{}{}
		return input, nil
	}

	g := WorkflowGraph{
		ID: "wf_runner_cancel_inflight_test",
		Nodes: []NodeDef{
			{NodeID: "A", Run: slow},
			{NodeID: "B", Run: after},
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
		},
	}

	runID := "runner-cancel-inflight-test-1"

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.runDAG(context.Background(), runID, g, json.RawMessage(`{"x":1}`), json.RawMessage(`{"id":"wf_runner_cancel_inflight_test"}`))
	}()

	// Wait until A is actually running
	select {
	case <-aStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for node A to start")
	}

	time.Sleep(50 * time.Millisecond) // make it truly "in-flight" :)
	cancelAt := time.Now()

	if err := r.CancelRun(context.Background(), runID, "test"); err != nil {
		t.Fatalf("CancelRun error: %v", err)
	}

	// runDAG should return quickly (proof: this is for cancel interrupts in-flight)
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("expected non-nil error from runDAG after cancel")
		}

		if !errors.Is(err, ErrRunCanceled) && !errors.Is(err, context.Canceled) {
			t.Fatalf("expected ErrRunCanceled or context.Canceled, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("cancel did NOT interrupt inflight node within 2s; elapsed=%s", time.Since(cancelAt))
	}

	// Node A must exit quickly due to ctx cancellation
	select {
	case aErr := <-aReturned:
		if aErr == nil {
			t.Fatalf("expected node A to return an error due to cancel, got nil")
		}
		if !errors.Is(aErr, context.Canceled) {
			t.Fatalf("expected node A error context.Canceled, got: %v", aErr)
		}
	case <-time.After(750 * time.Millisecond):
		t.Fatal("node A did not return quickly after cancel")
	}

	// Node B must NOT start
	select {
	case <-bStarted:
		t.Fatal("node B started even though run was canceled")
	default:
	}

	// Store should show canceled
	run, ok := store.GetRun(runID)
	if !ok {
		t.Fatalf("run not found in store: %s", runID)
	}

	if run.Status != RunStatusCanceled {
		t.Fatalf("expected run status %q, got %q", RunStatusCanceled, run.Status)
	}
}
