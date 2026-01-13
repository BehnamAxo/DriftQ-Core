package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
)

func TestRunDAG_ExternalCancelStopsScheduling(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)

	runID := "cancel-test-1"

	startA := make(chan struct{})
	releaseA := make(chan struct{})

	var startedB int32
	var startedC int32

	g := WorkflowGraph{
		ID: "wf_cancel",
		Nodes: []NodeDef{
			{
				NodeID: "A",
				Run: func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
					close(startA)     // signal A started
					<-releaseA        // block until test releases
					return input, nil // succeed
				},
			},
			{
				NodeID: "B",
				Run: func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
					atomic.AddInt32(&startedB, 1)
					return input, nil
				},
			},
			{
				NodeID: "C",
				Run: func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
					atomic.AddInt32(&startedC, 1)
					return input, nil
				},
			},
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
			{From: "B", To: "C"},
		},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.runDAG(context.Background(), runID, g, json.RawMessage(`{"x":1}`), nil)
	}()

	<-startA // wait until A is definitely running

	// external cancel while A is still running
	if err := r.CancelRun(context.Background(), runID, "test cancel"); err != nil {
		t.Fatalf("CancelRun failed: %v", err)
	}

	close(releaseA) // allow A to finish so scheduler can attempt to move forward

	err := <-errCh
	if !errors.Is(err, ErrRunCanceled) {
		t.Fatalf("expected ErrRunCanceled, got: %v", err)
	}

	// downstream steps must NOT start
	if atomic.LoadInt32(&startedB) != 0 || atomic.LoadInt32(&startedC) != 0 {
		t.Fatalf("downstream started unexpectedly: B=%d C=%d", startedB, startedC)
	}

	// run must be canceled in store
	run, ok := store.GetRun(runID)
	if !ok {
		t.Fatalf("run not found in store")
	}
	if run.Status != RunStatusCanceled {
		t.Fatalf("expected run status canceled, got %q", run.Status)
	}
}
