package engine

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"
)

func TestFanOutJoinRunsInParallel(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)

	// IMPORTANT: this is what we’re trying to make real in runDAG.
	// Today (sequential) this test should FAIL. After we add parallel scheduling, it should PASS
	r.maxParallel = 4

	release := make(chan struct{})
	started := make(chan string, 32)

	var inflight int64
	var maxInflight int64

	fanout := func(id string) NodeFunc {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			started <- id

			cur := atomic.AddInt64(&inflight, 1)
			for {
				prev := atomic.LoadInt64(&maxInflight)
				if cur <= prev || atomic.CompareAndSwapInt64(&maxInflight, prev, cur) {
					break
				}
			}

			select {
			case <-release:
				// proceed
			case <-ctx.Done():
				atomic.AddInt64(&inflight, -1)
				return nil, ctx.Err()
			}

			atomic.AddInt64(&inflight, -1)
			return input, nil
		}
	}

	noop := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return input, nil
	}

	g := WorkflowGraph{
		ID: "wf_join_parallel",
		Nodes: []NodeDef{
			{NodeID: "A", Run: noop},
			{NodeID: "B", Run: fanout("B")},
			{NodeID: "C", Run: fanout("C")},
			{NodeID: "D", Run: fanout("D")},
			{NodeID: "E", Run: fanout("E")},
			{NodeID: "J", Run: noop}, // join node
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
			{From: "A", To: "C"},
			{From: "A", To: "D"},
			{From: "A", To: "E"},

			{From: "B", To: "J"},
			{From: "C", To: "J"},
			{From: "D", To: "J"},
			{From: "E", To: "J"},
		},
	}

	runID := "join-parallel-test-1"

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.runDAG(context.Background(), runID, g, json.RawMessage(`{"x":1}`), nil)
	}()

	// Expect at least 2 fanout nodes to start BEFORE we release them
	seen := map[string]bool{}
	deadline := time.NewTimer(250 * time.Millisecond)
	defer deadline.Stop()

	for len(seen) < 2 {
		select {
		case id := <-started:
			seen[id] = true
		case <-deadline.C:
			close(release) // do NOT deadlock the test
			t.Fatalf("expected >=2 fanout nodes to start before release; saw %d (sequential scheduling still active)", len(seen))
		}
	}

	// Let fanout finish and join run
	close(release)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runDAG returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for run to finish")
	}

	if atomic.LoadInt64(&maxInflight) < 2 {
		t.Fatalf("expected max inflight >= 2, got %d", atomic.LoadInt64(&maxInflight))
	}

	run, ok := store.GetRun(runID)
	if !ok {
		t.Fatalf("run not found in store: %s", runID)
	}

	if run.Status != RunStatusSucceeded {
		t.Fatalf("expected run status %q, got %q", RunStatusSucceeded, run.Status)
	}
}
