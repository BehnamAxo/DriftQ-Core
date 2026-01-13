package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFanOutJoinRunsInParallel(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)

	// We want true fan-out concurrency
	r.maxParallel = 4

	// Fan-out nodes will block here until we release them
	release := make(chan struct{})

	// Used only to detect "join started too early" while we are still blocking fanout
	joinStarted := make(chan struct{}, 1)

	var inflight int64
	var maxInflight int64
	var finished int64

	var mu sync.Mutex
	starts := map[string]time.Time{}
	ends := map[string]time.Time{}

	trackMax := func(cur int64) {
		for {
			prev := atomic.LoadInt64(&maxInflight)
			if cur <= prev {
				return
			}

			if atomic.CompareAndSwapInt64(&maxInflight, prev, cur) {
				return
			}
		}
	}

	fanout := func(id string) NodeFunc {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			mu.Lock()
			starts[id] = time.Now().UTC()
			mu.Unlock()

			cur := atomic.AddInt64(&inflight, 1)
			trackMax(cur)

			// Block until test releases all fanout nodes
			select {
			case <-release:
				// proceed
			case <-ctx.Done():
				atomic.AddInt64(&inflight, -1)
				return nil, ctx.Err()
			}

			// Make the overlap measurable (otherwise super fast nodes look "0ms")
			time.Sleep(150 * time.Millisecond)

			mu.Lock()
			ends[id] = time.Now().UTC()
			mu.Unlock()

			atomic.AddInt64(&finished, 1)
			atomic.AddInt64(&inflight, -1)

			return input, nil
		}
	}

	noop := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return input, nil
	}

	join := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		// This should NEVER happen before all fanout deps finished
		if atomic.LoadInt64(&finished) != 4 {
			return nil, fmt.Errorf("join started before fanout finished: finished=%d", atomic.LoadInt64(&finished))
		}
		select {
		case joinStarted <- struct{}{}:
		default:
		}
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
			{NodeID: "J", Run: join}, // join node
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

	// Before releasing, we must see evidence of parallel in-flight work
	// Sequential scheduling can never reach maxInflight >= 2 because the first fanout node blocks
	deadline := time.NewTimer(800 * time.Millisecond)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	defer deadline.Stop()

	for atomic.LoadInt64(&maxInflight) < 2 {
		select {
		case <-joinStarted:
			// If this triggers, join is BROKEN (it started even though deps cannot have finished)
			close(release)
			t.Fatal("join started while fanout nodes were still blocked (join/deps logic is broken)")
		case <-tick.C:
			// keep waiting
		case <-deadline.C:
			close(release) // putting this here to avoid deadlock
			t.Fatalf("expected maxInflight >= 2 before release; got %d (still sequential scheduling)", atomic.LoadInt64(&maxInflight))
		}
	}

	// Now let fanout nodes finish, then join should run
	close(release)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runDAG returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
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

	// Sanity: join node execution should exist + succeed
	nodes := store.ListNodeExecutions(runID)
	foundJ := false
	for _, ne := range nodes {
		if ne.NodeID == "J" {
			foundJ = true
			if ne.Status != NodeStatusSucceeded {
				t.Fatalf("expected join node status %q, got %q", NodeStatusSucceeded, ne.Status)
			}
		}
	}
	if !foundJ {
		t.Fatal("expected join node execution, but none found")
	}

	// Optional extra proof: fanout nodes should overlap in time (not required, but nice)
	mu.Lock()
	defer mu.Unlock()
	for _, id := range []string{"B", "C", "D", "E"} {
		if starts[id].IsZero() || ends[id].IsZero() {
			t.Fatalf("missing timing for node %s (start=%v end=%v)", id, starts[id], ends[id])
		}
	}
}
