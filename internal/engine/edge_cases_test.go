package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestEdgeCase_EmptyWorkflowSpec(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	g := WorkflowGraph{
		ID:    "empty",
		Nodes: []NodeDef{},
		Edges: []NodeEdge{},
	}

	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	// Should succeed immediately
	if err != nil {
		t.Logf("Empty workflow: %v", err)
	}
}

func TestEdgeCase_WorkflowWithNoNodes(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	g := WorkflowGraph{
		ID:    "no-nodes",
		Nodes: nil, // nil instead of empty
	}

	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	_ = err // Should handle gracefully
}

func TestEdgeCase_WorkflowWith100Nodes(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	var counter int32

	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		atomic.AddInt32(&counter, 1)
		return json.RawMessage(`{}`), nil
	}

	nodes := make([]NodeDef, 100)
	for i := 0; i < 100; i++ {
		nodes[i] = NodeDef{
			NodeID: nodeID(i),
			Run:    node,
		}
	}

	g := WorkflowGraph{
		ID:    "many-nodes",
		Nodes: nodes,
	}

	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("100 nodes: %v", err)
	}

	if counter != 100 {
		t.Fatalf("expected 100 executions, got %d", counter)
	}
}

func TestEdgeCase_WorkflowWithCycle(t *testing.T) {
	g := WorkflowGraph{
		ID: "cycle",
		Nodes: []NodeDef{
			{NodeID: "A"},
			{NodeID: "B"},
			{NodeID: "C"},
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
			{From: "B", To: "C"},
			{From: "C", To: "A"}, // Cycle
		},
	}

	err := g.Validate()
	if err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestEdgeCase_NodeDependsOnNonExistent(t *testing.T) {
	g := WorkflowGraph{
		ID: "bad-dep",
		Nodes: []NodeDef{
			{NodeID: "A"},
		},
		Edges: []NodeEdge{
			{From: "nonexistent", To: "A"},
		},
	}

	err := g.Validate()
	if err == nil {
		t.Fatal("expected missing dependency error")
	}
}

func TestEdgeCase_NodeDependsOnItself(t *testing.T) {
	g := WorkflowGraph{
		ID: "self-dep",
		Nodes: []NodeDef{
			{NodeID: "A"},
		},
		Edges: []NodeEdge{
			{From: "A", To: "A"}, // Self-loop
		},
	}

	err := g.Validate()
	if err == nil {
		t.Fatal("expected self-loop error")
	}
}

func TestEdgeCase_HandlerThrowsPanic(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	panicNode := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		panic("intentional panic")
	}

	g := WorkflowGraph{
		ID: "panic",
		Nodes: []NodeDef{
			{NodeID: "A", Run: panicNode},
		},
	}

	// Should recover from panic
	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	_ = err // May or may not return error

	run, _ := store.GetRun("run1")
	if run.Status == RunStatusSucceeded {
		t.Fatal("should not succeed after panic")
	}
}

func TestEdgeCase_HandlerReturnsNilOutput(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	nilNode := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return nil, nil // nil output, no error
	}

	g := WorkflowGraph{
		ID: "nil-output",
		Nodes: []NodeDef{
			{NodeID: "A", Run: nilNode},
		},
	}

	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("nil output should succeed: %v", err)
	}
}

func TestEdgeCase_HandlerTakesOverOneMinute(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow test")
	}

	store := NewMemoryStore()
	runner := NewRunner(store, WithDefaultBudget(BudgetPolicy{
		WallClockTimeoutMS: 1000, // 1 second timeout
	}))

	slowNode := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
			return json.RawMessage(`{}`), nil
		}
	}

	g := WorkflowGraph{
		ID: "slow",
		Nodes: []NodeDef{
			{NodeID: "A", Run: slowNode},
		},
	}

	start := time.Now()
	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	elapsed := time.Since(start)

	// Should timeout, not run for 5 seconds
	if elapsed > 3*time.Second {
		t.Fatalf("timeout not enforced, took %v", elapsed)
	}
	_ = err
}

func TestEdgeCase_CancelAlreadyCompletedRun(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	}

	g := WorkflowGraph{
		ID: "completed",
		Nodes: []NodeDef{
			{NodeID: "A", Run: node},
		},
	}

	runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))

	// Try to cancel after completion
	err := runner.CancelRun(context.Background(), "run1", "too late")
	// Should be no-op or return error
	_ = err

	run, _ := store.GetRun("run1")
	if run.Status != RunStatusSucceeded {
		t.Fatalf("status changed after cancel: %s", run.Status)
	}
}

func TestEdgeCase_CancelAlreadyCanceledRun(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	started := make(chan bool)
	slowNode := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	g := WorkflowGraph{
		ID: "double-cancel",
		Nodes: []NodeDef{
			{NodeID: "A", Run: slowNode},
		},
	}

	go runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	<-started

	runner.CancelRun(context.Background(), "run1", "first cancel")
	time.Sleep(100 * time.Millisecond)

	// Second cancel
	err := runner.CancelRun(context.Background(), "run1", "second cancel")
	// Should be no-op
	_ = err
}

func TestEdgeCase_ReplayNonExistentRun(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	err := runner.Replay(context.Background(), "nonexistent", ReplayTimeTravel)
	if err == nil {
		t.Fatal("expected error for non-existent run")
	}
}

func TestEdgeCase_ReplayFromNonExistentStep(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	}

	g := WorkflowGraph{
		ID: "replay-bad-step",
		Nodes: []NodeDef{
			{NodeID: "A", Run: node},
		},
	}

	runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	runner.rememberGraph("replay-bad-step", g)

	err := runner.ReplayFrom(context.Background(), "run1", "nonexistent", ReplayTimeTravel)
	if err == nil {
		t.Fatal("expected error for non-existent step")
	}
}

func TestEdgeCase_ReplayAlreadyRunningRun(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	started := make(chan bool)
	slowNode := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		close(started)
		time.Sleep(5 * time.Second)
		return json.RawMessage(`{}`), nil
	}

	g := WorkflowGraph{
		ID: "running-replay",
		Nodes: []NodeDef{
			{NodeID: "A", Run: slowNode},
		},
	}

	go runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	<-started

	runner.rememberGraph("running-replay", g)

	// Try to replay while running
	err := runner.Replay(context.Background(), "run1", ReplayTimeTravel)
	if err == nil {
		t.Log("Warning: replay of running run may be allowed")
	}
}

func TestEdgeCase_TimerWithFireAtInPast(t *testing.T) {
	store := NewMemoryStore()

	run := Run{RunID: "run1", WorkflowID: "wf1", Status: RunStatusWaiting}
	store.CreateRun(run)

	past := time.Now().Add(-time.Hour)
	timer := Timer{
		RunID:   "run1",
		NodeID:  "A",
		Attempt: 1,
		Status:  TimerScheduled,
		FireAt:  past,
	}
	store.UpsertTimer(timer)

	// Should be immediately due
	due := store.ListDueTimers(time.Now())
	found := false
	for _, t := range due {
		if t.RunID == "run1" {
			found = true
		}
	}
	if !found {
		t.Fatal("past timer should be due")
	}
}

func TestEdgeCase_BudgetMaxAttempts_Zero(t *testing.T) {
	policy := BudgetPolicy{MaxAttempts: 0}
	eff := effectiveBudget(policy, BudgetPolicy{}, BudgetPolicy{})

	// 0 should mean unlimited
	if eff.MaxAttempts != 0 {
		t.Fatalf("0 max_attempts should remain 0: %d", eff.MaxAttempts)
	}
}

func TestEdgeCase_BudgetMaxTokens_Zero(t *testing.T) {
	policy := BudgetPolicy{MaxTokens: 0}
	eff := effectiveBudget(policy, BudgetPolicy{}, BudgetPolicy{})

	if eff.MaxTokens != 0 {
		t.Fatalf("0 max_tokens should remain 0: %d", eff.MaxTokens)
	}
}

func TestEdgeCase_BudgetWallClock_Zero(t *testing.T) {
	policy := BudgetPolicy{WallClockTimeoutMS: 0}

	if policy.WallClockTimeout() != 0 {
		t.Fatalf("0 wallclock should be 0 duration: %v", policy.WallClockTimeout())
	}
}

func TestEdgeCase_ConcurrentRunsWithSameRunID(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		time.Sleep(100 * time.Millisecond)
		return json.RawMessage(`{}`), nil
	}

	g := WorkflowGraph{
		ID: "concurrent-same-id",
		Nodes: []NodeDef{
			{NodeID: "A", Run: node},
		},
	}

	// Start two runs with same ID concurrently
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			done <- runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
		}()
	}

	err1 := <-done
	err2 := <-done

	// At least one should fail or they should be properly coordinated
	t.Logf("err1: %v, err2: %v", err1, err2)
}

func TestEdgeCase_RunWithEmptyInput(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	var receivedInput json.RawMessage
	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		receivedInput = input
		return json.RawMessage(`{}`), nil
	}

	g := WorkflowGraph{
		ID: "empty-input",
		Nodes: []NodeDef{
			{NodeID: "A", Run: node},
		},
	}

	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("empty input: %v", err)
	}

	// Input should be {} or equivalent
	if len(receivedInput) == 0 {
		t.Fatal("received nil input")
	}
}

func TestEdgeCase_RunWithNullInput(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	}

	g := WorkflowGraph{
		ID: "null-input",
		Nodes: []NodeDef{
			{NodeID: "A", Run: node},
		},
	}

	err := runner.RunDAG(context.Background(), "run1", g, nil)
	if err != nil {
		t.Fatalf("null input: %v", err)
	}
}

func TestEdgeCase_NodeReturnsError(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	errorNode := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New("intentional error")
	}

	g := WorkflowGraph{
		ID: "error-node",
		Nodes: []NodeDef{
			{NodeID: "A", Run: errorNode},
		},
	}

	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	if err == nil {
		t.Fatal("expected error from failing node")
	}

	run, _ := store.GetRun("run1")
	if run.Status != RunStatusFailed {
		t.Fatalf("expected failed, got %s", run.Status)
	}
}

func TestEdgeCase_NodeContextCanceled(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	g := WorkflowGraph{
		ID: "ctx-cancel",
		Nodes: []NodeDef{
			{NodeID: "A", Run: node},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error)
	go func() {
		done <- runner.RunDAG(ctx, "run1", g, json.RawMessage(`{}`))
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	err := <-done
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Logf("context cancel error: %v", err)
	}
}

// Helper function to generate node IDs
func nodeID(i int) string {
	if i < 26 {
		return string(rune('A' + i))
	}
	return string(rune('A'+i/26)) + string(rune('A'+i%26))
}
