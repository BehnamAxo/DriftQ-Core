package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// Basic Workflow Tests
func TestRunner_SingleNodeWorkflow(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"result": "done"}`), nil
	}

	wf := Workflow{
		WorkflowID: "wf_single",
		Nodes: []NodeDef{
			{NodeID: "A", Run: node},
		},
	}

	err := runner.RunWorkflow(context.Background(), "run1", wf, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("RunWorkflow: %v", err)
	}

	run, ok := store.GetRun("run1")
	if !ok {
		t.Fatal("run not found")
	}

	if run.Status != RunStatusSucceeded {
		t.Fatalf("expected succeeded, got %s", run.Status)
	}
}

func TestRunner_LinearChainWorkflow(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	callOrder := []string{}

	makeNode := func(name string) func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			callOrder = append(callOrder, name)
			return json.RawMessage(`{}`), nil
		}
	}

	g := WorkflowGraph{
		ID: "wf_chain",
		Nodes: []NodeDef{
			{NodeID: "A", Run: makeNode("A")},
			{NodeID: "B", Run: makeNode("B")},
			{NodeID: "C", Run: makeNode("C")},
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
			{From: "B", To: "C"},
		},
	}

	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("RunDAG: %v", err)
	}

	if len(callOrder) != 3 {
		t.Fatalf("expected 3 calls, got %d", len(callOrder))
	}

	if callOrder[0] != "A" || callOrder[1] != "B" || callOrder[2] != "C" {
		t.Fatalf("wrong order: %v", callOrder)
	}
}

func TestRunner_FanOutFanIn(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	var aCompleted, bCompleted, cCompleted, dCompleted int32

	nodeA := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		atomic.AddInt32(&aCompleted, 1)
		return json.RawMessage(`{"from": "A"}`), nil
	}

	nodeB := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt32(&bCompleted, 1)
		return json.RawMessage(`{"from": "B"}`), nil
	}

	nodeC := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		time.Sleep(30 * time.Millisecond)
		atomic.AddInt32(&cCompleted, 1)
		return json.RawMessage(`{"from": "C"}`), nil
	}

	nodeD := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		// Should receive outputs from both B and C
		atomic.AddInt32(&dCompleted, 1)
		return json.RawMessage(`{"from": "D"}`), nil
	}

	g := WorkflowGraph{
		ID: "wf_fanout",
		Nodes: []NodeDef{
			{NodeID: "A", Run: nodeA},
			{NodeID: "B", Run: nodeB},
			{NodeID: "C", Run: nodeC},
			{NodeID: "D", Run: nodeD},
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
			{From: "A", To: "C"},
			{From: "B", To: "D"},
			{From: "C", To: "D"},
		},
	}

	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("RunDAG: %v", err)
	}

	if aCompleted != 1 || bCompleted != 1 || cCompleted != 1 || dCompleted != 1 {
		t.Fatalf("not all nodes completed: A=%d B=%d C=%d D=%d",
			aCompleted, bCompleted, cCompleted, dCompleted)
	}

	run, _ := store.GetRun("run1")
	if run.Status != RunStatusSucceeded {
		t.Fatalf("expected succeeded, got %s", run.Status)
	}
}

// Error Handling Tests
func TestRunner_NodeFailure(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	failingNode := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New("intentional failure")
	}

	g := WorkflowGraph{
		ID: "wf_fail",
		Nodes: []NodeDef{
			{NodeID: "A", Run: failingNode},
		},
	}

	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	if err == nil {
		t.Fatal("expected error")
	}

	run, _ := store.GetRun("run1")
	if run.Status != RunStatusFailed {
		t.Fatalf("expected failed, got %s", run.Status)
	}
}

func TestRunner_DownstreamNotRunAfterFailure(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	var bCalled bool

	nodeA := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New("A failed")
	}

	nodeB := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		bCalled = true
		return json.RawMessage(`{}`), nil
	}

	g := WorkflowGraph{
		ID: "wf_downstream",
		Nodes: []NodeDef{
			{NodeID: "A", Run: nodeA},
			{NodeID: "B", Run: nodeB},
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
		},
	}

	runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))

	if bCalled {
		t.Fatal("B should not have been called after A failed")
	}
}

func TestRunner_NodePanic(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	panicNode := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		panic("intentional panic")
	}

	g := WorkflowGraph{
		ID: "wf_panic",
		Nodes: []NodeDef{
			{NodeID: "A", Run: panicNode},
		},
	}

	// Should recover from panic
	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	if err == nil {
		t.Log("Warning: panic may not be recovered or may be wrapped")
	}

	run, ok := store.GetRun("run1")
	if ok && run.Status == RunStatusSucceeded {
		t.Fatal("run should not succeed after panic")
	}
}

func TestRunner_NodeNilOutput(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	nilNode := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return nil, nil // nil output but no error
	}

	g := WorkflowGraph{
		ID: "wf_nil_output",
		Nodes: []NodeDef{
			{NodeID: "A", Run: nilNode},
		},
	}

	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("RunDAG: %v", err)
	}
}

// Cancel Tests
func TestRunner_CancelRun(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	started := make(chan bool)
	slowNode := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		close(started)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Second):
			return json.RawMessage(`{}`), nil
		}
	}

	g := WorkflowGraph{
		ID: "wf_cancel",
		Nodes: []NodeDef{
			{NodeID: "slow", Run: slowNode},
		},
	}

	go func() {
		runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	}()

	<-started

	err := runner.CancelRun(context.Background(), "run1", "test cancel")
	if err != nil {
		t.Fatalf("CancelRun: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	run, _ := store.GetRun("run1")
	if run.Status != RunStatusCanceled {
		t.Fatalf("expected canceled, got %s", run.Status)
	}
}

func TestRunner_CancelNonExistentRun(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	err := runner.CancelRun(context.Background(), "nonexistent", "test")
	if err == nil {
		t.Fatal("expected error for non-existent run")
	}
}

func TestRunner_CancelAlreadyCompleted(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	}

	g := WorkflowGraph{
		ID: "wf_completed",
		Nodes: []NodeDef{
			{NodeID: "A", Run: node},
		},
	}

	runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))

	// Try to cancel completed run
	err := runner.CancelRun(context.Background(), "run1", "too late")
	// May return error or be no-op
	_ = err
}

// Timeout Tests
func TestRunner_NodeTimeout(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	slowNode := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
			return json.RawMessage(`{}`), nil
		}
	}

	g := WorkflowGraph{
		ID: "wf_timeout",
		Nodes: []NodeDef{
			{NodeID: "slow", Run: slowNode, TimeoutMS: 100}, // 100ms timeout
		},
	}

	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	if err == nil {
		t.Log("Warning: timeout may not return error directly")
	}

	run, _ := store.GetRun("run1")
	if run.Status == RunStatusSucceeded {
		t.Fatal("run should not succeed with timeout")
	}
}

// Input/Output Passing Tests
func TestRunner_InputPassingToFirstNode(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	var receivedInput json.RawMessage

	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		receivedInput = input
		return json.RawMessage(`{}`), nil
	}

	g := WorkflowGraph{
		ID: "wf_input",
		Nodes: []NodeDef{
			{NodeID: "A", Run: node},
		},
	}

	initial := json.RawMessage(`{"key": "value", "num": 42}`)
	runner.RunDAG(context.Background(), "run1", g, initial)

	var m map[string]interface{}
	if err := json.Unmarshal(receivedInput, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if m["key"] != "value" {
		t.Fatalf("expected key=value, got %v", m)
	}
}

func TestRunner_OutputPassingBetweenNodes(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	nodeA := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"a_output": 123}`), nil
	}

	var bInput json.RawMessage
	nodeB := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		bInput = input
		return json.RawMessage(`{}`), nil
	}

	g := WorkflowGraph{
		ID: "wf_output",
		Nodes: []NodeDef{
			{NodeID: "A", Run: nodeA},
			{NodeID: "B", Run: nodeB},
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
		},
	}

	runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))

	// B's input should contain A's output
	var m map[string]interface{}
	if err := json.Unmarshal(bInput, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// When single dependency, input is keyed by node ID
	aOut, ok := m["A"]
	if !ok {
		t.Fatalf("expected A's output in B's input, got %s", bInput)
	}
	_ = aOut
}

// Event Logging Tests
func TestRunner_EventsLogged(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	}

	g := WorkflowGraph{
		ID: "wf_events",
		Nodes: []NodeDef{
			{NodeID: "A", Run: node},
		},
	}

	runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))

	events := store.ListEvents("run1")
	if len(events) < 4 {
		t.Fatalf("expected at least 4 events, got %d", len(events))
	}

	eventTypes := make(map[RunEventType]bool)
	for _, e := range events {
		eventTypes[e.Type] = true
	}

	required := []RunEventType{
		EventRunCreated,
		EventRunStarted,
		EventNodeStarted,
		EventNodeFinished,
		EventRunFinished,
	}

	for _, et := range required {
		if !eventTypes[et] {
			t.Errorf("missing event type: %s", et)
		}
	}
}

func TestRunner_EventSequenceIncreasing(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	}

	g := WorkflowGraph{
		ID: "wf_seq",
		Nodes: []NodeDef{
			{NodeID: "A", Run: node},
			{NodeID: "B", Run: node},
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
		},
	}

	runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))

	events := store.ListEvents("run1")
	for i := 1; i < len(events); i++ {
		if events[i].Seq <= events[i-1].Seq {
			t.Fatalf("event sequence not increasing: %d <= %d", events[i].Seq, events[i-1].Seq)
		}
	}
}

// Node Execution Records Tests
func TestRunner_NodeExecutionsRecorded(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"output": "test"}`), nil
	}

	g := WorkflowGraph{
		ID: "wf_exec",
		Nodes: []NodeDef{
			{NodeID: "A", Run: node},
		},
	}

	runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{"input": "data"}`))

	execs := store.ListNodeExecutions("run1")
	if len(execs) != 1 {
		t.Fatalf("expected 1 execution, got %d", len(execs))
	}

	exec := execs[0]
	if exec.NodeID != "A" {
		t.Fatalf("expected node A, got %s", exec.NodeID)
	}

	if exec.Status != NodeStatusSucceeded {
		t.Fatalf("expected succeeded, got %s", exec.Status)
	}

	if exec.StartedAt == nil || exec.EndedAt == nil {
		t.Fatal("timestamps not set")
	}

	if len(exec.Input) == 0 {
		t.Fatal("input not recorded")
	}

	if len(exec.Output) == 0 {
		t.Fatal("output not recorded")
	}
}

// Empty/Edge Case Tests
func TestRunner_EmptyWorkflowSpec(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	g := WorkflowGraph{
		ID:    "wf_empty",
		Nodes: []NodeDef{},
		Edges: []NodeEdge{},
	}

	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	// Empty workflow should succeed immediately
	if err != nil {
		t.Logf("empty workflow error: %v", err)
	}
}

func TestRunner_EmptyInput(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	}

	g := WorkflowGraph{
		ID: "wf_empty_input",
		Nodes: []NodeDef{
			{NodeID: "A", Run: node},
		},
	}

	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("empty input: %v", err)
	}
}

func TestRunner_NullInput(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	}

	g := WorkflowGraph{
		ID: "wf_null_input",
		Nodes: []NodeDef{
			{NodeID: "A", Run: node},
		},
	}

	err := runner.RunDAG(context.Background(), "run1", g, nil)
	if err != nil {
		t.Fatalf("null input: %v", err)
	}
}

func TestRunner_SameRunIDTwice(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	}

	g := WorkflowGraph{
		ID: "wf_dup",
		Nodes: []NodeDef{
			{NodeID: "A", Run: node},
		},
	}

	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("first run: %v", err)
	}

	// Second run with same ID should handle gracefully
	err = runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	// May succeed (idempotent) or fail - both are valid
	_ = err
}

// Large Workflow Tests
func TestRunner_ManyNodes(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	var counter int32

	makeNode := func() func(context.Context, json.RawMessage) (json.RawMessage, error) {
		return func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			atomic.AddInt32(&counter, 1)
			return json.RawMessage(`{}`), nil
		}
	}

	// Create 50 independent nodes
	nodes := make([]NodeDef, 50)
	for i := 0; i < 50; i++ {
		nodes[i] = NodeDef{
			NodeID: string(rune('A'+i/26)) + string(rune('A'+i%26)),
			Run:    makeNode(),
		}
	}

	g := WorkflowGraph{
		ID:    "wf_many",
		Nodes: nodes,
	}

	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("many nodes: %v", err)
	}

	if counter != 50 {
		t.Fatalf("expected 50 nodes, ran %d", counter)
	}
}

func TestRunner_DeepChain(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	depth := 20
	nodes := make([]NodeDef, depth)
	edges := make([]NodeEdge, depth-1)

	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	}

	for i := 0; i < depth; i++ {
		nodes[i] = NodeDef{
			NodeID: string(rune('A' + i)),
			Run:    node,
		}
		if i > 0 {
			edges[i-1] = NodeEdge{
				From: string(rune('A' + i - 1)),
				To:   string(rune('A' + i)),
			}
		}
	}

	g := WorkflowGraph{
		ID:    "wf_deep",
		Nodes: nodes,
		Edges: edges,
	}

	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("deep chain: %v", err)
	}
}
