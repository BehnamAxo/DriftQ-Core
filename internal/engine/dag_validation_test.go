package engine

import (
	"context"
	"encoding/json"
	"testing"
)

// DAG Validation Tests
func TestDAG_Validate_CycleDetected(t *testing.T) {
	g := WorkflowGraph{
		ID: "wf_cycle",
		Nodes: []NodeDef{
			{NodeID: "A"},
			{NodeID: "B"},
			{NodeID: "C"},
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
			{From: "B", To: "C"},
			{From: "C", To: "A"}, // Cycle!
		},
	}

	err := g.Validate()
	if err == nil {
		t.Fatal("expected cycle detection error")
	}
}

func TestDAG_Validate_SelfLoop(t *testing.T) {
	g := WorkflowGraph{
		ID: "wf_selfloop",
		Nodes: []NodeDef{
			{NodeID: "A"},
		},
		Edges: []NodeEdge{
			{From: "A", To: "A"}, // Self-loop
		},
	}

	err := g.Validate()
	if err == nil {
		t.Fatal("expected self-loop detection error")
	}
}

func TestDAG_Validate_NonExistentDependency(t *testing.T) {
	g := WorkflowGraph{
		ID: "wf_bad_dep",
		Nodes: []NodeDef{
			{NodeID: "A"},
		},
		Edges: []NodeEdge{
			{From: "nonexistent", To: "A"},
		},
	}

	err := g.Validate()
	if err == nil {
		t.Fatal("expected non-existent dependency error")
	}
}

func TestDAG_Validate_NonExistentTarget(t *testing.T) {
	g := WorkflowGraph{
		ID: "wf_bad_target",
		Nodes: []NodeDef{
			{NodeID: "A"},
		},
		Edges: []NodeEdge{
			{From: "A", To: "nonexistent"},
		},
	}

	err := g.Validate()
	if err == nil {
		t.Fatal("expected non-existent target error")
	}
}

func TestDAG_Validate_EmptyNodes(t *testing.T) {
	g := WorkflowGraph{
		ID:    "wf_empty",
		Nodes: []NodeDef{},
		Edges: []NodeEdge{},
	}

	// Empty DAG should be valid
	err := g.Validate()
	if err != nil {
		t.Fatalf("empty DAG should be valid: %v", err)
	}
}

func TestDAG_Validate_DuplicateNodeIDs(t *testing.T) {
	g := WorkflowGraph{
		ID: "wf_dup",
		Nodes: []NodeDef{
			{NodeID: "A"},
			{NodeID: "A"}, // Duplicate
		},
	}

	err := g.Validate()
	if err == nil {
		t.Fatal("expected duplicate node ID error")
	}
}

func TestDAG_Validate_DuplicateEdges(t *testing.T) {
	g := WorkflowGraph{
		ID: "wf_dup_edge",
		Nodes: []NodeDef{
			{NodeID: "A"},
			{NodeID: "B"},
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
			{From: "A", To: "B"}, // Duplicate
		},
	}

	// Duplicate edges may or may not be an error
	err := g.Validate()
	_ = err // Just ensure no panic
}

func TestDAG_Validate_ValidComplex(t *testing.T) {
	//    A
	//   / \
	//  B   C
	//   \ /
	//    D
	//    |
	//    E
	g := WorkflowGraph{
		ID: "wf_complex",
		Nodes: []NodeDef{
			{NodeID: "A"},
			{NodeID: "B"},
			{NodeID: "C"},
			{NodeID: "D"},
			{NodeID: "E"},
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
			{From: "A", To: "C"},
			{From: "B", To: "D"},
			{From: "C", To: "D"},
			{From: "D", To: "E"},
		},
	}

	err := g.Validate()
	if err != nil {
		t.Fatalf("valid DAG rejected: %v", err)
	}
}

// Replay Tests
func TestReplay_TimeTravel_ReusesCachedOutput(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	callCount := 0
	expensiveNode := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		callCount++
		return json.RawMessage(`{"computed": true}`), nil
	}

	g := WorkflowGraph{
		ID: "wf_replay",
		Nodes: []NodeDef{
			{NodeID: "expensive", Run: expensiveNode},
		},
	}

	// First run
	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("first run: %v", err)
	}

	if callCount != 1 {
		t.Fatalf("expected 1 call, got %d", callCount)
	}

	// Cache the graph for replay
	runner.rememberGraph("wf_replay", g)

	// Time-travel replay should reuse output
	err = runner.Replay(context.Background(), "run1", ReplayTimeTravel)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	// Should not have called the node again
	if callCount != 1 {
		t.Fatalf("expected still 1 call (cached), got %d", callCount)
	}
}

func TestReplay_Live_ReExecutes(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	callCount := 0
	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		callCount++
		return json.RawMessage(`{}`), nil
	}

	g := WorkflowGraph{
		ID: "wf_live_replay",
		Nodes: []NodeDef{
			{NodeID: "A", Run: node},
		},
	}

	// First run
	runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	if callCount != 1 {
		t.Fatalf("expected 1 call, got %d", callCount)
	}

	runner.rememberGraph("wf_live_replay", g)

	// Live replay should re-execute
	err := runner.Replay(context.Background(), "run1", ReplayLive)
	if err != nil {
		t.Fatalf("live replay: %v", err)
	}

	if callCount != 2 {
		t.Fatalf("expected 2 calls (re-executed), got %d", callCount)
	}
}

func TestReplay_NonExistentRun(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	err := runner.Replay(context.Background(), "nonexistent", ReplayTimeTravel)
	if err == nil {
		t.Fatal("expected error for non-existent run")
	}
}

func TestReplay_InvalidMode(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	}

	g := WorkflowGraph{
		ID: "wf_mode",
		Nodes: []NodeDef{
			{NodeID: "A", Run: node},
		},
	}

	runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	runner.rememberGraph("wf_mode", g)

	err := runner.Replay(context.Background(), "run1", ReplayMode("invalid"))
	if err == nil {
		t.Fatal("expected error for invalid mode")
	}
}

func TestReplayFrom_MiddleNode(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	var aCalls, bCalls, cCalls int

	nodeA := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		aCalls++
		return json.RawMessage(`{"a": 1}`), nil
	}

	nodeB := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		bCalls++
		return json.RawMessage(`{"b": 2}`), nil
	}

	nodeC := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		cCalls++
		return json.RawMessage(`{"c": 3}`), nil
	}

	g := WorkflowGraph{
		ID: "wf_from",
		Nodes: []NodeDef{
			{NodeID: "A", Run: nodeA},
			{NodeID: "B", Run: nodeB},
			{NodeID: "C", Run: nodeC},
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
			{From: "B", To: "C"},
		},
	}

	// First run
	runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))

	if aCalls != 1 || bCalls != 1 || cCalls != 1 {
		t.Fatalf("first run: A=%d B=%d C=%d", aCalls, bCalls, cCalls)
	}

	runner.rememberGraph("wf_from", g)

	// Replay from B with time-travel
	err := runner.ReplayFrom(context.Background(), "run1", "B", ReplayTimeTravel)
	if err != nil {
		t.Fatalf("ReplayFrom: %v", err)
	}

	// A should be cached, B and C re-run
	if aCalls != 1 {
		t.Fatalf("A should be cached, got %d calls", aCalls)
	}
}

func TestReplayFrom_NonExistentStep(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	}

	g := WorkflowGraph{
		ID: "wf_bad_step",
		Nodes: []NodeDef{
			{NodeID: "A", Run: node},
		},
	}

	runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	runner.rememberGraph("wf_bad_step", g)

	err := runner.ReplayFrom(context.Background(), "run1", "nonexistent", ReplayTimeTravel)
	if err == nil {
		t.Fatal("expected error for non-existent step")
	}
}

// Store Tests
func TestMemoryStore_CRUD(t *testing.T) {
	store := NewMemoryStore()

	// Create
	run := Run{
		RunID:      "run1",
		WorkflowID: "wf1",
		Status:     RunStatusQueued,
	}

	if err := store.CreateRun(run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	// Read
	got, ok := store.GetRun("run1")
	if !ok {
		t.Fatal("run not found")
	}

	if got.WorkflowID != "wf1" {
		t.Fatalf("wrong workflow: %s", got.WorkflowID)
	}

	// Update
	run.Status = RunStatusRunning
	if err := store.UpdateRun(run); err != nil {
		t.Fatalf("UpdateRun: %v", err)
	}

	got, _ = store.GetRun("run1")
	if got.Status != RunStatusRunning {
		t.Fatalf("update failed: %s", got.Status)
	}

	// List
	runs := store.ListRuns()
	if len(runs) != 1 {
		t.Fatalf("expected 1 run, got %d", len(runs))
	}
}

func TestMemoryStore_NodeExecutionCRUD(t *testing.T) {
	store := NewMemoryStore()

	run := Run{RunID: "run1", WorkflowID: "wf1", Status: RunStatusRunning}
	store.CreateRun(run)

	// Upsert
	ne := NodeExecution{
		RunID:      "run1",
		WorkflowID: "wf1",
		NodeID:     "A",
		Attempt:    1,
		Status:     NodeStatusRunning,
	}

	if err := store.UpsertNodeExecution(ne); err != nil {
		t.Fatalf("UpsertNodeExecution: %v", err)
	}

	// Get
	got, ok := store.GetNodeExecution("run1", "A", 1)
	if !ok {
		t.Fatal("node execution not found")
	}

	if got.Status != NodeStatusRunning {
		t.Fatalf("wrong status: %s", got.Status)
	}

	// List
	all := store.ListNodeExecutions("run1")
	if len(all) != 1 {
		t.Fatalf("expected 1, got %d", len(all))
	}
}

func TestMemoryStore_Events(t *testing.T) {
	store := NewMemoryStore()

	run := Run{RunID: "run1", WorkflowID: "wf1", Status: RunStatusRunning}
	store.CreateRun(run)

	// Append events
	e1, err := store.AppendEvent(RunEvent{RunID: "run1", Type: EventRunStarted})
	if err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	if e1.Seq != 1 {
		t.Fatalf("expected seq=1, got %d", e1.Seq)
	}

	e2, _ := store.AppendEvent(RunEvent{RunID: "run1", Type: EventNodeStarted})
	if e2.Seq != 2 {
		t.Fatalf("expected seq=2, got %d", e2.Seq)
	}

	// List
	events := store.ListEvents("run1")
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

func TestMemoryStore_Timers(t *testing.T) {
	store := NewMemoryStore()

	run := Run{RunID: "run1", WorkflowID: "wf1", Status: RunStatusWaiting}
	store.CreateRun(run)

	// Upsert timer
	timer := Timer{
		RunID:   "run1",
		NodeID:  "A",
		Attempt: 1,
		Status:  TimerScheduled,
	}

	if err := store.UpsertTimer(timer); err != nil {
		t.Fatalf("UpsertTimer: %v", err)
	}

	// Get
	got, ok := store.GetTimer("run1", "A", 1)
	if !ok {
		t.Fatal("timer not found")
	}

	if got.Status != TimerScheduled {
		t.Fatalf("wrong status: %s", got.Status)
	}

	// List
	timers := store.ListTimers("run1")
	if len(timers) != 1 {
		t.Fatalf("expected 1 timer, got %d", len(timers))
	}
}
