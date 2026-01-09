package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestRunner_RunDAG_FanOutFanIn(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	// A (root): returns {"x":1}
	nodeA := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"x":1}`), nil
	}

	// B depends on A: takes {"A": {"x":1}} -> {"x":2}
	nodeB := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		var deps map[string]json.RawMessage
		if err := json.Unmarshal(input, &deps); err != nil {
			return nil, err
		}

		var a map[string]int
		if err := json.Unmarshal(deps["A"], &a); err != nil {
			return nil, err
		}

		out, _ := json.Marshal(map[string]int{"x": a["x"] + 1})
		return out, nil
	}

	// C depends on A: takes {"A": {"x":1}} -> {"x":11}
	nodeC := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		var deps map[string]json.RawMessage
		if err := json.Unmarshal(input, &deps); err != nil {
			return nil, err
		}

		var a map[string]int
		if err := json.Unmarshal(deps["A"], &a); err != nil {
			return nil, err
		}

		out, _ := json.Marshal(map[string]int{"x": a["x"] + 10})
		return out, nil
	}

	// D depends on B and C: {"B":{"x":2},"C":{"x":11}} -> {"x":13}
	nodeD := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		var deps map[string]json.RawMessage
		if err := json.Unmarshal(input, &deps); err != nil {
			return nil, err
		}

		var b, c map[string]int
		if err := json.Unmarshal(deps["B"], &b); err != nil {
			return nil, err
		}

		if err := json.Unmarshal(deps["C"], &c); err != nil {
			return nil, err
		}

		out, _ := json.Marshal(map[string]int{"x": b["x"] + c["x"]})
		return out, nil
	}

	g := WorkflowGraph{
		ID: "wf_demo_dag",
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

	if err := runner.RunDAG(context.Background(), "run_dag_1", g, json.RawMessage(`{"x":1}`)); err != nil {
		t.Fatalf("RunDAG: %v", err)
	}

	nodes := store.ListNodeExecutions("run_dag_1")
	var d *NodeExecution
	for i := range nodes {
		if nodes[i].NodeID == "D" {
			d = &nodes[i]
			break
		}
	}

	if d == nil {
		t.Fatalf("missing node D execution")
	}

	var out map[string]int
	if err := json.Unmarshal(d.Output, &out); err != nil {
		t.Fatalf("unmarshal D output: %v", err)
	}

	if out["x"] != 13 {
		t.Fatalf("expected x=13, got %v", out)
	}

	evs := store.ListEvents("run_dag_1")
	var started []string
	for _, e := range evs {
		if e.Type == EventNodeStarted {
			started = append(started, e.NodeID)
		}
	}

	got := ""
	for _, s := range started {
		got += s
	}

	if got != "ABCD" {
		t.Fatalf("expected node start order ABCD, got %s", got)
	}
}

func TestRunner_TimeTravelReplay_DoesNotReexecuteSucceeded(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)

	// Track whether A gets called again (it must NOT on time-travel replay)
	var callsA int

	nodeA := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		callsA++
		return json.RawMessage(`{"a":"ok"}`), nil
	}

	// B fails first time, succeeds on replay
	first := true
	nodeB := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		if first {
			first = false
			return nil, errors.New("boom")
		}
		return json.RawMessage(`{"b":"ok"}`), nil
	}

	g := WorkflowGraph{
		ID: "wf_tt",
		Nodes: []NodeDef{
			{NodeID: "A", Run: nodeA},
			{NodeID: "B", Run: nodeB},
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
		},
	}

	runID := "tt_replay_1"

	// First run should fail on B
	err := r.RunDAG(context.Background(), runID, g, json.RawMessage(`{"x":1}`))
	if !errors.Is(err, ErrNodeFailed) {
		t.Fatalf("expected ErrNodeFailed, got %v", err)
	}

	if callsA != 1 {
		t.Fatalf("expected A called once, got %d", callsA)
	}

	// Time-travel replay: should NOT re-execute A
	if err := r.Replay(context.Background(), runID, ReplayTimeTravel); err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	if callsA != 1 {
		t.Fatalf("A was re-executed during time-travel replay; callsA=%d", callsA)
	}

	// Verify attempts: A=1, B=2
	var aAttempts, bAttempts int
	for _, ne := range store.ListNodeExecutions(runID) {
		switch ne.NodeID {
		case "A":
			aAttempts++
		case "B":
			bAttempts++
		}
	}

	if aAttempts != 1 {
		t.Fatalf("expected A attempts=1, got %d", aAttempts)
	}

	if bAttempts != 2 {
		t.Fatalf("expected B attempts=2, got %d", bAttempts)
	}

	// Verify run ended succeeded
	run, ok := store.GetRun(runID)
	if !ok {
		t.Fatalf("expected run to exist")
	}

	if run.Status != RunStatusSucceeded {
		t.Fatalf("expected run status succeeded, got %s", run.Status)
	}
}
