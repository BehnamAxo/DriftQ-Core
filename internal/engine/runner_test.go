package engine

import (
	"context"
	"encoding/json"
	"testing"
)

func TestRunner_2NodeWorkflow_SucceedsAndInspectable(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	// Node A: takes {"x":1} -> {"x":2}
	nodeA := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		var m map[string]int
		if err := json.Unmarshal(input, &m); err != nil {
			return nil, err
		}
		m["x"] = m["x"] + 1
		return json.Marshal(m)
	}

	// Node B: takes {"x":2} -> {"x":4}
	nodeB := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		var m map[string]int
		if err := json.Unmarshal(input, &m); err != nil {
			return nil, err
		}
		m["x"] = m["x"] * 2
		return json.Marshal(m)
	}

	wf := Workflow{
		WorkflowID: "wf_math",
		Nodes: []NodeDef{
			{NodeID: "A", Run: nodeA},
			{NodeID: "B", Run: nodeB},
		},
	}

	initial := json.RawMessage(`{"x":1}`)
	if err := runner.RunWorkflow(context.Background(), "run1", wf, initial); err != nil {
		t.Fatalf("RunWorkflow: %v", err)
	}

	// Inspect run state
	run, ok := store.GetRun("run1")
	if !ok {
		t.Fatalf("expected run")
	}

	if run.Status != RunStatusSucceeded {
		t.Fatalf("expected succeeded, got %s", run.Status)
	}

	if run.StartedAt == nil || run.EndedAt == nil {
		t.Fatalf("expected timestamps, got %+v", run)
	}

	// Inspect node executions
	nodes := store.ListNodeExecutions("run1")
	if len(nodes) != 2 {
		t.Fatalf("expected 2 node execs, got %d", len(nodes))
	}

	// Find node B output should be {"x":4}
	var nodeBExec *NodeExecution
	for i := range nodes {
		if nodes[i].NodeID == "B" {
			nodeBExec = &nodes[i]
		}
	}

	if nodeBExec == nil {
		t.Fatalf("expected node B exec")
	}

	var out map[string]int
	if err := json.Unmarshal(nodeBExec.Output, &out); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}

	if out["x"] != 4 {
		t.Fatalf("expected x=4, got %v", out)
	}

	// Inspect events (append-only)
	evs := store.ListEvents("run1")
	if len(evs) < 6 {
		t.Fatalf("expected at least 6 events, got %d", len(evs))
	}

	if evs[0].Seq != 1 {
		t.Fatalf("expected seq starting at 1, got %d", evs[0].Seq)
	}
}
