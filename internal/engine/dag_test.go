package engine

import "testing"

func TestWorkflowGraph_TopologicalOrder(t *testing.T) {
	g := WorkflowGraph{
		ID: "wf_demo_dag",
		Nodes: []NodeDef{
			{NodeID: "A"},
			{NodeID: "B"},
			{NodeID: "C"},
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
			{From: "B", To: "C"},
		},
	}

	if err := g.Validate(); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}

	order, err := g.TopologicalOrder()
	if err != nil {
		t.Fatalf("topological sort failed: %v", err)
	}

	got := ""
	for _, n := range order {
		got += n.NodeID
	}

	if got != "ABC" {
		t.Fatalf("unexpected order: %s", got)
	}
}

func TestWorkflowGraph_DetectCycle(t *testing.T) {
	g := WorkflowGraph{
		ID:    "wf_demo_dag",
		Nodes: []NodeDef{{NodeID: "A"}, {NodeID: "B"}},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
			{From: "B", To: "A"},
		},
	}

	if err := g.Validate(); err == nil {
		t.Fatal("expected cycle detection error")
	}
}
