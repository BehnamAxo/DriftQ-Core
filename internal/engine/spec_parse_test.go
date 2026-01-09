package engine

import "testing"

func TestParseWorkflowSpecJSON(t *testing.T) {
	raw := []byte(`{
	  "id": "wf_index_build",
	  "nodes": [
		{"id":"A", "topic":"ingest", "deps": [], "retry":{"max_attempts":3}},
		{"id":"B", "topic":"chunk",  "deps": ["A"]},
		{"id":"C", "topic":"embed",  "deps": ["B"]},
		{"id":"D", "topic":"index",  "deps": ["C"]}
	  ]
	}`)

	g, spec, err := ParseWorkflowSpecJSON(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if g.ID != "wf_index_build" {
		t.Fatalf("bad graph id: %s", g.ID)
	}

	if spec.ID != "wf_index_build" {
		t.Fatalf("bad spec id: %s", spec.ID)
	}

	if len(g.Nodes) != 4 {
		t.Fatalf("expected 4 nodes, got %d", len(g.Nodes))
	}

	if len(g.Edges) != 3 {
		t.Fatalf("expected 3 edges, got %d", len(g.Edges))
	}
}
