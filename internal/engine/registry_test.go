package engine

import (
	"context"
	"encoding/json"
	"testing"
)

func TestCompileSpecToExecutable(t *testing.T) {
	raw := []byte(`{
	  "id": "wf_demo",
	  "nodes": [
		{"id":"A", "topic":"a"},
		{"id":"B", "topic":"b", "deps":["A"]}
	  ]
	}`)

	g, spec, err := ParseWorkflowSpecJSON(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	reg := NewHandlerRegistry()
	reg.Register("a", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"x":1}`), nil
	})
	reg.Register("b", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"y":2}`), nil
	})

	exec, err := CompileSpecToExecutable(spec, g, reg)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	if exec.Nodes[0].Run == nil || exec.Nodes[1].Run == nil {
		t.Fatal("expected node Run funcs to be populated")
	}
}
