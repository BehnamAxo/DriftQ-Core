package engine

import (
	"context"
	"encoding/json"
	"testing"
)

func TestRunner_RunSpecJSON(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)
	reg := NewHandlerRegistry()

	reg.Register("a", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"x":1}`), nil
	})

	reg.Register("b", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		// expects {"A": {...}}
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
	})

	spec := []byte(`{
	  "id":"wf_from_spec",
	  "nodes":[
		{"id":"A","topic":"a"},
		{"id":"B","topic":"b","deps":["A"]}
	  ]
	}`)

	if err := r.RunSpecJSON(context.Background(), "run_spec_1", spec, reg, json.RawMessage(`{"x":0}`)); err != nil {
		t.Fatalf("run 1: %v", err)
	}

	if err := r.RunSpecJSON(context.Background(), "run_spec_2", spec, reg, json.RawMessage(`{"x":0}`)); err != nil {
		t.Fatalf("run 2: %v", err)
	}

	nodes1 := store.ListNodeExecutions("run_spec_1")
	nodes2 := store.ListNodeExecutions("run_spec_2")
	if len(nodes1) != 2 || len(nodes2) != 2 {
		t.Fatalf("expected 2 node executions each, got %d and %d", len(nodes1), len(nodes2))
	}
}
