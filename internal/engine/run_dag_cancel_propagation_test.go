package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestRunDAG_CancelStopsSchedulingNewNodes(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)

	runID := "cancel-prop-1"

	startedA := make(chan struct{})
	startedB := make(chan struct{})
	startedC := make(chan struct{})

	g := WorkflowGraph{
		ID: "wf_cancel",
		Nodes: []NodeDef{
			{
				NodeID: "A",
				Run: func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
					close(startedA)
					time.Sleep(500 * time.Millisecond) // Made this long enough so we can cancel mid-flight
					return json.RawMessage(`{"a":1}`), nil
				},
			},
			{
				NodeID: "B",
				Run: func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
					close(startedB)
					return json.RawMessage(`{"b":1}`), nil
				},
			},
			{
				NodeID: "C",
				Run: func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
					close(startedC)
					return json.RawMessage(`{"c":1}`), nil
				},
			},
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
			{From: "B", To: "C"},
		},
	}

	initialInput := json.RawMessage(`{"x":1}`)
	spec := json.RawMessage(`{"id":"wf_cancel"}`)

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.runDAG(context.Background(), runID, g, initialInput, spec)
	}()

	// Wait until A is actually running.
	select {
	case <-startedA:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for node A to start")
	}

	// Cancel while A is in-flight
	if err := r.CancelRun(context.Background(), runID, "user requested"); err != nil {
		t.Fatalf("CancelRun failed: %v", err)
	}

	// runDAG should return canceled (not succeeded)
	err := <-errCh
	if !errors.Is(err, ErrRunCanceled) {
		t.Fatalf("expected ErrRunCanceled, got: %v", err)
	}

	// Run status should be canceled
	run, ok := store.GetRun(runID)
	if !ok {
		t.Fatal("run missing from store")
	}

	if run.Status != RunStatusCanceled {
		t.Fatalf("expected run status %q, got %q", RunStatusCanceled, run.Status)
	}

	// B/C must NOT have started
	select {
	case <-startedB:
		t.Fatal("node B started after cancel (should not schedule new work)")
	default:
	}

	select {
	case <-startedC:
		t.Fatal("node C started after cancel (should not schedule new work)")
	default:
	}

	// Stronger: ensure no NodeStarted events for B/C
	evs := store.ListEvents(runID)
	for _, e := range evs {
		if e.Type == EventNodeStarted && (e.NodeID == "B" || e.NodeID == "C") {
			t.Fatalf("found unexpected %s for node %s after cancel", e.Type, e.NodeID)
		}
	}

	// And ensure we have a run_finished event that indicates canceled
	foundCanceledFinish := false
	for _, e := range evs {
		if e.Type != EventRunFinished {
			continue
		}

		if string(e.Payload) != "" && (hasSubstring(string(e.Payload), `"status":"canceled"`) || hasSubstring(string(e.Payload), `"canceled"`)) {
			foundCanceledFinish = true
			break
		}
	}

	if !foundCanceledFinish {
		t.Fatalf("expected a %s event with canceled status payload", EventRunFinished)
	}
}

func hasSubstring(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOfSubstr(s, sub) >= 0)
}

func indexOfSubstr(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}

	return -1
}
