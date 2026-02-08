package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestReplay_TimeTravel_UsesStoredInitialInput(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)

	runID := "run-replay-1"
	wfID := "wf-replay-1"

	// Cached executable graph so Replay doesn't need spec compilation
	g := WorkflowGraph{
		ID: wfID,
		Nodes: []NodeDef{
			{
				NodeID: "A",
				Run: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					// Echo input so we can assert what initial input was used
					return in, nil
				},
			},
		},
	}
	r.rememberGraph(wfID, g)

	initial := json.RawMessage(`{"hello":"world"}`)
	run := Run{
		RunID:        runID,
		WorkflowID:   wfID,
		Status:       RunStatusFailed, // MUST NOT be succeeded or replay will early-exit
		Spec:         json.RawMessage(`{"fake":"spec"}`),
		InitialInput: cloneRaw(initial),
	}

	if err := store.CreateRun(run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	if err := r.Replay(context.Background(), runID, ReplayTimeTravel); err != nil {
		t.Fatalf("Replay(TimeTravel): %v", err)
	}

	// Confirm node executed and used stored InitialInput.
	execs := store.ListNodeExecutions(runID)
	if len(execs) == 0 {
		t.Fatalf("expected node executions, got none")
	}

	ne := findNodeAttempt(t, execs, "A", 1)
	if ne.Status != NodeStatusSucceeded {
		t.Fatalf("expected node A succeeded, got %v", ne.Status)
	}

	if string(ne.Input) != string(initial) {
		t.Fatalf("expected node input %s, got %s", string(initial), string(ne.Input))
	}

	if string(ne.Output) != string(initial) {
		t.Fatalf("expected node output %s, got %s", string(initial), string(ne.Output))
	}

	// Confirm run ended up succeeded.
	gotRun, ok := store.GetRun(runID)
	if !ok {
		t.Fatalf("expected run to exist")
	}

	if gotRun.Status != RunStatusSucceeded {
		t.Fatalf("expected run succeeded, got %v", gotRun.Status)
	}
}

func TestReplay_TimeTravel_FallsBackToRecordedRootInputWhenInitialMissing(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)

	runID := "run-replay-2"
	wfID := "wf-replay-2"

	g := WorkflowGraph{
		ID: wfID,
		Nodes: []NodeDef{
			{
				NodeID: "A",
				Run: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					return in, nil
				},
			},
		},
	}
	r.rememberGraph(wfID, g)

	// Create run with NO InitialInput
	run := Run{
		RunID:      runID,
		WorkflowID: wfID,
		Status:     RunStatusFailed,
		Spec:       json.RawMessage(`{"fake":"spec"}`),
	}

	if err := store.CreateRun(run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	// Seed a previous attempt's Input so initialInputFromRun() has something to use.
	prevInput := json.RawMessage(`{"seed":"from_previous_attempt"}`)
	now := time.Now().UTC()
	if err := store.UpsertNodeExecution(NodeExecution{
		RunID:      runID,
		WorkflowID: wfID,
		NodeID:     "A",
		Attempt:    1,
		Status:     NodeStatusFailed, // not done, so replay will run again
		StartedAt:  &now,
		EndedAt:    &now,
		Input:      cloneRaw(prevInput),
		Error:      "boom",
	}); err != nil {
		t.Fatalf("UpsertNodeExecution(prev): %v", err)
	}

	if err := r.Replay(context.Background(), runID, ReplayTimeTravel); err != nil {
		t.Fatalf("Replay(TimeTravel): %v", err)
	}

	execs := store.ListNodeExecutions(runID)

	// Because attempt 1 existed, replay should run attempt 2
	ne2 := findNodeAttempt(t, execs, "A", 2)
	if ne2.Status != NodeStatusSucceeded {
		t.Fatalf("expected node A attempt2 succeeded, got %v", ne2.Status)
	}

	if string(ne2.Input) != string(prevInput) {
		t.Fatalf("expected node input from fallback %s, got %s", string(prevInput), string(ne2.Input))
	}
}

func TestCompileExecutableFromStoredSpec_NoSpec(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)

	_, err := r.compileExecutableFromStoredSpec("run-x", Run{
		RunID:      "run-x",
		WorkflowID: "wf-x",
		Spec:       nil,
	})

	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	if !strings.Contains(err.Error(), "no cached graph and no stored spec") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCompileExecutableFromStoredSpec_NoRegistry(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)

	_, err := r.compileExecutableFromStoredSpec("run-y", Run{
		RunID:      "run-y",
		WorkflowID: "wf-y",
		Spec:       []byte(`{"some":"spec"}`),
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	if !strings.Contains(err.Error(), "no handler registry") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReplay_Live_FullRerun_OverridesPriorSuccess(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)

	runID := "run-replay-live"
	wfID := "wf-replay-live"

	calls := 0
	g := WorkflowGraph{
		ID: wfID,
		Nodes: []NodeDef{
			{
				NodeID: "A",
				Run: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					calls++
					return in, nil
				},
			},
		},
	}
	r.rememberGraph(wfID, g)

	initial := json.RawMessage(`{"hello":"world"}`)
	if err := store.CreateRun(Run{
		RunID:        runID,
		WorkflowID:   wfID,
		Status:       RunStatusSucceeded, // prove live replay can re-run a succeeded run
		Spec:         json.RawMessage(`{"fake":"spec"}`),
		InitialInput: cloneRaw(initial),
	}); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	// Seed a previous successful node execution attempt so live replay has something to invalidate.
	now := time.Now().UTC()
	if err := store.UpsertNodeExecution(NodeExecution{
		RunID:      runID,
		WorkflowID: wfID,
		NodeID:     "A",
		Attempt:    1,
		Status:     NodeStatusSucceeded,
		StartedAt:  &now,
		EndedAt:    &now,
		Input:      cloneRaw(initial),
	}); err != nil {
		t.Fatalf("UpsertNodeExecution(prev): %v", err)
	}

	if err := r.Replay(context.Background(), runID, ReplayLive); err != nil {
		t.Fatalf("Replay(Live): %v", err)
	}

	if calls != 1 {
		t.Fatalf("expected handler to execute once on live replay, got %d", calls)
	}

	execs := store.ListNodeExecutions(runID)
	_ = findNodeAttempt(t, execs, "A", 2) // should create a new attempt
	run2, ok := store.GetRun(runID)
	if !ok {
		t.Fatalf("GetRun: missing run")
	}
	if run2.Status != RunStatusSucceeded {
		t.Fatalf("expected run to succeed after live replay, got %s", run2.Status)
	}
}


func findNodeAttempt(t *testing.T, execs []NodeExecution, nodeID string, attempt int) NodeExecution {
	t.Helper()
	for _, ne := range execs {
		if ne.NodeID == nodeID && ne.Attempt == attempt {
			return ne
		}
	}

	t.Fatalf("missing node execution: node=%s attempt=%d; execs=%+v", nodeID, attempt, execs)
	return NodeExecution{}
}
