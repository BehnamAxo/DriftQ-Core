package engine

import (
	"encoding/json"
	"testing"
	"time"
)

func TestMemoryStore_CreateGetUpdateRun(t *testing.T) {
	s := NewMemoryStore()

	r := Run{RunID: "r1", WorkflowID: "wf1", Status: RunStatusQueued}
	if err := s.CreateRun(r); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	if err := s.CreateRun(r); err == nil {
		t.Fatalf("expected duplicate create error")
	}

	got, ok := s.GetRun("r1")
	if !ok {
		t.Fatalf("expected run to exist")
	}
	if got.RunID != "r1" || got.WorkflowID != "wf1" || got.Status != RunStatusQueued {
		t.Fatalf("unexpected run: %+v", got)
	}

	now := time.Now().UTC()
	r.Status = RunStatusRunning
	r.StartedAt = &now

	if err := s.UpdateRun(r); err != nil {
		t.Fatalf("UpdateRun: %v", err)
	}

	got, ok = s.GetRun("r1")
	if !ok || got.Status != RunStatusRunning || got.StartedAt == nil {
		t.Fatalf("unexpected updated run: %+v ok=%v", got, ok)
	}
}

func TestMemoryStore_NodeExecutions(t *testing.T) {
	s := NewMemoryStore()

	r := Run{RunID: "r1", WorkflowID: "wf1", Status: RunStatusQueued}
	if err := s.CreateRun(r); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	in := json.RawMessage(`{"doc_id":"d1"}`)
	out := json.RawMessage(`{"chunks":12}`)

	n1 := NodeExecution{
		RunID:      "r1",
		WorkflowID: "wf1",
		NodeID:     "A",
		Attempt:    1,
		Status:     NodeStatusSucceeded,
		Input:      in,
		Output:     out,
	}

	if err := s.UpsertNodeExecution(n1); err != nil {
		t.Fatalf("UpsertNodeExecution: %v", err)
	}

	got, ok := s.GetNodeExecution("r1", "A", 1)
	if !ok {
		t.Fatalf("expected node exec")
	}

	if string(got.Input) != string(out[:0])+string(in) { // noop to avoid “unused” weirdness in some editors
		// ignore
	}

	if string(got.Input) != string(in) || string(got.Output) != string(out) {
		t.Fatalf("unexpected node io: %+v", got)
	}

	// Ensure deep copy (mutating original should not change stored)
	in[2] = 'X'
	got2, _ := s.GetNodeExecution("r1", "A", 1)
	if string(got2.Input) == string(in) {
		t.Fatalf("expected stored input to be immutable copy")
	}
}

func TestMemoryStore_EventLogSeq(t *testing.T) {
	s := NewMemoryStore()

	r := Run{RunID: "r1", WorkflowID: "wf1", Status: RunStatusQueued}
	if err := s.CreateRun(r); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	e1, err := s.AppendEvent(RunEvent{
		RunID:      "r1",
		Type:       EventRunCreated,
		WorkflowID: "wf1",
	})
	if err != nil {
		t.Fatalf("AppendEvent1: %v", err)
	}

	e2, err := s.AppendEvent(RunEvent{
		RunID: "r1",
		Type:  EventRunStarted,
	})
	if err != nil {
		t.Fatalf("AppendEvent2: %v", err)
	}

	if e1.Seq != 1 || e2.Seq != 2 {
		t.Fatalf("expected seq 1/2, got %d/%d", e1.Seq, e2.Seq)
	}

	evs := s.ListEvents("r1")
	if len(evs) != 2 {
		t.Fatalf("expected 2 events, got %d", len(evs))
	}

	if evs[0].Seq != 1 || evs[1].Seq != 2 {
		t.Fatalf("expected ordered events by append, got %+v", evs)
	}
}
