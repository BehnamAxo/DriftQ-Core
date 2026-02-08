package engine

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileStore_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "engine.wal")

	s1, err := OpenFileStore(walPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	t.Cleanup(func() { _ = s1.Close() })

	r := Run{RunID: "r1", WorkflowID: "wf1", Status: RunStatusQueued}
	if err := s1.CreateRun(r); err != nil {
		t.Fatalf("create run: %v", err)
	}

	// Append 2 events and ensure seq is assigned.
	e1, err := s1.AppendEvent(RunEvent{RunID: "r1", Type: EventRunCreated})
	if err != nil {
		t.Fatalf("append event1: %v", err)
	}

	if e1.Seq != 1 {
		t.Fatalf("expected seq=1, got %d", e1.Seq)
	}

	e2, err := s1.AppendEvent(RunEvent{RunID: "r1", Type: EventRunStarted})
	if err != nil {
		t.Fatalf("append event2: %v", err)
	}

	if e2.Seq != 2 {
		t.Fatalf("expected seq=2, got %d", e2.Seq)
	}

	// Upsert a node execution.
	now := time.Now().UTC()
	n := NodeExecution{RunID: "r1", WorkflowID: "wf1", NodeID: "stepA", Attempt: 1, Status: NodeStatusRunning, StartedAt: &now}
	if err := s1.UpsertNodeExecution(n); err != nil {
		t.Fatalf("upsert node: %v", err)
	}

	// Upsert a timer.
	tmr := Timer{RunID: "r1", WorkflowID: "wf1", NodeID: "stepA", Attempt: 1, Status: TimerScheduled, FireAt: now.Add(50 * time.Millisecond), CreatedAt: now, Reason: "test"}
	if err := s1.UpsertTimer(tmr); err != nil {
		t.Fatalf("upsert timer: %v", err)
	}

	// Close first store.
	if err := s1.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	// Sanity: WAL file exists and is non-empty.
	st, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}

	if st.Size() == 0 {
		t.Fatalf("expected non-empty wal")
	}

	// Reopen.
	s2, err := OpenFileStore(walPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}

	defer func() { _ = s2.Close() }()

	gotRun, ok := s2.GetRun("r1")
	if !ok {
		t.Fatalf("expected run r1")
	}

	if gotRun.WorkflowID != "wf1" || gotRun.Status != RunStatusQueued {
		t.Fatalf("unexpected run: %+v", gotRun)
	}

	evs := s2.ListEvents("r1")
	if len(evs) != 2 {
		t.Fatalf("expected 2 events, got %d", len(evs))
	}
	if evs[0].Seq != 1 || evs[1].Seq != 2 {
		t.Fatalf("unexpected seqs: %d,%d", evs[0].Seq, evs[1].Seq)
	}

	gotNode, ok := s2.GetNodeExecution("r1", "stepA", 1)
	if !ok {
		t.Fatalf("expected node execution")
	}

	if gotNode.Status != NodeStatusRunning {
		t.Fatalf("unexpected node status: %s", gotNode.Status)
	}

	gotTimer, ok := s2.GetTimer("r1", "stepA", 1)
	if !ok {
		t.Fatalf("expected timer")
	}

	if gotTimer.Status != TimerScheduled {
		t.Fatalf("unexpected timer status: %s", gotTimer.Status)
	}

	// Ensure seq continues after reopen.
	e3, err := s2.AppendEvent(RunEvent{RunID: "r1", Type: EventNodeStarted, NodeID: "stepA", Attempt: 1})
	if err != nil {
		t.Fatalf("append event3: %v", err)
	}

	if e3.Seq != 3 {
		t.Fatalf("expected seq=3, got %d", e3.Seq)
	}
}
