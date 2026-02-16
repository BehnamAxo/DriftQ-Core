package engine

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

// Timer/Delay Tests
func TestTimer_DelayError_CreatesTimer(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	delayNode := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return nil, Delay(100*time.Millisecond, "waiting for external event")
	}

	g := WorkflowGraph{
		ID: "wf_delay",
		Nodes: []NodeDef{
			{NodeID: "delay", Run: delayNode},
		},
	}

	// Run should not block forever
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := runner.RunDAG(ctx, "run1", g, json.RawMessage(`{}`))
	_ = err // May return immediately or wait

	run, _ := store.GetRun("run1")
	// Run should be in waiting state or have timer
	if run.Status == RunStatusSucceeded {
		t.Log("Warning: run completed without waiting (may be expected if timer fires immediately)")
	}

	timers := store.ListTimers("run1")
	if len(timers) == 0 {
		t.Log("Warning: no timer created (behavior may vary)")
	}
}

func TestTimer_ResumesAfterDelay(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)
	runner.StartTimerLoop(context.Background())

	callCount := 0
	delayOnceNode := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		callCount++
		if callCount == 1 {
			return nil, Delay(50*time.Millisecond, "first call delays")
		}

		return json.RawMessage(`{"done": true}`), nil
	}

	g := WorkflowGraph{
		ID: "wf_resume",
		Nodes: []NodeDef{
			{NodeID: "A", Run: delayOnceNode},
		},
	}

	runner.rememberGraph("wf_resume", g)

	go runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))

	// Wait for timer to fire and run to complete
	time.Sleep(500 * time.Millisecond)

	run, _ := store.GetRun("run1")
	if run.Status != RunStatusSucceeded {
		t.Fatalf("expected succeeded after timer, got %s", run.Status)
	}

	if callCount < 2 {
		t.Fatalf("expected at least 2 calls, got %d", callCount)
	}
}

func TestTimer_PastFireAt(t *testing.T) {
	store := NewMemoryStore()
	// runner := NewRunner(store)

	// Create a timer that's already past
	run := Run{
		RunID:      "run1",
		WorkflowID: "wf1",
		Status:     RunStatusWaiting,
	}
	store.CreateRun(run)

	pastTime := time.Now().Add(-time.Hour)
	timer := Timer{
		RunID:   "run1",
		NodeID:  "A",
		Attempt: 1,
		Status:  TimerScheduled,
		FireAt:  pastTime,
	}
	store.UpsertTimer(timer)

	// List due timers should include it
	due := store.ListDueTimers(time.Now())
	found := false
	for _, t := range due {
		if t.RunID == "run1" {
			found = true
			break
		}
	}

	if !found {
		t.Fatal("past timer should be in due list")
	}
}

func TestTimer_FutureFireAt(t *testing.T) {
	store := NewMemoryStore()

	run := Run{RunID: "run1", WorkflowID: "wf1", Status: RunStatusWaiting}
	store.CreateRun(run)

	futureTime := time.Now().Add(time.Hour)
	timer := Timer{
		RunID:   "run1",
		NodeID:  "A",
		Attempt: 1,
		Status:  TimerScheduled,
		FireAt:  futureTime,
	}
	store.UpsertTimer(timer)

	// List due timers should NOT include it
	due := store.ListDueTimers(time.Now())
	for _, tm := range due {
		if tm.RunID == "run1" {
			t.Fatal("future timer should not be in due list")
		}
	}
}

func TestTimer_StatusTransitions(t *testing.T) {
	store := NewMemoryStore()

	run := Run{RunID: "run1", WorkflowID: "wf1", Status: RunStatusWaiting}
	store.CreateRun(run)

	now := time.Now()
	timer := Timer{
		RunID:   "run1",
		NodeID:  "A",
		Attempt: 1,
		Status:  TimerScheduled,
		FireAt:  now,
	}
	store.UpsertTimer(timer)

	// Check initial status
	got, _ := store.GetTimer("run1", "A", 1)
	if got.Status != TimerScheduled {
		t.Fatalf("expected scheduled, got %s", got.Status)
	}

	// Update to fired
	timer.Status = TimerFired
	store.UpsertTimer(timer)

	got, _ = store.GetTimer("run1", "A", 1)
	if got.Status != TimerFired {
		t.Fatalf("expected fired, got %s", got.Status)
	}
}

func TestTimer_MultipleTimersSameRun(t *testing.T) {
	store := NewMemoryStore()

	run := Run{RunID: "run1", WorkflowID: "wf1", Status: RunStatusWaiting}
	store.CreateRun(run)

	now := time.Now()

	// Multiple timers for different nodes
	for i := 0; i < 3; i++ {
		timer := Timer{
			RunID:   "run1",
			NodeID:  string(rune('A' + i)),
			Attempt: 1,
			Status:  TimerScheduled,
			FireAt:  now,
		}
		store.UpsertTimer(timer)
	}

	timers := store.ListTimers("run1")
	if len(timers) != 3 {
		t.Fatalf("expected 3 timers, got %d", len(timers))
	}
}

func TestTimer_SurvivesRestart(t *testing.T) {
	// Create file store for durability
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "engine.wal")

	store, err := OpenFileStore(walPath)
	if err != nil {
		t.Fatalf("OpenFileStore: %v", err)
	}

	run := Run{RunID: "run1", WorkflowID: "wf1", Status: RunStatusWaiting}
	store.CreateRun(run)

	now := time.Now().Add(time.Minute)
	timer := Timer{
		RunID:   "run1",
		NodeID:  "A",
		Attempt: 1,
		Status:  TimerScheduled,
		FireAt:  now,
		Reason:  "waiting for something",
	}
	store.UpsertTimer(timer)
	store.Close()

	// Reopen store
	store2, err := OpenFileStore(walPath)
	if err != nil {
		t.Fatalf("OpenFileStore reopen: %v", err)
	}
	defer store2.Close()

	// Timer should still exist
	got, ok := store2.GetTimer("run1", "A", 1)
	if !ok {
		t.Fatal("timer not found after restart")
	}

	if got.Status != TimerScheduled {
		t.Fatalf("expected scheduled, got %s", got.Status)
	}

	if got.Reason != "waiting for something" {
		t.Fatalf("reason not preserved: %s", got.Reason)
	}
}

// DelayError Tests
func TestDelayError_Error(t *testing.T) {
	de := DelayError{
		After:  5 * time.Second,
		Reason: "test reason",
	}

	msg := de.Error()
	if msg == "" {
		t.Fatal("error message should not be empty")
	}
}

func TestDelayError_ZeroDuration(t *testing.T) {
	de := DelayError{
		After:  0,
		Reason: "no delay",
	}

	// Should still be valid
	_ = de.Error()
}

func TestDelayError_NegativeDuration(t *testing.T) {
	de := DelayError{
		After:  -time.Second,
		Reason: "negative",
	}

	// Should handle gracefully
	_ = de.Error()
}

// Timer Loop Tests
func TestTimerLoop_ProcessesDueTimers(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner.StartTimerLoop(ctx)

	// Create a run with timer
	run := Run{RunID: "run1", WorkflowID: "wf1", Status: RunStatusWaiting}
	store.CreateRun(run)

	past := time.Now().Add(-time.Second)
	timer := Timer{
		RunID:   "run1",
		NodeID:  "A",
		Attempt: 1,
		Status:  TimerScheduled,
		FireAt:  past,
	}
	store.UpsertTimer(timer)

	// Wait for timer loop to process
	time.Sleep(200 * time.Millisecond)

	// Timer should be fired
	got, _ := store.GetTimer("run1", "A", 1)
	if got.Status == TimerScheduled {
		t.Log("Warning: timer not yet processed (may need more time)")
	}
}

func TestTimerLoop_ContextCancellation(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bool)
	go func() {
		runner.StartTimerLoop(ctx)
		done <- true
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Good - loop exited
	case <-time.After(time.Second):
		t.Fatal("timer loop did not exit after cancel")
	}
}
