package engine

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestFireDueTimersAndResume_ResumesWaitingRun(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)
	runner.SetLogger(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))

	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)

	// Handler: attempt 1 delays; attempt 2 succeeds.
	reg.Register("flaky", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		att := AttemptFrom(ctx)
		if att <= 1 {
			return nil, Delay(0, "backoff")
		}

		return json.RawMessage(`{"ok":true}`), nil
	})

	spec := []byte(`{
		"id":"wf_timer_resume",
		"nodes":[
			{"id":"embed_chunks","topic":"flaky","deps":[],"retry":{"max_attempts":3}}
		]
	}`)

	// Create + run once (will go to WAITING because attempt 1 delays)
	ctx := context.Background()
	if err := runner.RunSpecJSON(ctx, "run-timer-resume", spec, reg, json.RawMessage(`{}`)); err != nil {
		t.Fatalf("RunSpecJSON: %v", err)
	}

	run, ok := store.GetRun("run-timer-resume")
	if !ok {
		t.Fatalf("expected run to exist")
	}
	if run.Status != RunStatusWaiting {
		t.Fatalf("expected run status WAITING, got %s", run.Status)
	}

	// Resume via timers (fire any due timers and resume waiting runs)
	now := time.Now().UTC().Add(250 * time.Millisecond)
	fired, resumed, err := runner.FireDueTimersAndResume(ctx, now)
	if err != nil {
		t.Fatalf("FireDueTimersAndResume: %v", err)
	}

	if fired < 1 {
		t.Fatalf("expected fired >= 1, got %d", fired)
	}

	if resumed != 1 {
		t.Fatalf("expected resumed == 1, got %d", resumed)
	}

	run2, _ := store.GetRun("run-timer-resume")
	if run2.Status != RunStatusSucceeded {
		t.Fatalf("expected run status SUCCEEDED after resume, got %s", run2.Status)
	}

	// Sanity: ensure node attempt 2 succeeded
	nodes := store.ListNodeExecutions("run-timer-resume")
	foundOk := false
	for _, ne := range nodes {
		if ne.NodeID == "embed_chunks" && ne.Status == NodeStatusSucceeded {
			foundOk = true
		}
	}

	if !foundOk {
		t.Fatalf("expected a succeeded node execution for embed_chunks")
	}
}
