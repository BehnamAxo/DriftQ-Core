package engine

import (
	"encoding/json"
	"testing"
	"time"
)

func TestInjectNodeFinishedProofFields_PreservesOutputAndAddsFields(t *testing.T) {
	out := json.RawMessage(`{"ok":true}`)
	base, err := json.Marshal(NodeFinishedPayload{Output: out})
	if err != nil {
		t.Fatalf("marshal base: %v", err)
	}

	start := time.Unix(10, 0).UTC()
	end := time.Unix(12, 0).UTC()

	gotRaw, err := InjectNodeFinishedProofFields(base, NodeFinishedProofFields{
		UsedCachedOutput: true,
		CachedAttempt:    7,
		QueuedAt:         &start,
		StartedAt:        &start,
		EndedAt:          &end,
		QueueMS:          0,
		WorkerMS:         2000,
	})
	if err != nil {
		t.Fatalf("InjectNodeFinishedProofFields: %v", err)
	}

	var got NodeFinishedPayload
	if err := json.Unmarshal(gotRaw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if string(got.Output) != string(out) {
		t.Fatalf("output changed: got=%s want=%s", string(got.Output), string(out))
	}
	if !got.UsedCachedOutput {
		t.Fatalf("expected used_cached_output=true")
	}
	if got.CachedAttempt != 7 {
		t.Fatalf("cached_attempt: got=%d want=%d", got.CachedAttempt, 7)
	}
	if got.StartedAt == nil || got.EndedAt == nil {
		t.Fatalf("expected started_at and ended_at to be set")
	}
	if got.WorkerMS != 2000 {
		t.Fatalf("worker_ms: got=%d want=%d", got.WorkerMS, 2000)
	}
}
