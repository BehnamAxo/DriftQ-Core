package engine

import (
	"context"
	"encoding/json"
	"testing"
)

func TestBuildNodeFinishedPayload_InlinesUnderLimit(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)

	r.SetArtifactInlineLimit(1024) // big enough to inline

	out := json.RawMessage(`{"x":1}`)
	p, err := r.buildNodeFinishedPayload(context.Background(), "run-1", "wf-1", "A", 1, out)
	if err != nil {
		t.Fatalf("buildNodeFinishedPayload: %v", err)
	}

	var got NodeFinishedPayload
	if err := json.Unmarshal(p, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	if string(got.Output) != string(out) {
		t.Fatalf("expected inline output; got=%s want=%s", string(got.Output), string(out))
	}

	if got.ArtifactRef != nil || got.ArtifactMeta != nil {
		t.Fatalf("expected no artifact fields, got ref=%v meta=%v", got.ArtifactRef, got.ArtifactMeta)
	}
}

func TestBuildNodeFinishedPayload_UsesArtifactOverLimit(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)

	as, err := NewLocalArtifactStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalArtifactStore: %v", err)
	}

	r.SetArtifactStore(as)
	r.SetArtifactInlineLimit(10) // force artifact for anything > 10 bytes

	out := json.RawMessage(`{"x":1,"big":"this should not be inlined"}`)

	p, err := r.buildNodeFinishedPayload(context.Background(), "run-1", "wf-1", "A", 1, out)
	if err != nil {
		t.Fatalf("buildNodeFinishedPayload: %v", err)
	}

	var got NodeFinishedPayload
	if err := json.Unmarshal(p, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	if len(got.Output) != 0 {
		t.Fatalf("expected Output to be empty when artifact-backed, got=%s", string(got.Output))
	}

	if got.ArtifactRef == nil || got.ArtifactRef.ArtifactID == "" {
		t.Fatalf("expected ArtifactRef, got=%v", got.ArtifactRef)
	}

	if got.ArtifactMeta == nil {
		t.Fatalf("expected ArtifactMeta, got nil")
	}

	blob, meta, err := r.GetArtifact(context.Background(), got.ArtifactRef.ArtifactID)
	if err != nil {
		t.Fatalf("GetArtifact: %v", err)
	}

	if string(blob) != string(out) {
		t.Fatalf("artifact blob mismatch: got=%s want=%s", string(blob), string(out))
	}

	if meta.ArtifactID != got.ArtifactRef.ArtifactID {
		t.Fatalf("meta id mismatch: got=%s want=%s", meta.ArtifactID, got.ArtifactRef.ArtifactID)
	}
}
