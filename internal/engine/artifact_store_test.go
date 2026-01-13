package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestLocalArtifactStore_PutGet_RoundTrip(t *testing.T) {
	root := t.TempDir()
	s, err := NewLocalArtifactStore(root)
	if err != nil {
		t.Fatalf("NewLocalArtifactStore: %v", err)
	}

	data := []byte(`{"x":1,"big":"blob"}`)

	ref, meta, err := s.Put(context.Background(), data, ArtifactMeta{
		ContentType: "application/json",
		RunID:       "run-1",
		WorkflowID:  "wf-1",
		NodeID:      "A",
		Attempt:     1,
		Labels:      map[string]string{"kind": "step_output"},
	})

	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	sum := sha256.Sum256(data)
	wantID := hex.EncodeToString(sum[:])

	if ref.ArtifactID != wantID {
		t.Fatalf("artifact id mismatch: got %q want %q", ref.ArtifactID, wantID)
	}

	if meta.ArtifactID != wantID || meta.Sha256 != wantID {
		t.Fatalf("meta id mismatch: meta=%+v wantID=%q", meta, wantID)
	}

	if meta.Size != int64(len(data)) {
		t.Fatalf("meta size mismatch: got %d want %d", meta.Size, len(data))
	}

	gotData, gotMeta, err := s.Get(context.Background(), ref.ArtifactID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if string(gotData) != string(data) {
		t.Fatalf("data mismatch: got %q want %q", string(gotData), string(data))
	}

	if gotMeta.RunID != "run-1" || gotMeta.NodeID != "A" || gotMeta.Attempt != 1 {
		t.Fatalf("meta mismatch: got %+v", gotMeta)
	}

	// Putting same bytes again should return same artifact id
	ref2, _, err := s.Put(context.Background(), data, ArtifactMeta{ContentType: "application/json"})
	if err != nil {
		t.Fatalf("Put again: %v", err)
	}

	if ref2.ArtifactID != wantID {
		t.Fatalf("expected stable id on same content: got %q want %q", ref2.ArtifactID, wantID)
	}
}

func TestLocalArtifactStore_InvalidID(t *testing.T) {
	root := t.TempDir()
	s, err := NewLocalArtifactStore(root)
	if err != nil {
		t.Fatalf("NewLocalArtifactStore: %v", err)
	}

	_, _, err = s.Get(context.Background(), "nope")
	if err != ErrInvalidArtifactID {
		t.Fatalf("expected ErrInvalidArtifactID, got %v", err)
	}
}
