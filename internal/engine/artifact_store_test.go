package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"
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

	if meta.ContentType != "application/json" {
		t.Fatalf("content-type mismatch: got %q want %q", meta.ContentType, "application/json")
	}

	if meta.Size != int64(len(data)) {
		t.Fatalf("meta size mismatch: got %d want %d", meta.Size, len(data))
	}

	if meta.CreatedAt.IsZero() {
		t.Fatalf("expected CreatedAt to be set")
	}

	// sanity: CreatedAt should be near "now"
	if time.Since(meta.CreatedAt) > 5*time.Second {
		t.Fatalf("CreatedAt seems too old: %v", meta.CreatedAt)
	}

	gotData, gotMeta, err := s.Get(context.Background(), ref.ArtifactID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if string(gotData) != string(data) {
		t.Fatalf("data mismatch: got %q want %q", string(gotData), string(data))
	}

	if gotMeta.ArtifactID != wantID || gotMeta.Sha256 != wantID {
		t.Fatalf("got meta id mismatch: got=%+v wantID=%q", gotMeta, wantID)
	}

	// Putting same bytes again should return same artifact id
	ref2, meta2, err := s.Put(context.Background(), data, ArtifactMeta{ContentType: "application/json"})
	if err != nil {
		t.Fatalf("Put again: %v", err)
	}

	if ref2.ArtifactID != wantID {
		t.Fatalf("expected stable id on same content: got %q want %q", ref2.ArtifactID, wantID)
	}

	// meta should still be coherent on second put
	if meta2.ArtifactID != wantID || meta2.Sha256 != wantID {
		t.Fatalf("meta2 id mismatch: meta2=%+v wantID=%q", meta2, wantID)
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

func TestLocalArtifactStore_NotFound(t *testing.T) {
	root := t.TempDir()
	s, err := NewLocalArtifactStore(root)
	if err != nil {
		t.Fatalf("NewLocalArtifactStore: %v", err)
	}

	// 64 hex chars but does NOT exist
	missing := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	_, _, err = s.Get(context.Background(), missing)
	if err != ErrArtifactNotFound {
		t.Fatalf("expected ErrArtifactNotFound, got %v", err)
	}
}
