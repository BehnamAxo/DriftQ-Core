package engine

import (
	"context"
	"testing"
	"time"
)

func TestLocalArtifactStore_PruneOlderThan(t *testing.T) {
	root := t.TempDir()
	s, err := NewLocalArtifactStore(root)
	if err != nil {
		t.Fatalf("NewLocalArtifactStore: %v", err)
	}

	now := time.Now().UTC()

	oldData := []byte(`{"old":true}`)
	newData := []byte(`{"new":true}`)

	oldRef, _, err := s.Put(context.Background(), oldData, ArtifactMeta{
		ContentType: "application/json",
		CreatedAt:   now.Add(-24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("Put old: %v", err)
	}

	newRef, _, err := s.Put(context.Background(), newData, ArtifactMeta{
		ContentType: "application/json",
		CreatedAt:   now.Add(-10 * time.Minute),
	})
	if err != nil {
		t.Fatalf("Put new: %v", err)
	}

	cutoff := now.Add(-1 * time.Hour)

	stats, err := s.PruneOlderThan(context.Background(), cutoff)
	if err != nil {
		t.Fatalf("PruneOlderThan: %v", err)
	}

	if stats.Deleted != 1 {
		t.Fatalf("expected Deleted=1, got %+v", stats)
	}

	// Old should be gone
	_, _, err = s.Get(context.Background(), oldRef.ArtifactID)
	if err != ErrArtifactNotFound {
		t.Fatalf("expected ErrArtifactNotFound for old artifact, got %v", err)
	}

	// New should remain
	got, _, err := s.Get(context.Background(), newRef.ArtifactID)
	if err != nil {
		t.Fatalf("Get new: %v", err)
	}
	if string(got) != string(newData) {
		t.Fatalf("new data mismatch: got %q want %q", string(got), string(newData))
	}
}
