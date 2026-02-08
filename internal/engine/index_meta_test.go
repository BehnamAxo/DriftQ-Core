package engine

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIndexVersion_PromoteRequiresSucceeded(t *testing.T) {
	s := NewMemoryStore()

	r := Run{RunID: "r1", WorkflowID: "wf1", Status: RunStatusFailed}
	if err := s.CreateRun(r); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	if _, err := PromoteIndexVersion(s, "r1", "v1"); err == nil {
		t.Fatalf("expected error promoting failed run")
	}

	r.Status = RunStatusSucceeded
	if err := s.UpdateRun(r); err != nil {
		t.Fatalf("UpdateRun: %v", err)
	}

	ver, err := PromoteIndexVersion(s, "r1", "")
	if err != nil {
		t.Fatalf("promote: %v", err)
	}

	if ver != "r1" {
		t.Fatalf("expected default version=r1, got %q", ver)
	}

	got, ok := GetActiveIndexVersion(s)
	if !ok || got != "r1" {
		t.Fatalf("expected active=r1, got %q ok=%v", got, ok)
	}

	if err := RollbackIndexVersion(s, "v0"); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	got, _ = GetActiveIndexVersion(s)
	if got != "v0" {
		t.Fatalf("expected active=v0, got %q", got)
	}
}

func TestFileStore_KVPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "engine.wal")

	s1, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("OpenFileStore: %v", err)
	}

	if err := s1.PutKV(MetaKeyActiveIndexVersion, "v123"); err != nil {
		t.Fatalf("PutKV: %v", err)
	}

	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and ensure value is replayed.
	s2, err := OpenFileStore(path)
	if err != nil {
		t.Fatalf("OpenFileStore (reopen): %v", err)
	}

	defer func() { _ = s2.Close() }()

	got, ok := s2.GetKV(MetaKeyActiveIndexVersion)
	if !ok || got != "v123" {
		t.Fatalf("expected v123, got %q ok=%v", got, ok)
	}

	// sanity: file exists
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected wal file: %v", err)
	}
}
