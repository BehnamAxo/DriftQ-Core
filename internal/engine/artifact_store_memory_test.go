package engine

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryArtifactStore_PutGetDelete_RoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := NewMemoryArtifactStore()

	data := []byte(`{"x":2}`)
	meta := ArtifactMeta{
		RunID:        "run1",
		WorkflowID:   "wf_demo",
		NodeID:       "A",
		Attempt:      1,
		ContentType:  "application/json",
		CreatedAt:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Labels:       map[string]string{"kind": "node_output"},
		Description:  "demo node A output",
		OriginalName: "A.output.json",
	}

	ref, gotMeta, err := s.Put(ctx, data, meta)
	if err != nil {
		t.Fatalf("Put error: %v", err)
	}

	if ref.ArtifactID == "" {
		t.Fatalf("expected non-empty ArtifactID")
	}

	if gotMeta.ArtifactID != ref.ArtifactID {
		t.Fatalf("meta artifact_id mismatch: got=%q want=%q", gotMeta.ArtifactID, ref.ArtifactID)
	}

	if gotMeta.Size != int64(len(data)) {
		t.Fatalf("meta size mismatch: got=%d want=%d", gotMeta.Size, len(data))
	}

	if gotMeta.Sha256 == "" {
		t.Fatalf("expected non-empty sha256")
	}

	if gotMeta.RunID != "run1" || gotMeta.NodeID != "A" || gotMeta.Attempt != 1 {
		t.Fatalf("meta fields not preserved: %+v", gotMeta)
	}

	gotBytes, gotMeta2, err := s.Get(ctx, ref.ArtifactID)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}

	if !bytes.Equal(gotBytes, data) {
		t.Fatalf("Get bytes mismatch: got=%q want=%q", string(gotBytes), string(data))
	}

	if gotMeta2.ArtifactID != ref.ArtifactID {
		t.Fatalf("Get meta artifact_id mismatch: got=%q want=%q", gotMeta2.ArtifactID, ref.ArtifactID)
	}

	if err := s.Delete(ctx, ref.ArtifactID); err != nil {
		t.Fatalf("Delete error: %v", err)
	}

	_, _, err = s.Get(ctx, ref.ArtifactID)
	if !errors.Is(err, ErrArtifactNotFound) {
		t.Fatalf("expected ErrArtifactNotFound after delete, got: %v", err)
	}
}

func TestMemoryArtifactStore_ListByRun_FiltersRun(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := NewMemoryArtifactStore()

	put := func(runID, nodeID string, created time.Time) string {
		ref, _, err := s.Put(ctx, []byte(`{"ok":true}`), ArtifactMeta{
			RunID:        runID,
			WorkflowID:   "wf_demo",
			NodeID:       nodeID,
			Attempt:      1,
			ContentType:  "application/json",
			CreatedAt:    created,
			Labels:       map[string]string{"kind": "node_output"},
			Description:  "test",
			OriginalName: nodeID + ".output.json",
		})

		if err != nil {
			t.Fatalf("Put error: %v", err)
		}
		return ref.ArtifactID
	}

	idA := put("run1", "A", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	idB := put("run1", "B", time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC))
	_ = put("run2", "A", time.Date(2026, 1, 1, 0, 0, 2, 0, time.UTC))

	metas, err := s.ListByRun(ctx, "run1", 50)
	if err != nil {
		t.Fatalf("ListByRun error: %v", err)
	}

	if len(metas) != 2 {
		t.Fatalf("expected 2 artifacts for run1, got %d (%+v)", len(metas), metas)
	}

	got := map[string]ArtifactMeta{}
	for _, m := range metas {
		if m.RunID != "run1" {
			t.Fatalf("expected RunID=run1, got %+v", m)
		}

		got[m.ArtifactID] = m
	}

	if _, ok := got[idA]; !ok {
		t.Fatalf("missing artifact idA=%s", idA)
	}

	if _, ok := got[idB]; !ok {
		t.Fatalf("missing artifact idB=%s", idB)
	}
}
