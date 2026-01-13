package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

type nodeFinishedPayloadWire struct {
	Output      json.RawMessage `json:"output,omitempty"`
	ArtifactRef *struct {
		ArtifactID string `json:"artifact_id"`
	} `json:"artifact_ref,omitempty"`
	ArtifactMeta *struct {
		ArtifactID  string    `json:"artifact_id"`
		Sha256      string    `json:"sha256"`
		ContentType string    `json:"content_type,omitempty"`
		Size        int64     `json:"size"`
		CreatedAt   time.Time `json:"created_at"`
	} `json:"artifact_meta,omitempty"`
}

func TestBuildNodeFinishedPayload_InlinesSmallOutput(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)

	// Inline limit large enough so this stays inline.
	r.SetArtifactInlineLimit(1024)

	out := json.RawMessage(`{"ok":true}`)

	b, err := r.buildNodeFinishedPayload(context.Background(), "run-1", "wf-1", "A", 1, out)
	if err != nil {
		t.Fatalf("buildNodeFinishedPayload: %v", err)
	}

	var p nodeFinishedPayloadWire
	if err := json.Unmarshal(b, &p); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	if len(p.Output) == 0 {
		t.Fatalf("expected inline output, got empty output; payload=%s", string(b))
	}

	if string(p.Output) != string(out) {
		t.Fatalf("inline output mismatch: got=%s want=%s", string(p.Output), string(out))
	}

	if p.ArtifactRef != nil || p.ArtifactMeta != nil {
		t.Fatalf("expected no artifact ref/meta for small output; payload=%s", string(b))
	}
}

func TestBuildNodeFinishedPayload_StoresArtifactWhenLargeOutput(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)

	// Force artifact for most outputs
	r.SetArtifactInlineLimit(32)

	as, err := NewLocalArtifactStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalArtifactStore: %v", err)
	}
	r.SetArtifactStore(as)

	// Deterministic large JSON
	bigOut := json.RawMessage(`{"blob":"` + strings.Repeat("A", 5000) + `"}`)

	b, err := r.buildNodeFinishedPayload(context.Background(), "run-2", "wf-2", "A", 1, bigOut)
	if err != nil {
		t.Fatalf("buildNodeFinishedPayload: %v", err)
	}

	var p nodeFinishedPayloadWire
	if err := json.Unmarshal(b, &p); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	if p.ArtifactRef == nil || p.ArtifactRef.ArtifactID == "" {
		t.Fatalf("expected artifact_ref with artifact_id; payload=%s", string(b))
	}

	if p.ArtifactMeta == nil || p.ArtifactMeta.ArtifactID == "" {
		t.Fatalf("expected artifact_meta; payload=%s", string(b))
	}

	if len(p.Output) != 0 {
		t.Fatalf("expected output omitted for artifact payload; got output=%s payload=%s", string(p.Output), string(b))
	}

	sum := sha256.Sum256(bigOut)
	wantID := hex.EncodeToString(sum[:])

	if p.ArtifactRef.ArtifactID != wantID {
		t.Fatalf("artifact_id mismatch: got=%q want=%q", p.ArtifactRef.ArtifactID, wantID)
	}

	if p.ArtifactMeta.ArtifactID != wantID || p.ArtifactMeta.Sha256 != wantID {
		t.Fatalf("artifact meta id mismatch: meta=%+v wantID=%q", p.ArtifactMeta, wantID)
	}

	// Prove bytes are actually stored and retrievable
	got, meta, err := r.GetArtifact(context.Background(), wantID)
	if err != nil {
		t.Fatalf("GetArtifact: %v", err)
	}

	if string(got) != string(bigOut) {
		t.Fatalf("artifact bytes mismatch: got=%dB want=%dB", len(got), len(bigOut))
	}

	if meta.ArtifactID != wantID || meta.Sha256 != wantID {
		t.Fatalf("stored meta mismatch: got=%+v wantID=%q", meta, wantID)
	}
}

func TestRunDAG_LargeOutputCreatesArtifact(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)

	r.SetArtifactInlineLimit(32)

	as, err := NewLocalArtifactStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalArtifactStore: %v", err)
	}
	r.SetArtifactStore(as)

	// Deterministic large output
	bigOut := json.RawMessage(`{"blob":"` + strings.Repeat("B", 6000) + `"}`)

	g := WorkflowGraph{
		ID: "wf_artifact_rundag",
		Nodes: []NodeDef{
			{
				NodeID: "A",
				Run: func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
					return bigOut, nil
				},
			},
		},
	}

	runID := "artifact-rundag-1"
	if err := r.runDAG(context.Background(), runID, g, json.RawMessage(`{"x":1}`), nil); err != nil {
		t.Fatalf("runDAG: %v", err)
	}

	run, ok := store.GetRun(runID)
	if !ok {
		t.Fatalf("run not found: %s", runID)
	}
	if run.Status != RunStatusSucceeded {
		t.Fatalf("expected run succeeded, got %q", run.Status)
	}

	// Check artifact exists by its content addressed id
	sum := sha256.Sum256(bigOut)
	artifactID := hex.EncodeToString(sum[:])

	got, meta, err := r.GetArtifact(context.Background(), artifactID)
	if err != nil {
		t.Fatalf("GetArtifact: %v", err)
	}

	if string(got) != string(bigOut) {
		t.Fatalf("artifact bytes mismatch: got=%dB want=%dB", len(got), len(bigOut))
	}

	if meta.ArtifactID != artifactID || meta.Sha256 != artifactID {
		t.Fatalf("artifact meta mismatch: got=%+v wantID=%q", meta, artifactID)
	}
}
