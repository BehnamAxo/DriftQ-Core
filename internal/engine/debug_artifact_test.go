package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDebugArtifactEndpoints(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	root := t.TempDir()
	as, err := NewLocalArtifactStore(root)
	if err != nil {
		t.Fatalf("NewLocalArtifactStore: %v", err)
	}

	runner.SetArtifactStore(as)
	ref, meta, err := runner.PutArtifact(context.Background(), []byte("hello"), ArtifactMeta{
		ContentType: "text/plain",
		RunID:       "run-1",
		WorkflowID:  "wf-1",
		NodeID:      "A",
		Attempt:     1,
	})

	if err != nil {
		t.Fatalf("PutArtifact: %v", err)
	}

	mux := http.NewServeMux()
	AttachDebugRoutes(mux, runner)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// meta endpoint
	resp, err := http.Get(srv.URL + "/debug/artifact-meta?artifact_id=" + ref.ArtifactID)
	if err != nil {
		t.Fatalf("GET meta: %v", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("meta status: got %d", resp.StatusCode)
	}

	var body struct {
		OK         bool         `json:"ok"`
		ArtifactID string       `json:"artifact_id"`
		Meta       ArtifactMeta `json:"meta"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode meta json: %v", err)
	}

	if !body.OK || body.ArtifactID != ref.ArtifactID {
		t.Fatalf("bad meta response: %+v", body)
	}

	if body.Meta.ArtifactID != meta.ArtifactID || body.Meta.Sha256 != meta.Sha256 {
		t.Fatalf("meta mismatch: got=%+v want=%+v", body.Meta, meta)
	}

	// blob endpoint
	resp2, err := http.Get(srv.URL + "/debug/artifact-get?artifact_id=" + ref.ArtifactID)
	if err != nil {
		t.Fatalf("GET blob: %v", err)
	}

	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("blob status: got %d", resp2.StatusCode)
	}

	if gotCT := resp2.Header.Get("Content-Type"); gotCT != "text/plain" {
		t.Fatalf("content-type: got %q want %q", gotCT, "text/plain")
	}

	buf := make([]byte, 5)
	n, _ := resp2.Body.Read(buf)
	if string(buf[:n]) != "hello" {
		t.Fatalf("blob mismatch: got %q", string(buf[:n]))
	}
}
