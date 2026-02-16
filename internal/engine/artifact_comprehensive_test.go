package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Artifact Store Tests
func TestArtifactStore_PutGet(t *testing.T) {
	store := NewMemoryArtifactStore()
	ctx := context.Background()

	data := []byte("test artifact data")
	meta := ArtifactMeta{
		ContentType: "text/plain",
		Size:        int64(len(data)),
	}

	ref, storedMeta, err := store.Put(ctx, data, meta)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	id := storedMeta.ArtifactID
	refStr := strings.TrimSpace(fmt.Sprint(ref))

	if id == "" {
		t.Fatal("expected artifact ID")
	}

	if refStr == "" {
		t.Fatal("expected artifact ref")
	}

	// Get it back
	gotData, gotMeta, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if string(gotData) != string(data) {
		t.Fatalf("data mismatch: got %s", gotData)
	}

	if gotMeta.ContentType != "text/plain" {
		t.Fatalf("meta mismatch: %s", gotMeta.ContentType)
	}

	if gotMeta.Size != int64(len(data)) {
		t.Fatalf("size mismatch: %d vs %d", gotMeta.Size, len(data))
	}
}

func TestArtifactStore_GetNonExistent(t *testing.T) {
	store := NewMemoryArtifactStore()
	ctx := context.Background()

	_, _, err := store.Get(ctx, "nonexistent-id")
	if err == nil {
		t.Fatal("expected error for non-existent artifact")
	}
}

func TestArtifactStore_LargeArtifact(t *testing.T) {
	store := NewMemoryArtifactStore()
	ctx := context.Background()

	// 10MB artifact
	data := make([]byte, 10*1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	meta := ArtifactMeta{
		ContentType: "application/octet-stream",
		Size:        int64(len(data)),
	}

	_, storedMeta, err := store.Put(ctx, data, meta)
	if err != nil {
		t.Fatalf("Put large: %v", err)
	}

	gotData, gotMeta, err := store.Get(ctx, storedMeta.ArtifactID)
	if err != nil {
		t.Fatalf("Get large: %v", err)
	}

	if len(gotData) != len(data) {
		t.Fatalf("size mismatch: %d vs %d", len(gotData), len(data))
	}
	if gotMeta.Size != int64(len(data)) {
		t.Fatalf("meta size mismatch: %d vs %d", gotMeta.Size, len(data))
	}
}

func TestArtifactStore_EmptyData(t *testing.T) {
	store := NewMemoryArtifactStore()
	ctx := context.Background()

	data := []byte{}
	meta := ArtifactMeta{
		ContentType: "text/plain",
		Size:        0,
	}

	_, storedMeta, err := store.Put(ctx, data, meta)
	if err != nil {
		t.Fatalf("Put empty: %v", err)
	}

	gotData, gotMeta, err := store.Get(ctx, storedMeta.ArtifactID)
	if err != nil {
		t.Fatalf("Get empty: %v", err)
	}

	if len(gotData) != 0 {
		t.Fatalf("expected empty, got %d bytes", len(gotData))
	}
	if gotMeta.Size != 0 {
		t.Fatalf("expected meta size 0, got %d", gotMeta.Size)
	}
}

func TestArtifactStore_BinaryData(t *testing.T) {
	store := NewMemoryArtifactStore()
	ctx := context.Background()

	// Binary data with all byte values
	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i)
	}

	meta := ArtifactMeta{
		ContentType: "application/octet-stream",
		Size:        int64(len(data)),
	}

	_, storedMeta, err := store.Put(ctx, data, meta)
	if err != nil {
		t.Fatalf("Put binary: %v", err)
	}

	gotData, gotMeta, err := store.Get(ctx, storedMeta.ArtifactID)
	if err != nil {
		t.Fatalf("Get binary: %v", err)
	}

	if gotMeta.Size != int64(len(data)) {
		t.Fatalf("meta size mismatch: %d vs %d", gotMeta.Size, len(data))
	}

	for i := range data {
		if gotData[i] != data[i] {
			t.Fatalf("byte mismatch at %d: %d vs %d", i, gotData[i], data[i])
		}
	}
}

func TestArtifactStore_JSONData(t *testing.T) {
	store := NewMemoryArtifactStore()
	ctx := context.Background()

	obj := map[string]interface{}{
		"key":    "value",
		"number": 42,
		"nested": map[string]interface{}{"a": 1},
	}
	data, _ := json.Marshal(obj)

	meta := ArtifactMeta{
		ContentType: "application/json",
		Size:        int64(len(data)),
	}

	_, storedMeta, err := store.Put(ctx, data, meta)
	if err != nil {
		t.Fatalf("Put JSON: %v", err)
	}

	gotData, gotMeta, err := store.Get(ctx, storedMeta.ArtifactID)
	if err != nil {
		t.Fatalf("Get JSON: %v", err)
	}

	if gotMeta.ContentType != "application/json" {
		t.Fatalf("content type mismatch: %s", gotMeta.ContentType)
	}

	var gotObj map[string]interface{}
	if err := json.Unmarshal(gotData, &gotObj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if gotObj["key"] != "value" {
		t.Fatalf("data mismatch: %v", gotObj)
	}
}

func TestArtifactStore_MultipleArtifacts(t *testing.T) {
	store := NewMemoryArtifactStore()
	ctx := context.Background()

	ids := make([]string, 10)
	for i := 0; i < 10; i++ {
		data := []byte(strings.Repeat("x", i+1))
		meta := ArtifactMeta{ContentType: "text/plain", Size: int64(len(data))}
		_, storedMeta, err := store.Put(ctx, data, meta)

		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}

		ids[i] = storedMeta.ArtifactID
	}

	// Verify all can be retrieved
	for i, id := range ids {
		data, gotMeta, err := store.Get(ctx, id)
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		if len(data) != i+1 {
			t.Fatalf("size mismatch %d: %d vs %d", i, len(data), i+1)
		}
		if gotMeta.Size != int64(i+1) {
			t.Fatalf("meta size mismatch %d: %d vs %d", i, gotMeta.Size, i+1)
		}
	}
}

// Artifact in Workflow Tests
func TestRunner_ArtifactStoreFallback(t *testing.T) {
	store := NewMemoryStore()
	artifactStore := NewMemoryArtifactStore()
	runner := NewRunner(store, WithArtifactStore(artifactStore))

	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		// Node produces large output that should go to artifact store
		return json.RawMessage(`{"large": "output that could be stored as artifact"}`), nil
	}

	g := WorkflowGraph{
		ID: "wf_artifact",
		Nodes: []NodeDef{
			{NodeID: "A", Run: node},
		},
	}

	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("RunDAG: %v", err)
	}
}

func TestRunner_ArtifactRef_InNodeOutput(t *testing.T) {
	store := NewMemoryStore()
	artifactStore := NewMemoryArtifactStore()
	runner := NewRunner(store, WithArtifactStore(artifactStore))

	// Node that explicitly stores artifact
	node := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		as := ArtifactStoreFrom(ctx)
		if as == nil {
			return json.RawMessage(`{"no_artifact_store": true}`), nil
		}

		data := []byte("large data to store")
		meta := ArtifactMeta{ContentType: "text/plain", Size: int64(len(data))}

		ref, storedMeta, err := as.Put(ctx, data, meta)
		if err != nil {
			return nil, err
		}

		return json.Marshal(map[string]string{
			"artifact_id":  storedMeta.ArtifactID,
			"artifact_ref": strings.TrimSpace(fmt.Sprint(ref)),
		})
	}

	g := WorkflowGraph{
		ID: "wf_artifact_ref",
		Nodes: []NodeDef{
			{NodeID: "A", Run: node},
		},
	}

	err := runner.RunDAG(context.Background(), "run1", g, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("RunDAG: %v", err)
	}

	// Check output has artifact reference
	execs := store.ListNodeExecutions("run1")
	if len(execs) == 0 {
		t.Fatal("no executions")
	}

	var out map[string]string
	_ = json.Unmarshal(execs[0].Output, &out)

	if out["artifact_id"] == "" {
		t.Fatal("expected artifact_id in output")
	}

	// Verify artifact exists
	data, _, err := artifactStore.Get(context.Background(), out["artifact_id"])
	if err != nil {
		t.Fatalf("artifact not found: %v", err)
	}
	if string(data) != "large data to store" {
		t.Fatalf("artifact data mismatch: %s", data)
	}
}

// Local File Artifact Store Tests
func TestLocalArtifactStore_PutGet(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewLocalArtifactStore(tmpDir)
	if err != nil {
		t.Fatalf("NewLocalArtifactStore: %v", err)
	}

	ctx := context.Background()
	data := []byte("local file artifact")
	meta := ArtifactMeta{ContentType: "text/plain", Size: int64(len(data))}

	ref, storedMeta, err := store.Put(ctx, data, meta)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	t.Logf("artifact id=%s ref=%v", storedMeta.ArtifactID, ref)

	gotData, gotMeta, err := store.Get(ctx, storedMeta.ArtifactID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if string(gotData) != "local file artifact" {
		t.Fatalf("data mismatch: %s", gotData)
	}

	if gotMeta.ContentType != "text/plain" {
		t.Fatalf("meta mismatch: %s", gotMeta.ContentType)
	}

	if gotMeta.Size != int64(len(data)) {
		t.Fatalf("meta size mismatch: %d vs %d", gotMeta.Size, len(data))
	}
}

func TestLocalArtifactStore_PersistsAcrossInstances(t *testing.T) {
	tmpDir := t.TempDir()

	// First instance
	store1, err := NewLocalArtifactStore(tmpDir)
	if err != nil {
		t.Fatalf("NewLocalArtifactStore store1: %v", err)
	}

	ctx := context.Background()
	data := []byte("persistent artifact")
	meta := ArtifactMeta{ContentType: "text/plain", Size: int64(len(data))}
	_, storedMeta, err := store1.Put(ctx, data, meta)
	if err != nil {
		t.Fatalf("Put store1: %v", err)
	}

	// Second instance should find it
	store2, err := NewLocalArtifactStore(tmpDir)
	if err != nil {
		t.Fatalf("NewLocalArtifactStore store2: %v", err)
	}

	gotData, _, err := store2.Get(ctx, storedMeta.ArtifactID)
	if err != nil {
		t.Fatalf("Get from new instance: %v", err)
	}

	if string(gotData) != "persistent artifact" {
		t.Fatalf("data not persisted: %s", gotData)
	}
}

// Artifact Metadata Tests
func TestArtifactMeta_ContentTypes(t *testing.T) {
	store := NewMemoryArtifactStore()
	ctx := context.Background()

	contentTypes := []string{
		"text/plain",
		"application/json",
		"application/octet-stream",
		"image/png",
		"audio/mp3",
		"video/mp4",
		"application/pdf",
		"text/html; charset=utf-8",
	}

	for _, ct := range contentTypes {
		meta := ArtifactMeta{ContentType: ct, Size: 1}
		_, storedMeta, err := store.Put(ctx, []byte("x"), meta)
		if err != nil {
			t.Errorf("Put %s: %v", ct, err)
			continue
		}

		_, gotMeta, err := store.Get(ctx, storedMeta.ArtifactID)
		if err != nil {
			t.Errorf("Get %s: %v", ct, err)
			continue
		}

		if gotMeta.ContentType != ct {
			t.Errorf("ContentType mismatch: %s vs %s", gotMeta.ContentType, ct)
		}
	}
}

func TestArtifactMeta_CustomFields(t *testing.T) {
	store := NewMemoryArtifactStore()
	ctx := context.Background()

	meta := ArtifactMeta{
		ContentType:  "application/json",
		Size:         10,
		OriginalName: "data.json",
		RunID:        "run-123",
		NodeID:       "node-A",
		WorkflowID:   "wf-1",
		Description:  "test",
		Labels:       map[string]string{"k": "v"},
		ArtifactID:   "", // store should assign
		Sha256:       "", // store may assign
		CreatedAt:    time.Time{},
		Attempt:      0,
	}

	_, storedMeta, err := store.Put(ctx, []byte(`{"a": 1}`), meta)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	_, gotMeta, err := store.Get(ctx, storedMeta.ArtifactID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if gotMeta.OriginalName != "data.json" {
		t.Errorf("OriginalName not preserved: %s", gotMeta.OriginalName)
	}

	if gotMeta.RunID != "run-123" {
		t.Errorf("RunID not preserved: %s", gotMeta.RunID)
	}

	if gotMeta.NodeID != "node-A" {
		t.Errorf("NodeID not preserved: %s", gotMeta.NodeID)
	}

	if gotMeta.WorkflowID != "wf-1" {
		t.Errorf("WorkflowID not preserved: %s", gotMeta.WorkflowID)
	}
}
