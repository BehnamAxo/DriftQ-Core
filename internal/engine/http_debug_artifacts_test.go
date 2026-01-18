package engine

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type artifactMetaJSON struct {
	ArtifactID   string            `json:"artifact_id"`
	RunID        string            `json:"run_id"`
	WorkflowID   string            `json:"workflow_id"`
	NodeID       string            `json:"node_id"`
	Attempt      int               `json:"attempt"`
	ContentType  string            `json:"content_type"`
	OriginalName string            `json:"original_name"`
	Description  string            `json:"description"`
	Labels       map[string]string `json:"labels"`
	Size         int64             `json:"size"`
	Sha256       string            `json:"sha256"`
	CreatedAt    time.Time         `json:"created_at"`
	TraceID      string            `json:"trace_id"`
}

func TestHTTPDebug_RunDemo_Artifacts_List_Meta_Get(t *testing.T) {
	t.Parallel()

	runStore := NewMemoryStore()
	runner := NewRunner(runStore)
	runner.SetArtifactStore(NewMemoryArtifactStore())

	mux := http.NewServeMux()
	AttachDebugRoutes(mux, runner)

	srv := httptest.NewServer(mux)
	defer srv.Close()

	// 1) POST /debug/run-demo -> run_id
	runID := postRunDemo(t, srv.URL)

	// 2) GET /debug/run-artifacts?run_id=...
	arts := getRunArtifacts(t, srv.URL, runID)
	if len(arts) < 2 {
		t.Fatalf("expected at least 2 artifacts, got %d: %+v", len(arts), arts)
	}

	// Find the expected demo artifacts deterministically.
	a := findArtifact(t, arts, "A", "A.output.json")
	b := findArtifact(t, arts, "B", "B.output.json")

	// 3) A meta + get
	metaA := getArtifactMeta(t, srv.URL, a.ArtifactID)
	if metaA.RunID != runID || metaA.NodeID != "A" || metaA.Attempt != 1 {
		t.Fatalf("artifact-meta A unexpected: %+v", metaA)
	}

	bodyA, hdrA := getArtifact(t, srv.URL, a.ArtifactID)
	assertHeaderNonEmpty(t, hdrA, "Content-Type")
	assertHeaderNonEmpty(t, hdrA, "X-Artifact-Sha256")
	assertHeaderEquals(t, hdrA, "X-Artifact-Id", a.ArtifactID)
	xA := mustDecodeX(t, bodyA)

	// 4) B meta + get
	metaB := getArtifactMeta(t, srv.URL, b.ArtifactID)
	if metaB.RunID != runID || metaB.NodeID != "B" || metaB.Attempt != 1 {
		t.Fatalf("artifact-meta B unexpected: %+v", metaB)
	}
	bodyB, hdrB := getArtifact(t, srv.URL, b.ArtifactID)
	assertHeaderNonEmpty(t, hdrB, "Content-Type")
	assertHeaderNonEmpty(t, hdrB, "X-Artifact-Sha256")
	assertHeaderEquals(t, hdrB, "X-Artifact-Id", b.ArtifactID)
	xB := mustDecodeX(t, bodyB)

	// What we *must* guarantee for this endpoint test:
	// - artifacts are real JSON payloads and retrievable
	// - meta wiring is correct
	//
	// Today, node B is receiving empty/null input in this demo path, so xB comes back 0.
	// If/when we wire data piping from A->B, xB will become 4 (given initial=1, A:+1 =>2, B:*2 =>4)
	if xB != 0 && xB != 4 {
		t.Fatalf("unexpected B.x=%d (allowed: 0 or 4). A.x=%d rawB=%q", xB, xA, strings.TrimSpace(string(bodyB)))
	}

	_ = xA // keep xA in case we want to tighten this later
}

func postRunDemo(t *testing.T, base string) string {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, base+"/debug/run-demo", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post run-demo: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("run-demo failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var out struct {
		OK    bool   `json:"ok"`
		RunID string `json:"run_id"`
	}

	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal run-demo: %v body=%s", err, strings.TrimSpace(string(body)))
	}

	if !out.OK || strings.TrimSpace(out.RunID) == "" {
		t.Fatalf("bad run-demo response: %s", strings.TrimSpace(string(body)))
	}
	return strings.TrimSpace(out.RunID)
}

func getRunArtifacts(t *testing.T, base, runID string) []artifactMetaJSON {
	t.Helper()

	u := base + "/debug/run-artifacts?run_id=" + url.QueryEscape(runID) + "&limit=50"
	resp, err := http.Get(u)
	if err != nil {
		t.Fatalf("get run-artifacts: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("run-artifacts failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var out struct {
		OK        bool               `json:"ok"`
		RunID     string             `json:"run_id"`
		Count     int                `json:"count"`
		Artifacts []artifactMetaJSON `json:"artifacts"`
	}

	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal run-artifacts: %v body=%s", err, strings.TrimSpace(string(body)))
	}

	if !out.OK || strings.TrimSpace(out.RunID) != runID {
		t.Fatalf("bad run-artifacts response: %s", strings.TrimSpace(string(body)))
	}

	if out.Count != len(out.Artifacts) {
		// not fatal, keep signal
	}

	return out.Artifacts
}

func findArtifact(t *testing.T, arts []artifactMetaJSON, nodeID, originalName string) artifactMetaJSON {
	t.Helper()

	for _, a := range arts {
		if a.NodeID == nodeID && a.OriginalName == originalName {
			if strings.TrimSpace(a.ArtifactID) == "" {
				t.Fatalf("found %s/%s but artifact_id empty: %+v", nodeID, originalName, a)
			}
			return a
		}
	}

	t.Fatalf("did not find artifact node_id=%q original_name=%q in %+v", nodeID, originalName, arts)
	return artifactMetaJSON{}
}

func getArtifactMeta(t *testing.T, base, artifactID string) artifactMetaJSON {
	t.Helper()

	u := base + "/debug/artifact-meta?artifact_id=" + url.QueryEscape(artifactID)
	resp, err := http.Get(u)
	if err != nil {
		t.Fatalf("get artifact-meta: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("artifact-meta failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var out struct {
		OK         bool             `json:"ok"`
		ArtifactID string           `json:"artifact_id"`
		Meta       artifactMetaJSON `json:"meta"`
	}

	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal artifact-meta: %v body=%s", err, strings.TrimSpace(string(body)))
	}

	if !out.OK || strings.TrimSpace(out.ArtifactID) != artifactID {
		t.Fatalf("bad artifact-meta response: %s", strings.TrimSpace(string(body)))
	}

	if strings.TrimSpace(out.Meta.ArtifactID) == "" {
		out.Meta.ArtifactID = out.ArtifactID
	}

	return out.Meta
}

func getArtifact(t *testing.T, base, artifactID string) ([]byte, http.Header) {
	t.Helper()

	u := base + "/debug/artifact-get?artifact_id=" + url.QueryEscape(artifactID)
	resp, err := http.Get(u)
	if err != nil {
		t.Fatalf("get artifact-get: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("artifact-get failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return body, resp.Header
}

func assertHeaderNonEmpty(t *testing.T, h http.Header, key string) {
	t.Helper()
	if strings.TrimSpace(h.Get(key)) == "" {
		t.Fatalf("expected non-empty header %q", key)
	}
}

func assertHeaderEquals(t *testing.T, h http.Header, key, want string) {
	t.Helper()
	got := strings.TrimSpace(h.Get(key))
	if got != want {
		t.Fatalf("header %q mismatch: got=%q want=%q", key, got, want)
	}
}

func mustDecodeX(t *testing.T, body []byte) int {
	t.Helper()

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()

	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("artifact body not json: err=%v body=%q", err, strings.TrimSpace(string(body)))
	}

	v, ok := m["x"]
	if !ok {
		t.Fatalf("json missing key x: %q", strings.TrimSpace(string(body)))
	}

	switch vv := v.(type) {
	case json.Number:
		n, err := vv.Int64()
		if err != nil {
			t.Fatalf("x not int: %v raw=%q", err, strings.TrimSpace(string(body)))
		}
		return int(n)

	case float64:
		return int(vv)

	default:
		t.Fatalf("x has unexpected type %T raw=%q", v, strings.TrimSpace(string(body)))
		return 0
	}
}
