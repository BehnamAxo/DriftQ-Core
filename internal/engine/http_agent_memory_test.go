package engine

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPDebug_AgentMemoryRoutes(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	runner.SetArtifactStore(NewMemoryArtifactStore())
	mux := http.NewServeMux()
	AttachDebugRoutes(mux, runner)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/debug/agent-memory", bytes.NewBufferString(`{
		"agent_id":"planner",
		"source_kind":"note",
		"content":"customer billing export request"
	}`))

	if err != nil {
		t.Fatalf("NewRequest POST /debug/agent-memory: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-Id", "tenant-a")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /debug/agent-memory: %v", err)
	}

	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /debug/agent-memory status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	req, err = http.NewRequest(http.MethodGet, srv.URL+"/debug/agent-memory?agent_id=planner", nil)
	if err != nil {
		t.Fatalf("NewRequest GET /debug/agent-memory: %v", err)
	}

	req.Header.Set("X-Tenant-Id", "tenant-a")
	resp, err = http.DefaultClient.Do(req)

	if err != nil {
		t.Fatalf("GET /debug/agent-memory: %v", err)
	}

	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/agent-memory status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var listResp struct {
		OK      bool               `json:"ok"`
		Count   int                `json:"count"`
		Entries []AgentMemoryEntry `json:"entries"`
	}

	if err := json.Unmarshal(body, &listResp); err != nil {
		t.Fatalf("decode list response: %v body=%s", err, string(body))
	}

	if !listResp.OK || listResp.Count != 1 || len(listResp.Entries) != 1 {
		t.Fatalf("unexpected list response: %+v", listResp)
	}

	req, err = http.NewRequest(http.MethodPost, srv.URL+"/debug/agent-memory/search", bytes.NewBufferString(`{
		"agent_id":"planner",
		"query":"billing export"
	}`))

	if err != nil {
		t.Fatalf("NewRequest POST /debug/agent-memory/search: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-Id", "tenant-a")
	resp, err = http.DefaultClient.Do(req)

	if err != nil {
		t.Fatalf("POST /debug/agent-memory/search: %v", err)
	}

	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /debug/agent-memory/search status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var searchResp struct {
		OK      bool                      `json:"ok"`
		Count   int                       `json:"count"`
		Results []AgentMemorySearchResult `json:"results"`
	}

	if err := json.Unmarshal(body, &searchResp); err != nil {
		t.Fatalf("decode search response: %v body=%s", err, string(body))
	}

	if !searchResp.OK || searchResp.Count == 0 || len(searchResp.Results) == 0 {
		t.Fatalf("unexpected search response: %+v", searchResp)
	}

	req, err = http.NewRequest(http.MethodPost, srv.URL+"/debug/agent-memory", bytes.NewBufferString(`{
		"agent_id":"writer",
		"source_kind":"note",
		"content":"wrong agent scope"
	}`))

	if err != nil {
		t.Fatalf("NewRequest scoped POST /debug/agent-memory: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-Id", "tenant-a")
	req.Header.Set("X-Agent-Id", "planner")
	resp, err = http.DefaultClient.Do(req)

	if err != nil {
		t.Fatalf("POST /debug/agent-memory scoped: %v", err)
	}

	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for scoped agent mismatch, got %d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
}
