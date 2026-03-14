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

func TestHTTPDebug_AgentStateRoutes(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	mux := http.NewServeMux()
	AttachDebugRoutes(mux, runner)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/debug/agent-state", bytes.NewBufferString(`{
		"agent_id":"planner",
		"state":{"mode":"draft"}
	}`))

	if err != nil {
		t.Fatalf("NewRequest POST /debug/agent-state: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-Id", "tenant-a")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /debug/agent-state: %v", err)
	}

	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /debug/agent-state status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var postResp struct {
		OK       bool               `json:"ok"`
		Snapshot AgentStateSnapshot `json:"snapshot"`
	}

	if err := json.Unmarshal(body, &postResp); err != nil {
		t.Fatalf("decode post response: %v body=%s", err, string(body))
	}

	if !postResp.OK || postResp.Snapshot.Version != 1 {
		t.Fatalf("unexpected post response: %+v", postResp)
	}

	req, err = http.NewRequest(http.MethodGet, srv.URL+"/debug/agent-state?agent_id=planner", nil)
	if err != nil {
		t.Fatalf("NewRequest GET /debug/agent-state: %v", err)
	}
	req.Header.Set("X-Tenant-Id", "tenant-a")

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /debug/agent-state: %v", err)
	}

	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/agent-state status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var getResp struct {
		OK       bool               `json:"ok"`
		Snapshot AgentStateSnapshot `json:"snapshot"`
	}

	if err := json.Unmarshal(body, &getResp); err != nil {
		t.Fatalf("decode get response: %v body=%s", err, string(body))
	}

	if !getResp.OK || getResp.Snapshot.AgentID != "planner" || getResp.Snapshot.Version != 1 {
		t.Fatalf("unexpected get response: %+v", getResp)
	}

	req, err = http.NewRequest(http.MethodGet, srv.URL+"/debug/agent-state/lineage?agent_id=planner", nil)
	if err != nil {
		t.Fatalf("NewRequest GET /debug/agent-state/lineage: %v", err)
	}

	req.Header.Set("X-Tenant-Id", "tenant-a")
	resp, err = http.DefaultClient.Do(req)

	if err != nil {
		t.Fatalf("GET /debug/agent-state/lineage: %v", err)
	}

	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/agent-state/lineage status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var lineageResp struct {
		OK      bool                 `json:"ok"`
		Count   int                  `json:"count"`
		Lineage []AgentStateSnapshot `json:"lineage"`
	}

	if err := json.Unmarshal(body, &lineageResp); err != nil {
		t.Fatalf("decode lineage response: %v body=%s", err, string(body))
	}

	if !lineageResp.OK || lineageResp.Count != 1 || len(lineageResp.Lineage) != 1 {
		t.Fatalf("unexpected lineage response: %+v", lineageResp)
	}

	req, err = http.NewRequest(http.MethodGet, srv.URL+"/debug/agent-state?agent_id=planner", nil)
	if err != nil {
		t.Fatalf("NewRequest GET /debug/agent-state without tenant: %v", err)
	}

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /debug/agent-state without tenant: %v", err)
	}

	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 without tenant, got %d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	req, err = http.NewRequest(http.MethodPost, srv.URL+"/debug/agent-state", bytes.NewBufferString(`{
		"agent_id":"writer",
		"state":{"mode":"draft"}
	}`))

	if err != nil {
		t.Fatalf("NewRequest scoped POST /debug/agent-state: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-Id", "tenant-a")
	req.Header.Set("X-Agent-Id", "planner")
	resp, err = http.DefaultClient.Do(req)

	if err != nil {
		t.Fatalf("POST /debug/agent-state with scoped agent: %v", err)
	}

	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for scoped agent mismatch, got %d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
}
