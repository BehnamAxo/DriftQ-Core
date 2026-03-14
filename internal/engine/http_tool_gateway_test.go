package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPDebug_ToolGatewayAndCalls(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)
	reg.Register("safe_tool", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"status":"ok","token":"secret"}`), nil
	})

	mux := http.NewServeMux()
	AttachDebugRoutes(mux, runner)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/debug/tool-gateway", bytes.NewBufferString(`{
	  "tools":[{"id":"safe","tool":"safe_tool","approved":true,"redact_fields":["api_key"]}],
	  "servers":[{"id":"mcp-safe","kind":"mcp","approved":true,"allowed_tools":["safe_tool"],"tenant_scopes":["tenant-a"],"sandbox_allowed":true}]
	}`))

	if err != nil {
		t.Fatalf("NewRequest POST /debug/tool-gateway: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-Id", "tenant-a")
	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		t.Fatalf("POST /debug/tool-gateway: %v", err)
	}

	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /debug/tool-gateway status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	req, err = http.NewRequest(http.MethodGet, srv.URL+"/debug/tool-gateway", nil)
	if err != nil {
		t.Fatalf("NewRequest GET /debug/tool-gateway: %v", err)
	}

	req.Header.Set("X-Tenant-Id", "tenant-a")
	resp, err = http.DefaultClient.Do(req)

	if err != nil {
		t.Fatalf("GET /debug/tool-gateway: %v", err)
	}

	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/tool-gateway status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	spec := []byte(`{"id":"wf_http_tool","nodes":[{"id":"call","topic":"safe_tool"}]}`)
	if err := runner.RunSpecJSON(WithTenantID(context.Background(), "tenant-a"), "run-http-tool", spec, reg, json.RawMessage(`{"api_key":"top-secret"}`)); err != nil {
		t.Fatalf("RunSpecJSON: %v", err)
	}

	req, err = http.NewRequest(http.MethodGet, srv.URL+"/debug/tool-calls?run_id=run-http-tool", nil)
	if err != nil {
		t.Fatalf("NewRequest GET /debug/tool-calls: %v", err)
	}

	req.Header.Set("X-Tenant-Id", "tenant-a")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /debug/tool-calls: %v", err)
	}

	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/tool-calls status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var listResp struct {
		OK      bool             `json:"ok"`
		Count   int              `json:"count"`
		Records []ToolCallRecord `json:"records"`
	}

	if err := json.Unmarshal(body, &listResp); err != nil {
		t.Fatalf("decode /debug/tool-calls response: %v body=%s", err, string(body))
	}

	if !listResp.OK || listResp.Count != 1 || len(listResp.Records) != 1 {
		t.Fatalf("unexpected /debug/tool-calls response: %+v", listResp)
	}
}
