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
	"time"
)

func TestHTTPDebug_BrainPolicyAndRouteExplain(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)
	reg.Register("llm.http.brain", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	})

	if err := runner.SaveToolGatewayBundle(ToolGatewayBundle{
		Tools: []ToolPolicy{{
			ID:       "llm-http-brain",
			Tool:     "llm.http.brain",
			Approved: true,
			AdaptiveRouting: &AdaptiveRoutingPolicy{
				CheapFirst: true,
				Routes: []AdaptiveRoute{
					{ID: "cheap", Provider: "openai", Model: "gpt-mini", EstimatedDollars: 0.01, Priority: 1},
					{ID: "strong", Provider: "anthropic", Model: "sonnet", EstimatedDollars: 0.07, Priority: 10},
				},
			},
		}},
	}); err != nil {
		t.Fatalf("SaveToolGatewayBundle: %v", err)
	}

	seedCtx := WithTenantID(context.Background(), "tenant-a")
	for i := 0; i < 3; i++ {
		runner.saveToolCallRecord(seedCtx, ToolCallRecord{
			At:         time.Now().UTC(),
			TenantID:   "tenant-a",
			Tool:       "llm.http.brain",
			WorkflowID: "wf_http_brain",
			RouteID:    "strong",
			Provider:   "anthropic",
			Model:      "sonnet",
			Allowed:    true,
			DurationMS: 160,
		})

		runner.saveToolCallRecord(seedCtx, ToolCallRecord{
			At:         time.Now().UTC(),
			TenantID:   "tenant-a",
			Tool:       "llm.http.brain",
			WorkflowID: "wf_http_brain",
			RouteID:    "cheap",
			Provider:   "openai",
			Model:      "gpt-mini",
			Allowed:    false,
			DurationMS: 70,
			Error:      "timeout",
		})
	}

	mux := http.NewServeMux()
	AttachDebugRoutes(mux, runner)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/debug/brain-policy", bytes.NewBufferString(`{
	  "enabled":true,
	  "lookback_records":50,
	  "min_samples":2
	}`))

	if err != nil {
		t.Fatalf("NewRequest POST /debug/brain-policy: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /debug/brain-policy: %v", err)
	}

	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /debug/brain-policy status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	req, err = http.NewRequest(http.MethodPost, srv.URL+"/debug/brain/route", bytes.NewBufferString(`{
	  "workflow_id":"wf_http_brain",
	  "node_id":"gen",
	  "attempt":1,
	  "tool":"llm.http.brain",
	  "input":{"prompt":"hello"}
	}`))

	if err != nil {
		t.Fatalf("NewRequest POST /debug/brain/route: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-Id", "tenant-a")
	resp, err = http.DefaultClient.Do(req)

	if err != nil {
		t.Fatalf("POST /debug/brain/route: %v", err)
	}

	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /debug/brain/route status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var explainResp struct {
		OK       bool          `json:"ok"`
		Decision BrainDecision `json:"decision"`
	}

	if err := json.Unmarshal(body, &explainResp); err != nil {
		t.Fatalf("decode /debug/brain/route response: %v body=%s", err, string(body))
	}

	if !explainResp.OK || explainResp.Decision.Selected.ID != "strong" {
		t.Fatalf("unexpected brain explain response: %+v", explainResp)
	}
}
