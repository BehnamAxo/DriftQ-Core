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

func TestHTTPDebug_ReplayBranchRoutes(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)

	reg.Register("emit.v1", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"version":"v1"}`), nil
	})
	reg.Register("emit.v2", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"version":"v2"}`), nil
	})

	spec := []byte(`{
	  "id":"wf_http_branching",
	  "nodes":[
	    {"id":"A","topic":"emit.v1"},
	    {"id":"B","topic":"emit.v1","deps":["A"]}
	  ]
	}`)

	if err := runner.RunSpecJSON(context.Background(), "run-http-branch-source", spec, reg, json.RawMessage(`{"seed":"base"}`)); err != nil {
		t.Fatalf("RunSpecJSON source: %v", err)
	}

	mux := http.NewServeMux()
	AttachDebugRoutes(mux, runner)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/debug/replay/branches", bytes.NewBufferString(`{
	  "source_run_id":"run-http-branch-source",
	  "branch_name":"http-alt",
	  "from_step":"B",
	  "mode":"live",
	  "spec_override":{
	    "id":"wf_http_branching",
	    "nodes":[
	      {"id":"A","topic":"emit.v1"},
	      {"id":"B","topic":"emit.v2","deps":["A"]}
	    ]
	  }
	}`))

	if err != nil {
		t.Fatalf("NewRequest POST /debug/replay/branches: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		t.Fatalf("POST /debug/replay/branches: %v", err)
	}

	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /debug/replay/branches status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var createResp struct {
		OK          bool                    `json:"ok"`
		Branch      ReplayBranchRecord      `json:"branch"`
		WhatChanged ForensicWhatChangedView `json:"what_changed"`
	}

	if err := json.Unmarshal(body, &createResp); err != nil {
		t.Fatalf("decode create branch response: %v body=%s", err, string(body))
	}

	if !createResp.OK || createResp.Branch.BranchRunID == "" {
		t.Fatalf("unexpected create branch response: %+v", createResp)
	}

	timelineResp, err := http.Get(srv.URL + "/debug/replay/branches?run_id=" + createResp.Branch.BranchRunID)
	if err != nil {
		t.Fatalf("GET /debug/replay/branches: %v", err)
	}

	body, _ = io.ReadAll(timelineResp.Body)
	timelineResp.Body.Close()

	if timelineResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/replay/branches status=%d body=%s", timelineResp.StatusCode, strings.TrimSpace(string(body)))
	}

	compareResp, err := http.Get(srv.URL + "/debug/replay/compare?left_run_id=run-http-branch-source&right_run_id=" + createResp.Branch.BranchRunID)
	if err != nil {
		t.Fatalf("GET /debug/replay/compare: %v", err)
	}

	body, _ = io.ReadAll(compareResp.Body)
	compareResp.Body.Close()

	if compareResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/replay/compare status=%d body=%s", compareResp.StatusCode, strings.TrimSpace(string(body)))
	}

	if !strings.Contains(string(body), `"workflow spec changed between runs"`) {
		t.Fatalf("expected workflow diff summary in compare response, got %s", string(body))
	}
}
