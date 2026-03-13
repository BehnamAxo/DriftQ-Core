package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPDebug_PolicyAndRunSpecSimulation(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)
	reg := NewHandlerRegistry()

	reg.Register("safe_tool", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	})
	runner.SetHandlerRegistry(reg)

	mux := http.NewServeMux()
	AttachDebugRoutes(mux, runner)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	policyBody := `{
	  "rules":[
	    {"id":"allow-builder-run","effect":"allow","actions":["run_start"],"roles":["builder"]},
	    {"id":"allow-builder-tools","effect":"allow","actions":["tool_execute"],"roles":["builder"],"tools":["safe_tool"]}
	  ]
	}`
	respBody := mustPostJSONStatus(t, srv.URL+"/debug/policy", policyBody, http.StatusOK)
	var saveResp struct {
		OK     bool                      `json:"ok"`
		Policy AuthorizationPolicyBundle `json:"policy"`
	}
	if err := json.Unmarshal(respBody, &saveResp); err != nil {
		t.Fatalf("decode save policy: %v", err)
	}
	if !saveResp.OK || len(saveResp.Policy.Rules) != 2 {
		t.Fatalf("unexpected save response: %+v", saveResp)
	}

	getResp, err := http.Get(srv.URL + "/debug/policy")
	if err != nil {
		t.Fatalf("GET /debug/policy: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(getResp.Body)
		t.Fatalf("GET /debug/policy status=%d body=%s", getResp.StatusCode, string(body))
	}

	simBody := `{
	  "run_id":"sim-run",
	  "policy_mode":"simulate",
	  "principal":{"id":"agent-viewer","roles":["viewer"]},
	  "spec":{
	    "id":"wf_http_authz",
	    "nodes":[
	      {"id":"A","topic":"safe_tool"}
	    ]
	  },
	  "input":{}
	}`
	respBody = mustPostJSONStatus(t, srv.URL+"/debug/run-spec", simBody, http.StatusOK)

	var simResp struct {
		OK            bool                        `json:"ok"`
		Simulated     bool                        `json:"simulated"`
		Authorization WorkflowAuthorizationReport `json:"authorization"`
	}
	if err := json.Unmarshal(respBody, &simResp); err != nil {
		t.Fatalf("decode simulate response: %v body=%s", err, string(respBody))
	}
	if !simResp.OK || !simResp.Simulated {
		t.Fatalf("unexpected simulate response: %+v", simResp)
	}
	if simResp.Authorization.Allowed {
		t.Fatalf("expected simulated deny, got %+v", simResp.Authorization)
	}
	if _, ok := store.GetRun("sim-run"); ok {
		t.Fatal("simulate mode should not create a run")
	}

	enforceBody := `{
	  "run_id":"enforce-run",
	  "principal":{"id":"agent-viewer","roles":["viewer"]},
	  "spec":{
	    "id":"wf_http_authz",
	    "nodes":[
	      {"id":"A","topic":"safe_tool"}
	    ]
	  },
	  "input":{}
	}`
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/debug/run-spec", bytes.NewBufferString(enforceBody))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /debug/run-spec enforce: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST /debug/run-spec enforce status=%d body=%s", resp.StatusCode, string(body))
	}
	if _, ok := store.GetRun("enforce-run"); ok {
		t.Fatal("denied enforce run should not be created")
	}
}
