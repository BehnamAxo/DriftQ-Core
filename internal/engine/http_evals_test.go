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

func TestHTTPDebug_Evals_RunAndPromote(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)
	reg := NewHandlerRegistry()

	reg.Register("emit_v1", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"answer":"v1"}`), nil
	})
	runner.SetHandlerRegistry(reg)

	sourceSpec := []byte(`{
	  "id":"wf_eval_http",
	  "nodes":[
	    {"id":"A","topic":"emit_v1"}
	  ]
	}`)
	if err := runner.RunSpecJSON(context.Background(), "src-http", sourceSpec, reg, json.RawMessage(`{}`)); err != nil {
		t.Fatalf("RunSpecJSON: %v", err)
	}

	mux := http.NewServeMux()
	AttachDebugRoutes(mux, runner)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	datasetBody := `{
	  "id":"dataset-http",
	  "cases":[
	    {
	      "id":"case-http",
	      "source_run_id":"src-http",
	      "target_node_id":"A",
	      "expected_output":{"answer":"v1"}
	    }
	  ]
	}`
	mustPostJSONStatus(t, srv.URL+"/debug/evals/datasets", datasetBody, http.StatusOK)

	suiteBody := `{
	  "id":"suite-http",
	  "dataset_id":"dataset-http",
	  "evaluator":"node_output_exact",
	  "target_node_id":"A",
	  "pass_threshold":1
	}`
	mustPostJSONStatus(t, srv.URL+"/debug/evals/suites", suiteBody, http.StatusOK)

	runRespBody := mustPostJSONStatus(t, srv.URL+"/debug/evals/run", `{"eval_run_id":"eval-http","suite_id":"suite-http"}`, http.StatusOK)
	var runResp struct {
		OK      bool       `json:"ok"`
		EvalRun EvalReport `json:"eval_run"`
	}
	if err := json.Unmarshal(runRespBody, &runResp); err != nil {
		t.Fatalf("decode eval run: %v body=%s", err, strings.TrimSpace(string(runRespBody)))
	}
	if !runResp.OK || !runResp.EvalRun.Passed {
		t.Fatalf("expected passing eval run, got %+v", runResp)
	}

	getResp, err := http.Get(srv.URL + "/debug/evals/run?id=eval-http")
	if err != nil {
		t.Fatalf("GET eval run: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(getResp.Body)
		t.Fatalf("GET eval run status=%d body=%s", getResp.StatusCode, string(body))
	}

	promoteRespBody := mustPostJSONStatus(t, srv.URL+"/debug/evals/promote", `{"eval_run_id":"eval-http","run_id":"src-http","version":"eval-http-v1"}`, http.StatusOK)
	var promoteResp struct {
		OK            bool   `json:"ok"`
		ActiveVersion string `json:"active_version"`
	}
	if err := json.Unmarshal(promoteRespBody, &promoteResp); err != nil {
		t.Fatalf("decode promote: %v body=%s", err, strings.TrimSpace(string(promoteRespBody)))
	}
	if !promoteResp.OK || promoteResp.ActiveVersion != "eval-http-v1" {
		t.Fatalf("unexpected promote response: %+v", promoteResp)
	}
}

func mustPostJSONStatus(t *testing.T, url string, body string, wantStatus int) []byte {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("POST %s status=%d want=%d body=%s", url, resp.StatusCode, wantStatus, string(respBody))
	}
	return respBody
}
