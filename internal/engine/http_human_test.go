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

func TestHTTPDebug_HumanRespondFlow(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)
	reg := NewHandlerRegistry()

	reg.Register("echo", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return cloneRaw(input), nil
	})
	runner.SetHandlerRegistry(reg)

	mux := http.NewServeMux()
	AttachDebugRoutes(mux, runner)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	respBody := mustPostJSONStatus(t, srv.URL+"/debug/run-spec", `{
	  "run_id":"http-human-run",
	  "spec":{
	    "id":"wf_http_human",
	    "nodes":[
	      {"id":"review","human":{"mode":"edit","prompt":"review"}},
	      {"id":"echo","topic":"echo","deps":["review"]}
	    ]
	  },
	  "input":{"message":"draft"}
	}`, http.StatusOK)

	var runResp struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal(respBody, &runResp); err != nil {
		t.Fatalf("decode run-spec response: %v", err)
	}
	if !runResp.OK {
		t.Fatalf("expected ok run-spec response, got %s", string(respBody))
	}

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/debug/human/tasks?run_id=http-human-run&status=pending", nil)
	if err != nil {
		t.Fatalf("NewRequest tasks: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /debug/human/tasks: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/human/tasks status=%d body=%s", resp.StatusCode, string(body))
	}

	var tasksResp struct {
		OK    bool        `json:"ok"`
		Tasks []HumanTask `json:"tasks"`
	}
	if err := json.Unmarshal(body, &tasksResp); err != nil {
		t.Fatalf("decode tasks response: %v body=%s", err, string(body))
	}
	if !tasksResp.OK || len(tasksResp.Tasks) != 1 {
		t.Fatalf("unexpected tasks response: %+v", tasksResp)
	}

	payload := `{"task_id":"` + tasksResp.Tasks[0].ID + `","decision":"approve","edited_input":{"message":"approved"}}`
	req, err = http.NewRequest(http.MethodPost, srv.URL+"/debug/human/respond", bytes.NewBufferString(payload))
	if err != nil {
		t.Fatalf("NewRequest respond: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /debug/human/respond: %v", err)
	}
	defer resp.Body.Close()
	body, _ = io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /debug/human/respond status=%d body=%s", resp.StatusCode, string(body))
	}

	run, ok := store.GetRun("http-human-run")
	if !ok || run.Status != RunStatusSucceeded {
		t.Fatalf("expected succeeded run after human respond, got ok=%v run=%+v", ok, run)
	}
}
