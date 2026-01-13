package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestCancelInterruptsInflightRun(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)

	reg := NewHandlerRegistry()

	startedCh := make(chan struct{})
	doneCh := make(chan struct{})
	var startedOnce sync.Once
	var doneOnce sync.Once

	// ctx-aware long work: this is what proves "interrupt in-flight" is real
	reg.Register("ctx_sleep_long", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		startedOnce.Do(func() { close(startedCh) })

		select {
		case <-time.After(10 * time.Second):
			doneOnce.Do(func() { close(doneCh) })
			return input, nil
		case <-ctx.Done():
			doneOnce.Do(func() { close(doneCh) })
			return nil, ctx.Err()
		}
	})

	runner.SetHandlerRegistry(reg)

	mux := http.NewServeMux()
	AttachDebugRoutes(mux, runner)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	runID := "cancel-inflight-test-1"

	runSpec := map[string]any{
		"run_id": runID,
		"spec": map[string]any{
			"id": "wf_cancel_inflight_test",
			"nodes": []any{
				map[string]any{"id": "A", "topic": "ctx_sleep_long"},
			},
		},
		"input": map[string]any{"x": 1},
	}

	// IMPORTANT: start run-spec in the background (because it blocks until run completes)
	runSpecDone := make(chan error, 1)
	go func() {
		runSpecDone <- postJSON(http.DefaultClient, srv.URL+"/debug/run-spec", runSpec)
	}()

	// Wait until node is actually running
	select {
	case <-startedCh:
		// good
	case <-time.After(1500 * time.Millisecond):
		state := mustGet(t, srv.URL+"/debug/run-state?run_id="+runID)
		t.Fatalf("node never started; state=%s", state)
	}

	// Cancel while node is in-flight
	if err := postJSON(http.DefaultClient, srv.URL+"/debug/run-cancel", map[string]any{
		"run_id": runID,
		"reason": "test",
	}); err != nil {
		t.Fatalf("cancel request failed: %v", err)
	}

	// PROOF: handler must exit quickly due to ctx cancellation (not after 10s)
	select {
	case <-doneCh:
		// good
	case <-time.After(1500 * time.Millisecond):
		state := mustGet(t, srv.URL+"/debug/run-state?run_id="+runID)
		t.Fatalf("cancel did NOT interrupt in-flight node fast enough; state=%s", state)
	}

	// Wait for /debug/run-spec to return (it should return soon after cancellation)
	select {
	case err := <-runSpecDone:
		if err != nil {
			t.Fatalf("run-spec failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("run-spec did not return after cancellation")
	}

	// Assert run status is canceled
	stateBody := mustGet(t, srv.URL+"/debug/run-state?run_id="+runID)
	var st struct {
		OK  bool `json:"ok"`
		Run Run  `json:"run"`
	}
	_ = json.Unmarshal([]byte(stateBody), &st)

	if st.Run.RunID == "" {
		t.Fatalf("missing run in state response: %s", stateBody)
	}

	if st.Run.Status != RunStatusCanceled {
		t.Fatalf("expected run status %q, got %q; state=%s", RunStatusCanceled, st.Run.Status, stateBody)
	}
}

func postJSON(client *http.Client, url string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("POST %s => %d body=%s", url, resp.StatusCode, string(bodyBytes))
	}
	return nil
}

func mustGet(t *testing.T, url string) string {
	t.Helper()

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	return string(bodyBytes)
}
