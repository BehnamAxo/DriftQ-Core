package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/engine"
)

func TestDebug_RunSpec_AndInspect(t *testing.T) {
	baseURL, shutdown := startDebugTestServer(t, func(reg *engine.HandlerRegistry) {
		reg.Register("delay_once_2s", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			time.Sleep(25 * time.Millisecond)
			return json.RawMessage(`{"ok":true,"after":"delay"}`), nil
		})
	})
	defer shutdown()

	runID := "spec-demo-" + time.Now().UTC().Format("20060102T150405.000000000Z")

	body := map[string]any{
		"run_id": runID,
		"spec": map[string]any{
			"id": "spec_demo",
			"nodes": []any{
				map[string]any{
					"id":    "A",
					"topic": "delay_once_2s", // IMPORTANT: this MUST match registry.Register(...)
				},
			},
			"edges": []any{},
		},
		"input": map[string]any{"x": 1},
	}

	status, respBody := httpPostJSON(t, baseURL, "/debug/run-spec", body, 5*time.Second)
	if status != http.StatusOK {
		t.Fatalf("POST /debug/run-spec status=%d body=%s", status, strings.TrimSpace(string(respBody)))
	}

	inspectPath := "/debug/run?run_id=" + url.QueryEscape(runID)
	istatus, ibody := httpGet(t, baseURL, inspectPath, 5*time.Second)
	if istatus != http.StatusOK {
		t.Fatalf("GET %s status=%d body=%s", inspectPath, istatus, strings.TrimSpace(string(ibody)))
	}

	var v map[string]any
	if err := json.Unmarshal(ibody, &v); err != nil {
		t.Fatalf("inspect response not json: %v body=%s", err, strings.TrimSpace(string(ibody)))
	}

	nodesAny, _ := v["nodes"].([]any)
	if len(nodesAny) == 0 {
		t.Fatalf("expected nodes in /debug/run response, got: %s", strings.TrimSpace(string(ibody)))
	}
}

func TestDebug_RunSpec_Cancel(t *testing.T) {
	started := make(chan struct{})
	var startedOnce sync.Once

	baseURL, shutdown := startDebugTestServer(t, func(reg *engine.HandlerRegistry) {
		reg.Register("ctx_sleep_long", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			startedOnce.Do(func() { close(started) })
			select {
			case <-time.After(30 * time.Second):
				return input, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
	})
	defer shutdown()

	runID := "spec-cancel-" + time.Now().UTC().Format("20060102T150405.000000000Z")

	runSpecBody := map[string]any{
		"run_id": runID,
		"spec": map[string]any{
			"id": "spec_cancel_demo",
			"nodes": []any{
				map[string]any{
					"id":    "A",
					"topic": "ctx_sleep_long", // IMPORTANT: same here it MUST match registry.Register(...)
				},
			},
			"edges": []any{},
		},
		"input": map[string]any{"x": 1},
	}

	type res struct {
		status int
		body   []byte
	}
	done := make(chan res, 1)

	go func() {
		st, b := httpPostJSON(t, baseURL, "/debug/run-spec", runSpecBody, 35*time.Second)
		done <- res{status: st, body: b}
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatalf("run never started; cancellation test can't proceed")
	}

	cancelBody := map[string]any{
		"run_id": runID,
		"reason": "test cancel",
	}

	cstatus, cbody := httpPostJSON(t, baseURL, "/debug/run-cancel", cancelBody, 5*time.Second)
	if cstatus != http.StatusOK {
		t.Fatalf("POST /debug/run-cancel status=%d body=%s", cstatus, strings.TrimSpace(string(cbody)))
	}

	r := <-done
	if r.status != http.StatusOK {
		t.Fatalf("POST /debug/run-spec after cancel status=%d body=%s", r.status, strings.TrimSpace(string(r.body)))
	}

	var out map[string]any
	_ = json.Unmarshal(r.body, &out)
	if canceled, _ := out["canceled"].(bool); !canceled {
		t.Fatalf("expected canceled=true in run-spec response, got: %s", strings.TrimSpace(string(r.body)))
	}
}

func startDebugTestServer(t *testing.T, register func(reg *engine.HandlerRegistry)) (baseURL string, shutdown func()) {
	t.Helper()

	store := engine.NewMemoryStore()
	runner := engine.NewRunner(store)

	reg := engine.NewHandlerRegistry()
	if register != nil {
		register(reg)
	}
	runner.SetHandlerRegistry(reg)

	mux := http.NewServeMux()
	engine.AttachDebugRoutes(mux, runner)

	srv := httptest.NewServer(mux)
	return srv.URL, srv.Close
}

func httpPostJSON(t *testing.T, baseURL, path string, body any, timeout time.Duration) (int, []byte) {
	t.Helper()

	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("json marshal: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(baseURL, "/")+path, bytes.NewReader(b))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	c := &http.Client{Timeout: timeout}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()

	rb, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, rb
}

func httpGet(t *testing.T, baseURL, path string, timeout time.Duration) (int, []byte) {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(baseURL, "/")+path, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	c := &http.Client{Timeout: timeout}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()

	rb, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, rb
}
