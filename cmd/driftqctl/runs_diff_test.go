package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRunsDiff_PrintsAttemptDelta(t *testing.T) {
	// NOTE: do NOT run in parallel: captureStdout swaps os.Stdout globally.
	// t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("expected GET, got %s", r.Method)
		}

		switch r.URL.Path {
		case "/debug/run-state":
			// Newer implementation can compute diffs from run-state by comparing attempts.
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"ok": true,
				"run": {"id":"spec-diff-1"},
				"nodes": [
					{
						"run_id": "spec-diff-1",
						"workflow_id": "wf1",
						"node_id": "A",
						"attempt": 1,
						"status": "failed",
						"error": "boom",
						"input": {"x": 1},
						"output": {}
					},
					{
						"run_id": "spec-diff-1",
						"workflow_id": "wf1",
						"node_id": "A",
						"attempt": 2,
						"status": "succeeded",
						"error": "",
						"input": {"x": 1},
						"output": {"ok": true}
					}
				],
				"events": []
			}`))
			return

		case "/debug/run-node":
			// Older implementation may call a dedicated node-diff endpoint.
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"ok": true,
				"from": {
					"run_id": "spec-diff-1",
					"node_id": "A",
					"attempt": 1,
					"status": "failed",
					"error": "boom",
					"input": {"x": 1},
					"output": {}
				},
				"to": {
					"run_id": "spec-diff-1",
					"node_id": "A",
					"attempt": 2,
					"status": "succeeded",
					"error": "",
					"input": {"x": 1},
					"output": {"ok": true}
				},
				"diff": {
					"status": "failed -> succeeded",
					"error": "boom -> -",
					"dur": "590.9µs -> 0s",
					"input": "(unchanged)",
					"output": "changed"
				}
			}`))
			return

		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	out := captureStdout(t, func() {
		err := runsDiff(srv.URL, 3*time.Second, []string{
			"--run-id", "spec-diff-1",
			"--node-id", "A",
			"--from", "1",
			"--to", "2",
		})
		if err != nil {
			t.Fatalf("runsDiff returned error: %v", err)
		}
	})

	// Be tolerant: driftqctl prints are human-friendly and may change spacing.
	if !(strings.Contains(out, "attempt") && strings.Contains(out, "1") && strings.Contains(out, "2")) {
		t.Fatalf("expected attempt info, got:\n%s", out)
	}
	if !(strings.Contains(out, "failed") && strings.Contains(out, "succeeded")) {
		t.Fatalf("expected status delta, got:\n%s", out)
	}
	if !strings.Contains(out, "boom") {
		t.Fatalf("expected error mention, got:\n%s", out)
	}
	if !strings.Contains(out, "ok") {
		t.Fatalf("expected output mention, got:\n%s", out)
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}

	os.Stdout = w
	fn()

	_ = w.Close()
	os.Stdout = orig

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)
	_ = r.Close()

	return buf.String()
}
