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
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/debug/run-state" {
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{
  "ok": true,
  "run": {"run_id":"spec-diff-1","workflow_id":"wf_diff","status":"succeeded"},
  "nodes": [
    {"run_id":"spec-diff-1","workflow_id":"wf_diff","node_id":"A","attempt":1,"status":"failed","error":"boom","input":{"x":1},"started_at":"2026-01-16T19:13:22.4050345Z","ended_at":"2026-01-16T19:13:22.4056254Z"},
    {"run_id":"spec-diff-1","workflow_id":"wf_diff","node_id":"A","attempt":2,"status":"succeeded","input":{"x":1},"output":{"ok":true},"started_at":"2026-01-16T19:13:26.0251286Z","ended_at":"2026-01-16T19:13:26.0251286Z"}
  ],
  "events": [],
  "timers": []
}`))
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

	if !strings.Contains(out, "attempts=1->2") {
		t.Fatalf("expected attempts header, got:\n%s", out)
	}

	if !strings.Contains(out, "status: failed -> succeeded") {
		t.Fatalf("expected status delta, got:\n%s", out)
	}

	if !strings.Contains(out, "error:  boom -> -") {
		t.Fatalf("expected error delta, got:\n%s", out)
	}

	if !strings.Contains(out, "output: changed") {
		t.Fatalf("expected output changed, got:\n%s", out)
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
