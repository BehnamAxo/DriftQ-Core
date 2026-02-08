package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRunsReplay_POSTsRunReplayAndPrintsNextSteps(t *testing.T) {
	// NOTE: do NOT run in parallel: captureStdout swaps os.Stdout globally (defined in runs_diff_test.go).
	// t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}

		if r.URL.Path != "/debug/run-replay" {
			t.Fatalf("expected /debug/run-replay, got %s", r.URL.Path)
		}

		b, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()

		var req map[string]any
		if err := json.Unmarshal(b, &req); err != nil {
			t.Fatalf("bad json body: %v body=%s", err, string(b))
		}

		if req["run_id"] != "r1" || req["from_step"] != "A" || req["mode"] != "live" {
			t.Fatalf("unexpected req: %+v", req)
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"run_id":"r1"}`))
	}))
	defer srv.Close()

	out := captureStdout(t, func() {
		if err := cmdRuns(srv.URL, 2*time.Second, []string{
			"replay",
			"--run-id", "r1",
			"--from-step", "A",
			"--mode", "live",
		}); err != nil {
			t.Fatalf("cmdRuns replay: %v", err)
		}
	})

	if !strings.Contains(out, "replay started run_id=r1 from_step=A mode=live") {
		t.Fatalf("unexpected output: %s", out)
	}

	if !strings.Contains(out, "next: driftqctl runs timeline --run-id r1") {
		t.Fatalf("missing next timeline hint: %s", out)
	}
}

func TestRunsTimeline_PrintsProofFields(t *testing.T) {
	// NOTE: do NOT run in parallel: captureStdout swaps os.Stdout globally (defined in runs_diff_test.go).
	// t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("expected GET, got %s", r.Method)
		}
		if r.URL.Path != "/debug/run-state" {
			t.Fatalf("expected /debug/run-state, got %s", r.URL.Path)
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"ok": true,
			"run": {"id":"r1"},
			"nodes": [],
			"events": [
				{
					"type": "node_finished",
					"step_id": "embed_chunks",
					"attempt": 2,
					"payload": {
						"used_cached_output": true,
						"cached_attempt": 1,
						"queue_ms": 123,
						"worker_ms": 456
					}
				}
			]
		}`))
	}))
	defer srv.Close()

	out := captureStdout(t, func() {
		if err := cmdRuns(srv.URL, 2*time.Second, []string{
			"timeline",
			"--run-id", "r1",
		}); err != nil {
			t.Fatalf("cmdRuns timeline: %v", err)
		}
	})

	// Header: tolerate tabs/spaces and additional columns.
	if !(strings.Contains(out, "attempt") && strings.Contains(out, "step") &&
		strings.Contains(out, "queue_ms") && strings.Contains(out, "worker_ms")) {
		t.Fatalf("missing header: %s", out)
	}

	// Row: tolerate tabwriter spacing.
	if !(strings.Contains(out, "2") && strings.Contains(out, "embed_chunks") &&
		strings.Contains(out, "true") && strings.Contains(out, "123") && strings.Contains(out, "456")) {
		t.Fatalf("missing row: %s", out)
	}
}
