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

func TestRunsActiveIndex_PrintsValue(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("expected GET, got %s", r.Method)
		}
		if r.URL.Path != "/debug/index/active" {
			t.Fatalf("expected /debug/index/active, got %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"active_version":"v1"}`))
	}))
	defer srv.Close()

	out := captureStdout(t, func() {
		if err := cmdRuns(srv.URL, 2*time.Second, []string{"active-index"}); err != nil {
			t.Fatalf("cmdRuns active-index: %v", err)
		}
	})
	if !strings.Contains(out, "active_index_version=v1") {
		t.Fatalf("unexpected output: %s", out)
	}
}

func TestRunsPromote_POSTsAndPrintsNext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/debug/index/promote" {
			t.Fatalf("expected /debug/index/promote, got %s", r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()

		var req map[string]any
		if err := json.Unmarshal(b, &req); err != nil {
			t.Fatalf("bad json: %v body=%s", err, string(b))
		}
		if req["run_id"] != "r1" || req["version"] != "r1" {
			t.Fatalf("unexpected req: %+v", req)
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"active_version":"r1"}`))
	}))
	defer srv.Close()

	out := captureStdout(t, func() {
		if err := cmdRuns(srv.URL, 2*time.Second, []string{
			"promote",
			"--run-id", "r1",
		}); err != nil {
			t.Fatalf("cmdRuns promote: %v", err)
		}
	})

	if !strings.Contains(out, "promoted active_index_version=r1") {
		t.Fatalf("unexpected output: %s", out)
	}
	if !strings.Contains(out, "next: driftqctl runs active-index") {
		t.Fatalf("missing next hint: %s", out)
	}
}

func TestRunsRollback_POSTsAndPrintsNext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/debug/index/rollback" {
			t.Fatalf("expected /debug/index/rollback, got %s", r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()

		var req map[string]any
		if err := json.Unmarshal(b, &req); err != nil {
			t.Fatalf("bad json: %v body=%s", err, string(b))
		}
		if req["version"] != "v0" {
			t.Fatalf("unexpected req: %+v", req)
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"active_version":"v0"}`))
	}))
	defer srv.Close()

	out := captureStdout(t, func() {
		if err := cmdRuns(srv.URL, 2*time.Second, []string{
			"rollback",
			"--to", "v0",
		}); err != nil {
			t.Fatalf("cmdRuns rollback: %v", err)
		}
	})

	if !strings.Contains(out, "rolled back active_index_version=v0") {
		t.Fatalf("unexpected output: %s", out)
	}
	if !strings.Contains(out, "next: driftqctl runs active-index") {
		t.Fatalf("missing next hint: %s", out)
	}
}
