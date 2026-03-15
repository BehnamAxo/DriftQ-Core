package engine

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

func writeForensicError(w http.ResponseWriter, prefix string, err error) {
	switch {
	case errors.Is(err, ErrRunNotFound):
		http.Error(w, prefix+": "+err.Error(), http.StatusNotFound)
	case errors.Is(err, ErrTenantAccessDenied), errors.Is(err, ErrAuthorizationDenied):
		http.Error(w, prefix+": "+err.Error(), http.StatusForbidden)
	default:
		http.Error(w, prefix+": "+err.Error(), http.StatusBadRequest)
	}
}

func attachForensicRoutes(mux *http.ServeMux, runner *Runner) {
	mux.HandleFunc("/debug/forensics/lineage", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)
		runID := strings.TrimSpace(r.URL.Query().Get("run_id"))

		if runID == "" {
			http.Error(w, "run_id is required", http.StatusBadRequest)
			return
		}

		graph, err := runner.BuildExecutionGraph(ctx, runID)
		if err != nil {
			writeForensicError(w, "build execution graph failed", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":       true,
			"graph":    graph,
			"trace_id": traceID,
		})
	})

	mux.HandleFunc("/debug/forensics/run-diff", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)
		fromRunID := strings.TrimSpace(r.URL.Query().Get("from_run_id"))
		toRunID := strings.TrimSpace(r.URL.Query().Get("to_run_id"))

		if fromRunID == "" || toRunID == "" {
			http.Error(w, "from_run_id and to_run_id are required", http.StatusBadRequest)
			return
		}

		diff, err := runner.DiffRuns(ctx, fromRunID, toRunID)

		if err != nil {
			writeForensicError(w, "diff runs failed", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":       true,
			"diff":     diff,
			"trace_id": traceID,
		})
	})

	mux.HandleFunc("/debug/forensics/workflow-diff", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)
		fromRunID := strings.TrimSpace(r.URL.Query().Get("from_run_id"))
		toRunID := strings.TrimSpace(r.URL.Query().Get("to_run_id"))

		if fromRunID == "" || toRunID == "" {
			http.Error(w, "from_run_id and to_run_id are required", http.StatusBadRequest)
			return
		}

		diff, err := runner.DiffRuns(ctx, fromRunID, toRunID)
		if err != nil {
			writeForensicError(w, "workflow diff failed", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":            true,
			"workflow_diff": diff.WorkflowSpec,
			"trace_id":      traceID,
		})
	})

	mux.HandleFunc("/debug/forensics/root-cause", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)
		runID := strings.TrimSpace(r.URL.Query().Get("run_id"))

		if runID == "" {
			http.Error(w, "run_id is required", http.StatusBadRequest)
			return
		}

		view, err := runner.BuildRootCauseView(ctx, runID)
		if err != nil {
			writeForensicError(w, "build root cause view failed", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":         true,
			"root_cause": view,
			"trace_id":   traceID,
		})
	})

	mux.HandleFunc("/debug/forensics/what-changed", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)
		fromRunID := strings.TrimSpace(r.URL.Query().Get("from_run_id"))
		toRunID := strings.TrimSpace(r.URL.Query().Get("to_run_id"))

		if fromRunID == "" || toRunID == "" {
			http.Error(w, "from_run_id and to_run_id are required", http.StatusBadRequest)
			return
		}

		view, err := runner.BuildWhatChangedView(ctx, fromRunID, toRunID)
		if err != nil {
			writeForensicError(w, "build what-changed view failed", err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":           true,
			"what_changed": view,
			"trace_id":     traceID,
		})
	})
}
