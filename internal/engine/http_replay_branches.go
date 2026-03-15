package engine

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

func writeReplayBranchError(w http.ResponseWriter, prefix string, err error) {
	switch {
	case errors.Is(err, ErrRunNotFound):
		http.Error(w, prefix+": "+err.Error(), http.StatusNotFound)
	case errors.Is(err, ErrTenantAccessDenied), errors.Is(err, ErrAuthorizationDenied):
		http.Error(w, prefix+": "+err.Error(), http.StatusForbidden)
	default:
		http.Error(w, prefix+": "+err.Error(), http.StatusBadRequest)
	}
}

func attachReplayBranchRoutes(mux *http.ServeMux, runner *Runner) {
	mux.HandleFunc("/debug/replay/branches", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodPost:
		default:
			w.Header().Set("Allow", http.MethodGet+", "+http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)

		switch r.Method {
		case http.MethodGet:
			runID := strings.TrimSpace(r.URL.Query().Get("run_id"))
			if runID == "" {
				http.Error(w, "run_id is required", http.StatusBadRequest)
				return
			}

			timeline, err := runner.BuildReplayTimeline(ctx, runID)
			if err != nil {
				writeReplayBranchError(w, "build replay timeline failed", err)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":       true,
				"timeline": timeline,
				"trace_id": traceID,
			})

		case http.MethodPost:
			var body ReplayBranchRequest
			dec := json.NewDecoder(r.Body)
			dec.DisallowUnknownFields()
			if err := dec.Decode(&body); err != nil {
				http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
				return
			}

			record, err := runner.CreateReplayBranch(ctx, body)
			if err != nil {
				writeReplayBranchError(w, "create replay branch failed", err)
				return
			}

			run, _ := runner.store.GetRun(record.BranchRunID)
			whatChanged, diffErr := runner.BuildWhatChangedView(ctx, record.SourceRunID, record.BranchRunID)

			w.Header().Set("Content-Type", "application/json")
			resp := map[string]any{
				"ok":       true,
				"branch":   record,
				"run":      run,
				"trace_id": traceID,
			}

			if diffErr == nil {
				resp["what_changed"] = whatChanged
			}
			_ = json.NewEncoder(w).Encode(resp)
		}
	})

	mux.HandleFunc("/debug/replay/branch", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)
		branchID := strings.TrimSpace(r.URL.Query().Get("id"))
		branchRunID := strings.TrimSpace(r.URL.Query().Get("run_id"))

		var (
			record ReplayBranchRecord
			ok     bool
			err    error
		)

		if branchID != "" {
			record, ok, err = runner.GetReplayBranch(ctx, branchID)
		} else {
			record, ok, err = runner.GetReplayBranchByRun(ctx, branchRunID)
		}

		if err != nil {
			writeReplayBranchError(w, "get replay branch failed", err)
			return
		}

		if !ok {
			http.Error(w, "replay branch not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":       true,
			"branch":   record,
			"trace_id": traceID,
		})
	})

	mux.HandleFunc("/debug/replay/compare", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)
		leftRunID := strings.TrimSpace(r.URL.Query().Get("left_run_id"))
		rightRunID := strings.TrimSpace(r.URL.Query().Get("right_run_id"))

		if leftRunID == "" || rightRunID == "" {
			http.Error(w, "left_run_id and right_run_id are required", http.StatusBadRequest)
			return
		}

		view, err := runner.BuildWhatChangedView(ctx, leftRunID, rightRunID)
		if err != nil {
			writeReplayBranchError(w, "compare replay branches failed", err)
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
