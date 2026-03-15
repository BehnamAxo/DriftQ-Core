package engine

import (
	"encoding/json"
	"net/http"
	"strings"
)

func attachWorkflowReleaseRoutes(mux *http.ServeMux, runner *Runner) {
	mux.HandleFunc("/debug/workflows/releases/versions", func(w http.ResponseWriter, r *http.Request) {
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
			workflowID := strings.TrimSpace(r.URL.Query().Get("workflow_id"))
			versions, err := runner.ListWorkflowReleaseVersions(workflowID)

			if err != nil {
				http.Error(w, "list workflow release versions failed: "+err.Error(), http.StatusBadRequest)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":        true,
				"versions":  versions,
				"trace_id":  traceID,
				"tenant_id": effectiveTenantFromContext(ctx),
			})
		case http.MethodPost:
			var body WorkflowReleaseVersion
			dec := json.NewDecoder(r.Body)
			dec.DisallowUnknownFields()

			if err := dec.Decode(&body); err != nil {
				http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
				return
			}

			if err := runner.SaveWorkflowReleaseVersion(body); err != nil {
				http.Error(w, "save workflow release version failed: "+err.Error(), http.StatusBadRequest)
				return
			}

			version, _, _ := runner.GetWorkflowReleaseVersion(body.ID)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":       true,
				"version":  version,
				"trace_id": traceID,
			})
		}
	})

	mux.HandleFunc("/debug/workflows/releases/version", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		id := strings.TrimSpace(r.URL.Query().Get("id"))
		version, ok, err := runner.GetWorkflowReleaseVersion(id)

		if err != nil {
			http.Error(w, "get workflow release version failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		if !ok {
			http.Error(w, "workflow release version not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":       true,
			"version":  version,
			"trace_id": traceID,
		})
	})

	mux.HandleFunc("/debug/workflows/releases/channel", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodPost:
		default:
			w.Header().Set("Allow", http.MethodGet+", "+http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)

		switch r.Method {
		case http.MethodGet:
			workflowID := strings.TrimSpace(r.URL.Query().Get("workflow_id"))
			env := WorkflowEnvironment(strings.TrimSpace(r.URL.Query().Get("environment")))
			channel, ok, err := runner.GetWorkflowReleaseChannel(workflowID, env)

			if err != nil {
				http.Error(w, "get workflow release channel failed: "+err.Error(), http.StatusBadRequest)
				return
			}

			if !ok {
				http.Error(w, "workflow release channel not found", http.StatusNotFound)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":       true,
				"channel":  channel,
				"trace_id": traceID,
			})
		case http.MethodPost:
			var body WorkflowReleaseChannel
			dec := json.NewDecoder(r.Body)
			dec.DisallowUnknownFields()

			if err := dec.Decode(&body); err != nil {
				http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
				return
			}

			if err := runner.SaveWorkflowReleaseChannel(body); err != nil {
				http.Error(w, "save workflow release channel failed: "+err.Error(), http.StatusBadRequest)
				return
			}

			channel, _, _ := runner.GetWorkflowReleaseChannel(body.WorkflowID, body.Environment)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":       true,
				"channel":  channel,
				"trace_id": traceID,
			})
		}
	})

	mux.HandleFunc("/debug/workflows/releases/promote", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)
		var body struct {
			WorkflowID  string              `json:"workflow_id"`
			Environment WorkflowEnvironment `json:"environment"`
			VersionID   string              `json:"version_id"`
		}

		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()

		if err := dec.Decode(&body); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}

		channel, err := runner.PromoteWorkflowRelease(ctx, body.WorkflowID, body.Environment, body.VersionID)
		if err != nil {
			http.Error(w, "promote workflow release failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":       true,
			"channel":  channel,
			"trace_id": traceID,
		})
	})

	mux.HandleFunc("/debug/workflows/releases/rollback", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)

		var body struct {
			WorkflowID  string              `json:"workflow_id"`
			Environment WorkflowEnvironment `json:"environment"`
			VersionID   string              `json:"version_id,omitempty"`
		}

		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()

		if err := dec.Decode(&body); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}

		channel, err := runner.RollbackWorkflowRelease(ctx, body.WorkflowID, body.Environment, body.VersionID)
		if err != nil {
			http.Error(w, "rollback workflow release failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":       true,
			"channel":  channel,
			"trace_id": traceID,
		})
	})

	mux.HandleFunc("/debug/workflows/releases/diff", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		workflowID := strings.TrimSpace(r.URL.Query().Get("workflow_id"))
		fromVersionID := strings.TrimSpace(r.URL.Query().Get("from_version_id"))
		toVersionID := strings.TrimSpace(r.URL.Query().Get("to_version_id"))
		diff, err := runner.DiffWorkflowReleaseVersions(workflowID, fromVersionID, toVersionID)

		if err != nil {
			http.Error(w, "diff workflow release versions failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":       true,
			"diff":     diff,
			"trace_id": traceID,
		})
	})

	mux.HandleFunc("/debug/workflows/releases/resolve", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		workflowID := strings.TrimSpace(r.URL.Query().Get("workflow_id"))
		env := WorkflowEnvironment(strings.TrimSpace(r.URL.Query().Get("environment")))
		runID := strings.TrimSpace(r.URL.Query().Get("run_id"))
		resolution, err := runner.ResolveWorkflowRelease(workflowID, env, runID)

		if err != nil {
			http.Error(w, "resolve workflow release failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":         true,
			"resolution": resolution,
			"trace_id":   traceID,
		})
	})

	mux.HandleFunc("/debug/workflows/releases/finalize-canary", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)

		var body struct {
			EvalRunID   string              `json:"eval_run_id"`
			WorkflowID  string              `json:"workflow_id"`
			Environment WorkflowEnvironment `json:"environment"`
		}

		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()

		if err := dec.Decode(&body); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}

		channel, action, err := runner.FinalizeWorkflowCanaryWithEvalGate(ctx, body.EvalRunID, body.WorkflowID, body.Environment)
		if err != nil {
			http.Error(w, "finalize canary failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":       true,
			"action":   action,
			"channel":  channel,
			"trace_id": traceID,
		})
	})
}
