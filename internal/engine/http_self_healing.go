package engine

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

func attachSelfHealingRoutes(mux *http.ServeMux, runner *Runner) {
	mux.HandleFunc("/debug/self-heal/artifacts", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)
		runID := strings.TrimSpace(r.URL.Query().Get("run_id"))
		limit := 100

		if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n < 1 {
				http.Error(w, "limit must be a positive int", http.StatusBadRequest)
				return
			}
			limit = n
		}

		artifacts, err := runner.ListSelfHealingArtifacts(ctx, runID, limit)
		if err != nil {
			http.Error(w, "list self-heal artifacts failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":        true,
			"count":     len(artifacts),
			"artifacts": artifacts,
			"trace_id":  traceID,
		})
	})

	mux.HandleFunc("/debug/self-heal/artifact", func(w http.ResponseWriter, r *http.Request) {
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
			artifactID := strings.TrimSpace(r.URL.Query().Get("id"))
			runID := strings.TrimSpace(r.URL.Query().Get("run_id"))

			var (
				artifact SelfHealingArtifact
				ok       bool
				err      error
			)

			if artifactID != "" {
				artifact, ok, err = runner.GetSelfHealingArtifact(ctx, artifactID)
			} else {
				artifact, ok, err = runner.GetSelfHealingArtifactByRun(ctx, runID)
			}

			if err != nil {
				http.Error(w, "get self-heal artifact failed: "+err.Error(), http.StatusInternalServerError)
				return
			}

			if !ok {
				http.Error(w, "self-heal artifact not found", http.StatusNotFound)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":       true,
				"artifact": artifact,
				"trace_id": traceID,
			})

		case http.MethodPost:
			var body struct {
				RunID string `json:"run_id"`
			}

			dec := json.NewDecoder(r.Body)
			dec.DisallowUnknownFields()

			if err := dec.Decode(&body); err != nil {
				http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
				return
			}

			artifact, err := runner.CaptureSelfHealingArtifact(ctx, body.RunID)
			if err != nil {
				http.Error(w, "capture self-heal artifact failed: "+err.Error(), http.StatusBadRequest)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":       true,
				"artifact": artifact,
				"trace_id": traceID,
			})
		}
	})

	mux.HandleFunc("/debug/self-heal/replay", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)

		var body struct {
			RunID string `json:"run_id"`
		}

		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}

		artifact, err := runner.ReplaySelfHealingArtifact(ctx, body.RunID)
		if err != nil {
			http.Error(w, "self-heal replay failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		run, _ := runner.store.GetRun(strings.TrimSpace(body.RunID))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":       true,
			"artifact": artifact,
			"run":      run,
			"trace_id": traceID,
		})
	})
}
