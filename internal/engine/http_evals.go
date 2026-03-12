package engine

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

func attachEvalRoutes(mux *http.ServeMux, runner *Runner) {
	mux.HandleFunc("/debug/evals/evaluators", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		names := AvailableEvalEvaluators()
		values := make([]string, 0, len(names))
		for _, name := range names {
			values = append(values, string(name))
		}
		sort.Strings(values)

		writeEvalJSON(w, map[string]any{
			"ok":         true,
			"evaluators": values,
		})
	})

	mux.HandleFunc("/debug/evals/datasets", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			items, err := runner.ListEvalDatasets()
			if err != nil {
				http.Error(w, "list datasets failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			writeEvalJSON(w, map[string]any{
				"ok":       true,
				"count":    len(items),
				"datasets": items,
			})

		case http.MethodPost:
			var dataset EvalDataset
			dec := json.NewDecoder(r.Body)
			dec.DisallowUnknownFields()
			if err := dec.Decode(&dataset); err != nil {
				http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
				return
			}

			if err := runner.SaveEvalDataset(dataset); err != nil {
				http.Error(w, "save dataset failed: "+err.Error(), http.StatusBadRequest)
				return
			}
			writeEvalJSON(w, map[string]any{
				"ok":         true,
				"dataset_id": dataset.ID,
			})

		default:
			w.Header().Set("Allow", "GET, POST")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/debug/evals/dataset", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id := strings.TrimSpace(r.URL.Query().Get("id"))
		if id == "" {
			http.Error(w, "id is required", http.StatusBadRequest)
			return
		}

		dataset, ok, err := runner.GetEvalDataset(id)
		if err != nil {
			http.Error(w, "get dataset failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			http.Error(w, "dataset not found", http.StatusNotFound)
			return
		}

		writeEvalJSON(w, map[string]any{
			"ok":      true,
			"dataset": dataset,
		})
	})

	mux.HandleFunc("/debug/evals/suites", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			items, err := runner.ListEvalSuites()
			if err != nil {
				http.Error(w, "list suites failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			writeEvalJSON(w, map[string]any{
				"ok":     true,
				"count":  len(items),
				"suites": items,
			})

		case http.MethodPost:
			var suite EvalSuite
			dec := json.NewDecoder(r.Body)
			dec.DisallowUnknownFields()
			if err := dec.Decode(&suite); err != nil {
				http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
				return
			}

			if err := runner.SaveEvalSuite(suite); err != nil {
				http.Error(w, "save suite failed: "+err.Error(), http.StatusBadRequest)
				return
			}
			writeEvalJSON(w, map[string]any{
				"ok":       true,
				"suite_id": suite.ID,
			})

		default:
			w.Header().Set("Allow", "GET, POST")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/debug/evals/suite", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id := strings.TrimSpace(r.URL.Query().Get("id"))
		if id == "" {
			http.Error(w, "id is required", http.StatusBadRequest)
			return
		}

		suite, ok, err := runner.GetEvalSuite(id)
		if err != nil {
			http.Error(w, "get suite failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			http.Error(w, "suite not found", http.StatusNotFound)
			return
		}

		writeEvalJSON(w, map[string]any{
			"ok":    true,
			"suite": suite,
		})
	})

	mux.HandleFunc("/debug/evals/runs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		items, err := runner.ListEvalReports()
		if err != nil {
			http.Error(w, "list eval runs failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		limit := len(items)
		if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				http.Error(w, "limit must be a positive int", http.StatusBadRequest)
				return
			}
			if n < limit {
				limit = n
			}
		}
		if limit < len(items) {
			items = items[:limit]
		}

		writeEvalJSON(w, map[string]any{
			"ok":        true,
			"count":     len(items),
			"eval_runs": items,
		})
	})

	mux.HandleFunc("/debug/evals/run", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			id := strings.TrimSpace(r.URL.Query().Get("id"))
			if id == "" {
				http.Error(w, "id is required", http.StatusBadRequest)
				return
			}

			report, ok, err := runner.GetEvalReport(id)
			if err != nil {
				http.Error(w, "get eval run failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			if !ok {
				http.Error(w, "eval run not found", http.StatusNotFound)
				return
			}

			writeEvalJSON(w, map[string]any{
				"ok":       true,
				"eval_run": report,
			})

		case http.MethodPost:
			var req EvalRunRequest
			dec := json.NewDecoder(r.Body)
			dec.DisallowUnknownFields()
			if err := dec.Decode(&req); err != nil {
				http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
				return
			}

			report, err := runner.RunEvalSuite(r.Context(), req)
			if err != nil {
				http.Error(w, "eval run failed: "+err.Error(), http.StatusBadRequest)
				return
			}

			writeEvalJSON(w, map[string]any{
				"ok":       true,
				"eval_run": report,
			})

		default:
			w.Header().Set("Allow", "GET, POST")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/debug/evals/case-from-run", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var body struct {
			DatasetID string `json:"dataset_id"`
			CaseID    string `json:"case_id"`
			Name      string `json:"name"`
			RunID     string `json:"run_id"`
			NodeID    string `json:"node_id"`
		}
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}

		dataset, evalCase, err := runner.CreateEvalCaseFromRun(
			strings.TrimSpace(body.DatasetID),
			strings.TrimSpace(body.CaseID),
			strings.TrimSpace(body.Name),
			strings.TrimSpace(body.RunID),
			strings.TrimSpace(body.NodeID),
		)
		if err != nil {
			http.Error(w, "create eval case failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		writeEvalJSON(w, map[string]any{
			"ok":         true,
			"dataset_id": dataset.ID,
			"case":       evalCase,
		})
	})

	mux.HandleFunc("/debug/evals/promote", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var body struct {
			EvalRunID string `json:"eval_run_id"`
			RunID     string `json:"run_id"`
			Version   string `json:"version"`
		}
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}

		active, err := runner.PromoteRunWithEvalGate(
			strings.TrimSpace(body.EvalRunID),
			strings.TrimSpace(body.RunID),
			strings.TrimSpace(body.Version),
		)
		if err != nil {
			if errors.Is(err, ErrRunNotFound) {
				http.Error(w, "run not found", http.StatusNotFound)
				return
			}
			http.Error(w, "promotion gate failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		writeEvalJSON(w, map[string]any{
			"ok":             true,
			"active_version": active,
		})
	})
}

func writeEvalJSON(w http.ResponseWriter, payload any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(payload)
}
