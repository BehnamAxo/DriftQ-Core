package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// Adds minimal observability endpoints
func AttachDebugRoutes(mux *http.ServeMux, runner *Runner) {
	mux.HandleFunc("/debug/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		snap := runner.MetricsSnapshot()

		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(snap)
	})

	// NEW: run a demo 2-node workflow to generate metrics quickly
	mux.HandleFunc("/debug/run-demo", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Node A: x = x + 1
		nodeA := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			var m map[string]int
			if err := json.Unmarshal(input, &m); err != nil {
				return nil, err
			}
			time.Sleep(25 * time.Millisecond) // simulate work
			m["x"] = m["x"] + 1
			return json.Marshal(m)
		}

		// Node B: x = x * 2
		nodeB := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			var m map[string]int
			if err := json.Unmarshal(input, &m); err != nil {
				return nil, err
			}
			time.Sleep(40 * time.Millisecond) // simulate work
			m["x"] = m["x"] * 2
			return json.Marshal(m)
		}

		wf := Workflow{
			WorkflowID: "wf_demo",
			Nodes: []NodeDef{
				{NodeID: "A", Run: nodeA},
				{NodeID: "B", Run: nodeB},
			},
		}

		runID := "demo-" + time.Now().UTC().Format("20060102T150405.000000000Z")
		initial := json.RawMessage(`{"x":1}`)

		err := runner.RunWorkflow(r.Context(), runID, wf, initial)
		if err != nil {
			http.Error(w, "run failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// Return a small response so you can inspect via other endpoints later (future).
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":     true,
			"run_id": runID,
		})
	})
}
