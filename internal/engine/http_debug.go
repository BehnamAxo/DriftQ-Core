package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
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

	// run a demo 2-node workflow to generate metrics quickly
	mux.HandleFunc("/debug/run-demo", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Pull trace_id from the incoming request (set by middleware) and if it's missing, generate one so logs still correlate
		traceID := strings.TrimSpace(r.Header.Get("X-Trace-Id"))
		if traceID == "" {
			traceID = NewTraceID()
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

		// IMPORTANT: run with engine trace context so runner logs use the same trace_id
		ctx := WithTraceID(r.Context(), traceID)

		err := runner.RunWorkflow(ctx, runID, wf, initial)
		if err != nil {
			http.Error(w, "run failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":       true,
			"run_id":   runID,
			"trace_id": traceID,
		})
	})

	// Body is:
	// {
	//   "run_id": "optional",
	//   "spec": { ...workflow spec json... },
	//   "input": { ...any json... }
	// }
	mux.HandleFunc("/debug/run-spec", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := strings.TrimSpace(r.Header.Get("X-Trace-Id"))
		if traceID == "" {
			traceID = NewTraceID()
		}

		var body struct {
			RunID string          `json:"run_id"`
			Spec  json.RawMessage `json:"spec"`
			Input json.RawMessage `json:"input"`
		}

		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}

		if len(body.Spec) == 0 {
			http.Error(w, "spec is required", http.StatusBadRequest)
			return
		}

		if len(body.Input) == 0 {
			body.Input = json.RawMessage(`{}`)
		}

		runID := strings.TrimSpace(body.RunID)
		if runID == "" {
			runID = "spec-" + time.Now().UTC().Format("20060102T150405.000000000Z")
		}

		reg := runner.HandlerRegistry()
		if reg == nil {
			http.Error(w, "no handler registry configured on runner", http.StatusInternalServerError)
			return
		}

		ctx := WithTraceID(r.Context(), traceID)

		if err := runner.RunSpecJSON(ctx, runID, body.Spec, reg, body.Input); err != nil {
			http.Error(w, "run failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":       true,
			"run_id":   runID,
			"trace_id": traceID,
		})
	})
}
