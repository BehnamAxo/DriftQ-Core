package engine

import (
	"context"
	"encoding/json"
	"errors"
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
			// If the run was canceled, that's not a "bad request". so we treat it as a successful control outcome
			if errors.Is(err, ErrRunCanceled) || errors.Is(err, context.Canceled) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"ok":       true,
					"canceled": true,
					"run_id":   runID,
					"trace_id": traceID,
				})
				return
			}

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

	// Body: { "run_id": "..." , "reason": "optional reason" }
	mux.HandleFunc("/debug/run-cancel", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var body struct {
			RunID  string `json:"run_id"`
			Reason string `json:"reason"`
		}

		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}

		body.RunID = strings.TrimSpace(body.RunID)
		body.Reason = strings.TrimSpace(body.Reason)
		if body.RunID == "" {
			http.Error(w, "run_id is required", http.StatusBadRequest)
			return
		}

		traceID := strings.TrimSpace(r.Header.Get("X-Trace-Id"))
		if traceID == "" {
			traceID = NewTraceID()
		}

		ctx := WithTraceID(r.Context(), traceID)

		if err := runner.CancelRun(ctx, body.RunID, body.Reason); err != nil {
			// Treat not-found as 404; everything else as 400 for now.
			if errors.Is(err, ErrRunNotFound) {
				http.Error(w, "run not found", http.StatusNotFound)
				return
			}
			http.Error(w, "cancel failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":       true,
			"run_id":   body.RunID,
			"trace_id": traceID,
			"canceled": true,
		})
	})

	mux.HandleFunc("/debug/run-state", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		runID := r.URL.Query().Get("run_id")
		if runID == "" {
			http.Error(w, "missing run_id", http.StatusBadRequest)
			return
		}

		run, ok := runner.store.GetRun(runID)
		if !ok {
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}

		resp := map[string]any{
			"ok":     true,
			"run":    run,
			"nodes":  runner.store.ListNodeExecutions(runID),
			"events": runner.store.ListEvents(runID),
			"timers": runner.store.ListTimers(runID),
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
}
