package engine

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type nodeStatusRow struct {
	NodeID      string     `json:"node_id"`
	Attempt     int        `json:"attempt"`
	Status      string     `json:"status"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	EndedAt     *time.Time `json:"ended_at,omitempty"`
	Error       string     `json:"error,omitempty"`
	HasInput    bool       `json:"has_input"`
	HasOutput   bool       `json:"has_output"`
	InputBytes  int        `json:"input_bytes,omitempty"`
	OutputBytes int        `json:"output_bytes,omitempty"`
}

func traceIDFromRequest(req *http.Request) string {
	tid := strings.TrimSpace(req.Header.Get("X-Trace-Id"))
	if tid == "" {
		tid = NewTraceID()
	}

	return tid
}

// This is runner-only
func AttachDebugRoutes(mux *http.ServeMux, runner *Runner) {
	// snapshot internal runner metrics (this is in-memory metrics, not Prometheus)
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

	// run a tiny 2-node DAG so you can generate runs/events/metrics quickly
	mux.HandleFunc("/debug/run-demo", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := WithTraceID(r.Context(), traceID)

		nodeA := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			var m map[string]int
			if err := json.Unmarshal(input, &m); err != nil {
				return nil, err
			}
			time.Sleep(25 * time.Millisecond)
			m["x"] = m["x"] + 1
			return json.Marshal(m)
		}

		nodeB := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			var m map[string]int
			if err := json.Unmarshal(input, &m); err != nil {
				return nil, err
			}
			time.Sleep(40 * time.Millisecond)
			m["x"] = m["x"] * 2
			return json.Marshal(m)
		}

		g := WorkflowGraph{
			ID: "wf_demo",
			Nodes: []NodeDef{
				{NodeID: "A", Run: nodeA},
				{NodeID: "B", Run: nodeB},
			},
			Edges: []NodeEdge{
				{From: "A", To: "B"},
			},
		}

		runID := "demo-" + time.Now().UTC().Format("20060102T150405.000000000Z")
		initial := json.RawMessage(`{"x":1}`)

		if err := runner.RunDAG(ctx, runID, g, initial); err != nil {
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

	// Body:
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

		traceID := traceIDFromRequest(r)

		var body struct {
			RunID string          `json:"run_id"`
			Spec  json.RawMessage `json:"spec"`
			Input json.RawMessage `json:"input"`
		}

		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
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

	// Body: { "run_id": "...", "reason": "optional" }
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

		traceID := traceIDFromRequest(r)
		ctx := WithTraceID(r.Context(), traceID)

		if err := runner.CancelRun(ctx, body.RunID, body.Reason); err != nil {
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

	// Big dump: run + raw node executions + events + timers (for deep debugging)
	mux.HandleFunc("/debug/run-state", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		runID := strings.TrimSpace(r.URL.Query().Get("run_id"))
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
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(resp)
	})

	// Small/fast summary: run + per-node status rows (this is what your CLI should use)
	mux.HandleFunc("/debug/run", runner.handleDebugRun)

	mux.HandleFunc("/debug/artifact-meta", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		artifactID := strings.TrimSpace(r.URL.Query().Get("artifact_id"))
		if artifactID == "" {
			http.Error(w, "missing artifact_id", http.StatusBadRequest)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := WithTraceID(r.Context(), traceID)

		_, meta, err := runner.GetArtifact(ctx, artifactID)
		if err != nil {
			if errors.Is(err, ErrInvalidArtifactID) {
				http.Error(w, "invalid artifact id", http.StatusBadRequest)
				return
			}

			if errors.Is(err, ErrArtifactNotFound) {
				http.Error(w, "artifact not found", http.StatusNotFound)
				return
			}

			if errors.Is(err, ErrArtifactStoreUnset) {
				http.Error(w, "artifact store not configured", http.StatusBadRequest)
				return
			}

			http.Error(w, "get artifact meta failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{
			"ok":          true,
			"artifact_id": artifactID,
			"meta":        meta,
			"trace_id":    traceID,
		})
	})

	mux.HandleFunc("/debug/artifact-get", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		artifactID := strings.TrimSpace(r.URL.Query().Get("artifact_id"))
		if artifactID == "" {
			http.Error(w, "missing artifact_id", http.StatusBadRequest)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := WithTraceID(r.Context(), traceID)

		b, meta, err := runner.GetArtifact(ctx, artifactID)
		if err != nil {
			if errors.Is(err, ErrInvalidArtifactID) {
				http.Error(w, "invalid artifact id", http.StatusBadRequest)
				return
			}

			if errors.Is(err, ErrArtifactNotFound) {
				http.Error(w, "artifact not found", http.StatusNotFound)
				return
			}

			if errors.Is(err, ErrArtifactStoreUnset) {
				http.Error(w, "artifact store not configured", http.StatusBadRequest)
				return
			}

			http.Error(w, "get artifact failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		ct := strings.TrimSpace(meta.ContentType)
		if ct == "" {
			ct = "application/octet-stream"
		}

		w.Header().Set("Content-Type", ct)
		w.Header().Set("X-Artifact-Id", meta.ArtifactID)
		w.Header().Set("X-Artifact-Sha256", meta.Sha256)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(b)
	})

	// (Optional helper endpoint: I can delete this later)
	mux.HandleFunc("/debug/topics-create", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		name := strings.TrimSpace(r.URL.Query().Get("name"))
		if name == "" {
			http.Error(w, "missing name", http.StatusBadRequest)
			return
		}

		partitions := 1
		if p := strings.TrimSpace(r.URL.Query().Get("partitions")); p != "" {
			fs := flag.NewFlagSet("topics-create", flag.ContinueOnError)
			pi := fs.Int("partitions", 1, "")
			_ = fs.Parse([]string{"-partitions=" + p})
			partitions = *pi
			if partitions < 1 {
				http.Error(w, "partitions must be >= 1", http.StatusBadRequest)
				return
			}
		}

		// This endpoint is only here to help test the CLI
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":         true,
			"name":       name,
			"partitions": partitions,
			"note":       "use the real /v1/topics endpoint via driftqctl; this is just a helper",
		})
	})
}

// This is broker-only and it mounts debug-only topic inspection routes
func AttachTopicDebugRoutes(mux *http.ServeMux, b any) {
	type topicLister interface {
		ListTopics() ([]string, error)
	}

	type topicCounter interface {
		TopicCount(topic string) (int64, error)
	}

	type topicPeeker interface {
		Peek(topic string, limit int) ([]any, error)
	}

	mux.HandleFunc("/debug/topics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		li, ok := b.(topicLister)
		if !ok {
			http.Error(w, "broker does not support ListTopics()", http.StatusNotImplemented)
			return
		}

		topics, err := li.ListTopics()
		if err != nil {
			http.Error(w, "list topics failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		out := make([]map[string]any, 0, len(topics))
		for _, t := range topics {
			row := map[string]any{"topic": t}

			if c, ok := b.(topicCounter); ok {
				if n, err := c.TopicCount(t); err == nil {
					row["messages"] = n
				}
			}

			out = append(out, row)
		}

		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{"ok": true, "topics": out})
	})

	mux.HandleFunc("/debug/topics/peek", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		topic := strings.TrimSpace(r.URL.Query().Get("topic"))
		if topic == "" {
			http.Error(w, "missing topic", http.StatusBadRequest)
			return
		}

		limit := 10
		if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				limit = n
			}
		}

		if limit < 1 {
			limit = 1
		}

		if limit > 100 {
			limit = 100
		}

		pk, ok := b.(topicPeeker)
		if !ok {
			http.Error(w, "peek not supported by broker", http.StatusNotImplemented)
			return
		}

		msgs, err := pk.Peek(topic, limit)
		if err != nil {
			http.Error(w, "peek failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{
			"ok":       true,
			"topic":    topic,
			"limit":    limit,
			"messages": msgs,
		})
	})
}

func (r *Runner) handleDebugRun(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	runID := strings.TrimSpace(req.URL.Query().Get("run_id"))
	if runID == "" {
		http.Error(w, "missing run_id", http.StatusBadRequest)
		return
	}

	run, ok := r.store.GetRun(runID)
	if !ok {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}

	nodes := r.store.ListNodeExecutions(runID)

	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].NodeID == nodes[j].NodeID {
			return nodes[i].Attempt < nodes[j].Attempt
		}
		return nodes[i].NodeID < nodes[j].NodeID
	})

	out := make([]nodeStatusRow, 0, len(nodes))
	for _, ne := range nodes {
		out = append(out, nodeStatusRow{
			NodeID:      ne.NodeID,
			Attempt:     ne.Attempt,
			Status:      string(ne.Status),
			StartedAt:   ne.StartedAt,
			EndedAt:     ne.EndedAt,
			Error:       ne.Error,
			HasInput:    len(ne.Input) > 0,
			HasOutput:   len(ne.Output) > 0,
			InputBytes:  len(ne.Input),
			OutputBytes: len(ne.Output),
		})
	}

	resp := map[string]any{
		"ok":    true,
		"run":   run,
		"nodes": out,
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(resp)
}
