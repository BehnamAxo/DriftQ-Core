package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/debugtypes"
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

type runArtifactLister interface {
	ListByRun(ctx context.Context, runID string, limit int) ([]ArtifactMeta, error)
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
	mux.HandleFunc("/debug/run-artifacts", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := WithTraceID(r.Context(), traceID)

		runID := strings.TrimSpace(r.URL.Query().Get("run_id"))
		if runID == "" {
			http.Error(w, "run_id is required", http.StatusBadRequest)
			return
		}

		limit := 200
		if s := strings.TrimSpace(r.URL.Query().Get("limit")); s != "" {
			n, err := strconv.Atoi(s)
			if err != nil || n <= 0 {
				http.Error(w, "limit must be a positive int", http.StatusBadRequest)
				return
			}
			limit = n
		}

		store, err := runner.getArtifactStore()
		if err != nil {
			if errors.Is(err, ErrArtifactStoreUnset) {
				http.Error(w, "artifact store not configured", http.StatusBadRequest)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		l, ok := store.(runArtifactLister)
		if !ok {
			http.Error(w, fmt.Sprintf("artifact store does not support listing (type=%T)", store), http.StatusNotImplemented)
			return
		}

		items, err := l.ListByRun(ctx, runID, limit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":        true,
			"run_id":    runID,
			"count":     len(items),
			"artifacts": items,
			"trace_id":  traceID,
		})
	})

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

	// POST /debug/run-demo
	// Optional input:
	//   - query: ?x=5
	//   - JSON body: {"x":5}
	mux.HandleFunc("/debug/run-demo", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := WithTraceID(r.Context(), traceID)

		type demoPayload struct {
			X int `json:"x"`
		}

		// default input
		x := 1

		// query overrides
		if qs := strings.TrimSpace(r.URL.Query().Get("x")); qs != "" {
			n, err := strconv.Atoi(qs)
			if err != nil {
				http.Error(w, "x must be an int", http.StatusBadRequest)
				return
			}
			x = n
		} else {
			// optional JSON body
			var p demoPayload
			dec := json.NewDecoder(r.Body)
			dec.DisallowUnknownFields()
			if err := dec.Decode(&p); err != nil && !errors.Is(err, io.EOF) {
				http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
				return
			}
			// if body present, use it (even if x=0)
			if !errors.Is(dec.Decode(&struct{}{}), io.EOF) {
				// if there's extra junk after the first object
				// NOTE: Decoder's state is weird after second decode; easiest is to just reject.
				// But we already attempted a second decode; if it's not EOF, it's extra tokens.
				http.Error(w, "bad json: trailing data", http.StatusBadRequest)
				return
			}
			// If the first decode succeeded (not EOF), use p.X
			// We detect that by checking ContentLength/Body read is messy; easiest: trust that if p != zero value and body existed.
			// But p.X may legitimately be 0, so we need a safer approach.
			// The simplest: if ContentLength != 0, assume caller intended a body.
			if r.ContentLength != 0 {
				x = p.X
			}
		}

		// IMPORTANT: define runID before node funcs so they can capture it
		runID := "demo-" + time.Now().UTC().Format("20060102T150405.000000000Z")

		initialBytes, _ := json.Marshal(demoPayload{X: x})
		initial := json.RawMessage(initialBytes)

		nodeA := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			var p demoPayload
			if err := json.Unmarshal(input, &p); err != nil {
				return nil, err
			}

			time.Sleep(25 * time.Millisecond)
			p.X++

			out, err := json.Marshal(p)
			if err != nil {
				return nil, err
			}

			// write artifact (fail the node if artifact store isn't configured)
			_, _, err = runner.PutArtifact(ctx, out, ArtifactMeta{
				ContentType:  "application/json",
				RunID:        runID,
				WorkflowID:   "wf_demo",
				NodeID:       "A",
				Attempt:      1,
				OriginalName: "A.output.json",
				Labels:       map[string]string{"kind": "node_output"},
				Description:  "demo node A output",
			})
			if err != nil {
				return nil, err
			}

			return out, nil
		}

		nodeB := func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			var p demoPayload
			if err := json.Unmarshal(input, &p); err != nil {
				return nil, err
			}

			time.Sleep(40 * time.Millisecond)
			p.X *= 2

			out, err := json.Marshal(p)
			if err != nil {
				return nil, err
			}

			_, _, err = runner.PutArtifact(ctx, out, ArtifactMeta{
				ContentType:  "application/json",
				RunID:        runID,
				WorkflowID:   "wf_demo",
				NodeID:       "B",
				Attempt:      1,
				OriginalName: "B.output.json",
				Labels:       map[string]string{"kind": "node_output"},
				Description:  "demo node B output",
			})
			if err != nil {
				return nil, err
			}

			return out, nil
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

		if err := runner.RunDAG(ctx, runID, g, initial); err != nil {
			// Make the common failure mode obvious
			if errors.Is(err, ErrArtifactStoreUnset) {
				http.Error(w, "run failed: artifact store not configured", http.StatusBadRequest)
				return
			}
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

	// List runs (newest first), GET /debug/runs?limit=50
	mux.HandleFunc("/debug/runs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		limit := 50
		if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				limit = n
			}
		}

		if limit < 1 {
			limit = 1
		}
		if limit > 500 {
			limit = 500
		}

		ids := runner.store.ListRuns()
		if len(ids) > limit {
			ids = ids[:limit]
		}

		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{
			"ok":    true,
			"count": len(ids),
			"runs":  ids,
		})
	})

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
			n, err := strconv.Atoi(p)
			if err != nil || n < 1 {
				http.Error(w, "partitions must be >= 1", http.StatusBadRequest)
				return
			}
			partitions = n
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
	// (UNCHANGED BELOW)
	type topicLister interface {
		ListTopics() ([]string, error)
	}
	type topicCounter interface {
		TopicCount(topic string) (int64, error)
	}
	type topicPeeker interface {
		Peek(topic string, limit int) ([]any, error)
	}
	type topicListerCtx interface {
		ListTopics(ctx context.Context) ([]string, error)
	}
	type topicCounterCtx interface {
		TopicCount(ctx context.Context, topic string) (int64, error)
	}

	type LagRow = debugtypes.ConsumerLagRow

	type consumerLagInspector interface {
		ConsumerLag(ctx context.Context, group string, topic string) ([]debugtypes.ConsumerLagRow, error)
	}

	mux.HandleFunc("/debug/topics/lag", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		group := strings.TrimSpace(r.URL.Query().Get("group"))
		if group == "" {
			http.Error(w, "missing group", http.StatusBadRequest)
			return
		}

		topic := strings.TrimSpace(r.URL.Query().Get("topic")) // optional

		// optional partition filter
		partFilter := (*int)(nil)
		if v := strings.TrimSpace(r.URL.Query().Get("partition")); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				http.Error(w, "invalid partition", http.StatusBadRequest)
				return
			}
			partFilter = &n
		}

		li, ok := b.(consumerLagInspector)
		if !ok {
			http.Error(w, "lag not supported by broker", http.StatusNotImplemented)
			return
		}

		rows, err := li.ConsumerLag(r.Context(), group, topic)
		if err != nil {
			http.Error(w, "lag failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// compute lag + apply partition filter
		out := make([]LagRow, 0, len(rows))
		for _, row := range rows {
			if partFilter != nil && row.Partition != *partFilter {
				continue
			}

			lag := row.HeadOffset - row.CommittedOffset
			if lag < 0 {
				lag = 0
			}
			row.Lag = lag
			out = append(out, row)
		}

		// stable output order
		sort.Slice(out, func(i, j int) bool {
			if out[i].Topic != out[j].Topic {
				return out[i].Topic < out[j].Topic
			}
			if out[i].Group != out[j].Group {
				return out[i].Group < out[j].Group
			}
			return out[i].Partition < out[j].Partition
		})

		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{
			"ok":    true,
			"group": group,
			"topic": topic,
			"rows":  out,
		})
	})

	mux.HandleFunc("/debug/topics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ctx := r.Context()

		var (
			topics []string
			err    error
		)

		if li, ok := b.(topicListerCtx); ok {
			topics, err = li.ListTopics(ctx)
		} else if li, ok := b.(topicLister); ok {
			topics, err = li.ListTopics()
		} else {
			http.Error(w, "broker does not support ListTopics()", http.StatusNotImplemented)
			return
		}

		if err != nil {
			http.Error(w, "list topics failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		out := make([]map[string]any, 0, len(topics))

		for _, t := range topics {
			row := map[string]any{"topic": t}

			// best-effort counts if supported
			if c, ok := b.(topicCounterCtx); ok {
				if n, err := c.TopicCount(ctx, t); err == nil {
					row["messages"] = n
				}
			} else if c, ok := b.(topicCounter); ok {
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
