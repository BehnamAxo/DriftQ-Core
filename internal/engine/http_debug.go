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
	attachEvalRoutes(mux, runner)
	attachWorkflowReleaseRoutes(mux, runner)

	mux.HandleFunc("/debug/policy", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
		case http.MethodPost:
		default:
			w.Header().Set("Allow", http.MethodGet+", "+http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)

		switch r.Method {
		case http.MethodGet:
			bundle, ok, err := runner.GetAuthorizationPolicy()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":         true,
				"configured": ok,
				"policy":     bundle,
				"trace_id":   traceID,
			})

		case http.MethodPost:
			var bundle AuthorizationPolicyBundle
			dec := json.NewDecoder(r.Body)
			dec.DisallowUnknownFields()
			if err := dec.Decode(&bundle); err != nil {
				http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
				return
			}

			if err := runner.SaveAuthorizationPolicy(bundle); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			saved, _, err := runner.GetAuthorizationPolicy()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":       true,
				"policy":   saved,
				"trace_id": traceID,
			})
		}
	})

	mux.HandleFunc("/debug/risk-policy", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
		case http.MethodPost:
		default:
			w.Header().Set("Allow", http.MethodGet+", "+http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)

		switch r.Method {
		case http.MethodGet:
			policy, ok, err := runner.GetRiskPolicy()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if !ok {
				policy = defaultRiskPolicy()
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":         true,
				"configured": ok,
				"policy":     policy,
				"trace_id":   traceID,
			})

		case http.MethodPost:
			var policy RiskPolicy
			dec := json.NewDecoder(r.Body)
			dec.DisallowUnknownFields()
			if err := dec.Decode(&policy); err != nil {
				http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
				return
			}

			if err := runner.SaveRiskPolicy(policy); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			saved, _, err := runner.GetRiskPolicy()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":       true,
				"policy":   saved,
				"trace_id": traceID,
			})
		}
	})

	mux.HandleFunc("/debug/tool-gateway", func(w http.ResponseWriter, r *http.Request) {
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
			bundle, ok, err := runner.GetToolGatewayBundle()
			if err != nil {
				http.Error(w, "get tool gateway failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			if !ok {
				bundle = ToolGatewayBundle{}
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":        true,
				"bundle":    bundle,
				"tenant_id": effectiveTenantFromContext(ctx),
				"trace_id":  traceID,
			})

		case http.MethodPost:
			var body ToolGatewayBundle
			dec := json.NewDecoder(r.Body)
			dec.DisallowUnknownFields()
			if err := dec.Decode(&body); err != nil {
				http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
				return
			}

			if err := runner.SaveToolGatewayBundle(body); err != nil {
				http.Error(w, "save tool gateway failed: "+err.Error(), http.StatusBadRequest)
				return
			}

			bundle, _, _ := runner.GetToolGatewayBundle()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":        true,
				"bundle":    bundle,
				"tenant_id": effectiveTenantFromContext(ctx),
				"trace_id":  traceID,
			})
		}
	})

	mux.HandleFunc("/debug/tool-calls", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)
		runID := strings.TrimSpace(r.URL.Query().Get("run_id"))
		tool := strings.TrimSpace(r.URL.Query().Get("tool"))
		limit := 100
		if rawLimit := strings.TrimSpace(r.URL.Query().Get("limit")); rawLimit != "" {
			n, err := strconv.Atoi(rawLimit)
			if err != nil || n < 1 {
				http.Error(w, "limit must be a positive int", http.StatusBadRequest)
				return
			}
			limit = n
		}

		records, err := runner.ListToolCallRecords(ctx, runID, tool, limit)
		if err != nil {
			http.Error(w, "list tool calls failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":        true,
			"count":     len(records),
			"records":   records,
			"tenant_id": effectiveTenantFromContext(ctx),
			"trace_id":  traceID,
		})
	})

	mux.HandleFunc("/debug/side-effects", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)
		runID := strings.TrimSpace(r.URL.Query().Get("run_id"))
		status := SideEffectStatus(strings.TrimSpace(r.URL.Query().Get("status")))
		limit := 100

		if rawLimit := strings.TrimSpace(r.URL.Query().Get("limit")); rawLimit != "" {
			n, err := strconv.Atoi(rawLimit)
			if err != nil || n < 1 {
				http.Error(w, "limit must be a positive int", http.StatusBadRequest)
				return
			}
			limit = n
		}

		receipts, err := runner.ListSideEffectReceipts(ctx, runID, status, limit)
		if err != nil {
			http.Error(w, "list side effects failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":        true,
			"count":     len(receipts),
			"receipts":  receipts,
			"tenant_id": effectiveTenantFromContext(ctx),
			"trace_id":  traceID,
		})
	})

	mux.HandleFunc("/debug/side-effects/commit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)
		var body struct {
			ReceiptID string `json:"receipt_id"`
		}

		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}

		receipt, err := runner.CommitSideEffect(ctx, body.ReceiptID)
		if err != nil {
			var pendingErr *HumanApprovalPendingError
			if errors.As(err, &pendingErr) {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}

			if errors.Is(err, ErrSideEffectReceiptNotFound) {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}

			http.Error(w, "commit side effect failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":        true,
			"receipt":   receipt,
			"tenant_id": effectiveTenantFromContext(ctx),
			"trace_id":  traceID,
		})
	})

	mux.HandleFunc("/debug/side-effects/compensate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)

		var body struct {
			ReceiptID string `json:"receipt_id"`
		}

		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()

		if err := dec.Decode(&body); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}

		receipt, err := runner.CompensateSideEffect(ctx, body.ReceiptID)
		if err != nil {
			if errors.Is(err, ErrSideEffectReceiptNotFound) {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}

			http.Error(w, "compensate side effect failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":        true,
			"receipt":   receipt,
			"tenant_id": effectiveTenantFromContext(ctx),
			"trace_id":  traceID,
		})
	})

	mux.HandleFunc("/debug/human/tasks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)
		runID := strings.TrimSpace(r.URL.Query().Get("run_id"))
		status := HumanTaskStatus(strings.TrimSpace(r.URL.Query().Get("status")))
		limit := 100

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

		tasks, err := runner.ListHumanTasks(runID, status, limit)
		if err != nil {
			http.Error(w, "list human tasks failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		tenantID := effectiveTenantFromContext(ctx)
		filtered := make([]HumanTask, 0, len(tasks))

		for _, task := range tasks {
			if tenantID != "" && task.TenantID != "" && task.TenantID != tenantID {
				continue
			}
			filtered = append(filtered, task)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":        true,
			"count":     len(filtered),
			"tasks":     filtered,
			"tenant_id": tenantID,
			"trace_id":  traceID,
		})
	})

	mux.HandleFunc("/debug/human/respond", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)

		var body struct {
			TaskID      string          `json:"task_id"`
			Decision    HumanDecision   `json:"decision"`
			EditedInput json.RawMessage `json:"edited_input,omitempty"`
			Comment     string          `json:"comment,omitempty"`
			Resume      *bool           `json:"resume,omitempty"`
		}

		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()

		if err := dec.Decode(&body); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}

		resume := true
		if body.Resume != nil {
			resume = *body.Resume
		}

		task, err := runner.ResolveHumanTask(ctx, body.TaskID, body.Decision, body.EditedInput, body.Comment, resume)
		if err != nil {
			if errors.Is(err, ErrHumanTaskNotFound) {
				http.Error(w, "human task not found", http.StatusNotFound)
				return
			}
			if errors.Is(err, ErrHumanTaskResolved) || errors.Is(err, ErrTenantAccessDenied) {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			http.Error(w, "resolve human task failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":       true,
			"task":     task,
			"trace_id": traceID,
		})
	})

	mux.HandleFunc("/debug/run-artifacts", func(w http.ResponseWriter, r *http.Request) {
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

		run, ok := runner.store.GetRun(runID)
		if !ok {
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}

		if err := runner.ensureRunTenantAccess(ctx, run, "debug.run_artifacts"); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
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
		ctx := debugContextFromRequest(r, traceID)

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
			RunID      string          `json:"run_id"`
			Spec       json.RawMessage `json:"spec"`
			Input      json.RawMessage `json:"input"`
			TenantID   string          `json:"tenant_id,omitempty"`
			Principal  *Principal      `json:"principal,omitempty"`
			PolicyMode PolicyMode      `json:"policy_mode,omitempty"`
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

		mode := body.PolicyMode
		if mode == "" {
			mode = PolicyModeEnforce
		}
		if mode != PolicyModeEnforce && mode != PolicyModeSimulate {
			http.Error(w, "policy_mode must be enforce or simulate", http.StatusBadRequest)
			return
		}

		ctx := WithTraceID(r.Context(), traceID)
		if body.Principal != nil {
			body.Principal.Normalize()
			ctx = WithPrincipal(ctx, *body.Principal)
		}

		tenantID := strings.TrimSpace(body.TenantID)
		if tenantID == "" && body.Principal != nil {
			tenantID = body.Principal.TenantID
		}

		if tenantID != "" {
			ctx = WithTenantID(ctx, tenantID)
		}

		reg := runner.HandlerRegistryForTenant(effectiveTenantFromContext(ctx))
		if reg == nil {
			http.Error(w, "no handler registry configured on runner", http.StatusInternalServerError)
			return
		}

		if mode == PolicyModeSimulate {
			authzReport, err := runner.EvaluateRunSpecAuthorization(ctx, runID, body.Spec, reg)
			if err != nil {
				http.Error(w, "authorization simulate failed: "+err.Error(), http.StatusBadRequest)
				return
			}

			authzReport.Mode = PolicyModeSimulate

			riskReport, err := runner.EvaluateRunSpecRisk(ctx, runID, body.Spec, reg, body.Input)
			if err != nil {
				http.Error(w, "risk simulate failed: "+err.Error(), http.StatusBadRequest)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":            true,
				"simulated":     true,
				"run_id":        runID,
				"authorization": authzReport,
				"risk":          riskReport,
				"trace_id":      traceID,
			})
			return
		}

		if err := runner.RunSpecJSON(ctx, runID, body.Spec, reg, body.Input); err != nil {
			var authErr *AuthorizationError
			if errors.As(err, &authErr) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"ok":            false,
					"run_id":        runID,
					"error":         authErr.Error(),
					"authorization": authErr.Report,
					"trace_id":      traceID,
				})
				return
			}

			var riskErr *RiskError
			if errors.As(err, &riskErr) {
				w.Header().Set("Content-Type", "application/json")
				if errors.Is(err, ErrRiskApprovalRequired) {
					w.WriteHeader(http.StatusConflict)
				} else {
					w.WriteHeader(http.StatusForbidden)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"ok":       false,
					"run_id":   runID,
					"error":    riskErr.Error(),
					"risk":     riskErr.Report,
					"trace_id": traceID,
				})
				return
			}

			var humanErr *HumanApprovalPendingError
			if errors.As(err, &humanErr) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusAccepted)

				_ = json.NewEncoder(w).Encode(map[string]any{
					"ok":             false,
					"run_id":         runID,
					"human_approval": true,
					"task":           humanErr.Task,
					"trace_id":       traceID,
				})
				return
			}

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
		ctx := debugContextFromRequest(r, traceID)

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

	// Body: { "run_id": "...", "from_step": "optional", "mode": "time_travel"|"live" }
	mux.HandleFunc("/debug/run-replay", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var body struct {
			RunID    string `json:"run_id"`
			FromStep string `json:"from_step"`
			Mode     string `json:"mode"`
		}

		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}

		body.RunID = strings.TrimSpace(body.RunID)
		body.FromStep = strings.TrimSpace(body.FromStep)
		body.Mode = strings.TrimSpace(body.Mode)
		if body.RunID == "" {
			http.Error(w, "run_id is required", http.StatusBadRequest)
			return
		}

		mode := ReplayTimeTravel
		if body.Mode != "" {
			mode = ReplayMode(body.Mode)
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)

		if err := runner.ReplayFrom(ctx, body.RunID, body.FromStep, mode); err != nil {
			if errors.Is(err, ErrRunNotFound) {
				http.Error(w, "run not found", http.StatusNotFound)
				return
			}

			if errors.Is(err, ErrTenantAccessDenied) {
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}

			http.Error(w, "replay failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":        true,
			"run_id":    body.RunID,
			"from_step": body.FromStep,
			"mode":      string(mode),
			"trace_id":  traceID,
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

		traceID := traceIDFromRequest(r)
		run, ok := runner.store.GetRun(runID)

		if !ok {
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}

		ctx := debugContextFromRequest(r, traceID)
		if err := runner.ensureRunTenantAccess(ctx, run, "debug.run_state"); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
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

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)
		tenantID := effectiveTenantFromContext(ctx)
		ids := runner.store.ListRuns()
		filtered := make([]string, 0, len(ids))

		for _, runID := range ids {
			run, ok := runner.store.GetRun(runID)
			if !ok {
				continue
			}

			if tenantID != "" && strings.TrimSpace(run.TenantID) != tenantID {
				continue
			}

			filtered = append(filtered, runID)
		}

		if len(filtered) > limit {
			filtered = filtered[:limit]
		}

		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{
			"ok":        true,
			"count":     len(filtered),
			"runs":      filtered,
			"tenant_id": tenantID,
		})
	})

	mux.HandleFunc("/debug/audit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		limit := 100
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

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)
		tenantID := effectiveTenantFromContext(ctx)
		runID := strings.TrimSpace(r.URL.Query().Get("run_id"))
		records, err := runner.ListAuditRecords(tenantID, runID, limit)

		if err != nil {
			http.Error(w, "list audit failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{
			"ok":        true,
			"count":     len(records),
			"records":   records,
			"tenant_id": tenantID,
			"run_id":    runID,
		})
	})

	mux.HandleFunc("/debug/agent-state", func(w http.ResponseWriter, r *http.Request) {
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
			agentID := strings.TrimSpace(r.URL.Query().Get("agent_id"))
			if agentID == "" {
				http.Error(w, "agent_id is required", http.StatusBadRequest)
				return
			}

			opts := AgentStateReadOptions{}
			if rawVersion := strings.TrimSpace(r.URL.Query().Get("version")); rawVersion != "" {
				version, err := strconv.Atoi(rawVersion)
				if err != nil || version < 1 {
					http.Error(w, "version must be a positive int", http.StatusBadRequest)
					return
				}
				opts.Version = version
			}

			snapshot, err := runner.ReadAgentState(ctx, agentID, opts)
			if err != nil {
				switch {
				case errors.Is(err, ErrAgentStateNotFound):
					http.Error(w, err.Error(), http.StatusNotFound)
				case errors.Is(err, ErrTenantAccessDenied), errors.Is(err, ErrAgentStateAccessDenied):
					http.Error(w, err.Error(), http.StatusForbidden)
				default:
					http.Error(w, "read agent state failed: "+err.Error(), http.StatusBadRequest)
				}
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":        true,
				"snapshot":  snapshot,
				"tenant_id": effectiveTenantFromContext(ctx),
				"trace_id":  traceID,
			})

		case http.MethodPost:
			var body AgentStateWriteRequest
			dec := json.NewDecoder(r.Body)
			dec.DisallowUnknownFields()
			if err := dec.Decode(&body); err != nil {
				http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
				return
			}

			snapshot, err := runner.WriteAgentState(ctx, body)
			if err != nil {
				switch {
				case errors.Is(err, ErrAgentStateReplayWriteDenied), errors.Is(err, ErrAgentStateVersionConflict):
					http.Error(w, err.Error(), http.StatusConflict)
				case errors.Is(err, ErrTenantAccessDenied), errors.Is(err, ErrAgentStateAccessDenied):
					http.Error(w, err.Error(), http.StatusForbidden)
				default:
					http.Error(w, "write agent state failed: "+err.Error(), http.StatusBadRequest)
				}
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":        true,
				"snapshot":  snapshot,
				"tenant_id": effectiveTenantFromContext(ctx),
				"trace_id":  traceID,
			})
		}
	})

	mux.HandleFunc("/debug/agent-state/lineage", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)
		agentID := strings.TrimSpace(r.URL.Query().Get("agent_id"))
		if agentID == "" {
			http.Error(w, "agent_id is required", http.StatusBadRequest)
			return
		}

		limit := 100
		if rawLimit := strings.TrimSpace(r.URL.Query().Get("limit")); rawLimit != "" {
			n, err := strconv.Atoi(rawLimit)
			if err != nil || n < 1 {
				http.Error(w, "limit must be a positive int", http.StatusBadRequest)
				return
			}
			limit = n
		}

		lineage, err := runner.ListAgentStateLineage(ctx, agentID, limit)
		if err != nil {
			if errors.Is(err, ErrTenantAccessDenied) || errors.Is(err, ErrAgentStateAccessDenied) {
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
			http.Error(w, "list agent state lineage failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":        true,
			"count":     len(lineage),
			"lineage":   lineage,
			"tenant_id": effectiveTenantFromContext(ctx),
			"trace_id":  traceID,
		})
	})

	mux.HandleFunc("/debug/agent-memory", func(w http.ResponseWriter, r *http.Request) {
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
			agentID := strings.TrimSpace(r.URL.Query().Get("agent_id"))
			if agentID == "" {
				http.Error(w, "agent_id is required", http.StatusBadRequest)
				return
			}

			limit := 50
			if rawLimit := strings.TrimSpace(r.URL.Query().Get("limit")); rawLimit != "" {
				n, err := strconv.Atoi(rawLimit)
				if err != nil || n < 1 {
					http.Error(w, "limit must be a positive int", http.StatusBadRequest)
					return
				}
				limit = n
			}

			entries, err := runner.ListAgentMemory(ctx, agentID, limit)
			if err != nil {
				switch {
				case errors.Is(err, ErrTenantAccessDenied), errors.Is(err, ErrAgentMemoryAccessDenied):
					http.Error(w, err.Error(), http.StatusForbidden)
				default:
					http.Error(w, "list agent memory failed: "+err.Error(), http.StatusBadRequest)
				}
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":        true,
				"count":     len(entries),
				"entries":   entries,
				"tenant_id": effectiveTenantFromContext(ctx),
				"trace_id":  traceID,
			})

		case http.MethodPost:
			var body AgentMemoryWriteRequest
			dec := json.NewDecoder(r.Body)
			dec.DisallowUnknownFields()
			if err := dec.Decode(&body); err != nil {
				http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
				return
			}

			entry, err := runner.WriteAgentMemory(ctx, body)
			if err != nil {
				switch {
				case errors.Is(err, ErrAgentMemoryReplayWriteDenied):
					http.Error(w, err.Error(), http.StatusConflict)
				case errors.Is(err, ErrTenantAccessDenied), errors.Is(err, ErrAgentMemoryAccessDenied), errors.Is(err, ErrAgentStateAccessDenied):
					http.Error(w, err.Error(), http.StatusForbidden)
				default:
					http.Error(w, "write agent memory failed: "+err.Error(), http.StatusBadRequest)
				}
				return
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":        true,
				"entry":     entry,
				"tenant_id": effectiveTenantFromContext(ctx),
				"trace_id":  traceID,
			})
		}
	})

	mux.HandleFunc("/debug/agent-memory/search", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)

		var body AgentMemorySearchRequest
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}

		results, err := runner.SearchAgentMemory(ctx, body)
		if err != nil {
			switch {
			case errors.Is(err, ErrTenantAccessDenied), errors.Is(err, ErrAgentMemoryAccessDenied):
				http.Error(w, err.Error(), http.StatusForbidden)
			default:
				http.Error(w, "search agent memory failed: "+err.Error(), http.StatusBadRequest)
			}
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":        true,
			"count":     len(results),
			"results":   results,
			"tenant_id": effectiveTenantFromContext(ctx),
			"trace_id":  traceID,
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
		ctx := debugContextFromRequest(r, traceID)

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
		ctx := debugContextFromRequest(r, traceID)

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

	mux.HandleFunc("/debug/index/active", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ver, ok := GetActiveIndexVersion(runner.store)
		resp := map[string]any{
			"ok":             true,
			"active_version": "",
		}

		if ok {
			resp["active_version"] = ver
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/debug/index/promote", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// support query params OR JSON body
		runID := strings.TrimSpace(r.URL.Query().Get("run_id"))
		version := strings.TrimSpace(r.URL.Query().Get("version"))

		if runID == "" {
			body, _ := io.ReadAll(r.Body)
			_ = r.Body.Close()
			if len(body) > 0 {
				var req struct {
					RunID   string `json:"run_id"`
					Version string `json:"version"`
				}

				_ = json.Unmarshal(body, &req)
				runID = strings.TrimSpace(req.RunID)
				version = strings.TrimSpace(req.Version)
			}
		}

		if runID == "" {
			http.Error(w, "run_id required", http.StatusBadRequest)
			return
		}

		ver, err := PromoteIndexVersion(runner.store, runID, version)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp := map[string]any{"ok": true, "active_version": ver}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/debug/index/rollback", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		version := strings.TrimSpace(r.URL.Query().Get("version"))
		if version == "" {
			body, _ := io.ReadAll(r.Body)
			_ = r.Body.Close()
			if len(body) > 0 {
				var req struct {
					Version string `json:"version"`
				}

				_ = json.Unmarshal(body, &req)
				version = strings.TrimSpace(req.Version)
			}
		}

		if err := RollbackIndexVersion(runner.store, version); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp := map[string]any{"ok": true, "active_version": version}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
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

	type messageStateInspector interface {
		MessageStates(ctx context.Context, group, topic, status, owner string, limit int) ([]debugtypes.MessageStateRow, error)
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

	mux.HandleFunc("/debug/messages/state", func(w http.ResponseWriter, r *http.Request) {
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

		topic := strings.TrimSpace(r.URL.Query().Get("topic"))
		status := strings.TrimSpace(r.URL.Query().Get("status"))
		owner := strings.TrimSpace(r.URL.Query().Get("owner"))

		limit := 100
		if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				http.Error(w, "invalid limit", http.StatusBadRequest)
				return
			}

			limit = n
		}

		mi, ok := b.(messageStateInspector)
		if !ok {
			http.Error(w, "message state not supported by broker", http.StatusNotImplemented)
			return
		}

		rows, err := mi.MessageStates(r.Context(), group, topic, status, owner, limit)
		if err != nil {
			http.Error(w, "message state failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{
			"ok":     true,
			"group":  group,
			"topic":  topic,
			"status": status,
			"owner":  owner,
			"rows":   rows,
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

	traceID := traceIDFromRequest(req)
	ctx := debugContextFromRequest(req, traceID)
	if err := r.ensureRunTenantAccess(ctx, run, "debug.run"); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
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
