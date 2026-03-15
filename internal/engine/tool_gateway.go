package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	ErrToolNotApproved       = errors.New("tool is not approved")
	ErrToolServerNotApproved = errors.New("tool server is not approved")
	ErrToolSchemaInvalid     = errors.New("tool schema validation failed")
	ErrToolCallNotFound      = errors.New("tool call not found")
)

const (
	toolGatewayBundleKey = "tool_gateway/bundle"
	toolCallIndexKey     = "tool_gateway/calls/index"
	defaultToolServerID  = "local"
)

type ToolServerDefinition struct {
	ID             string   `json:"id"`
	Kind           string   `json:"kind,omitempty"`
	Endpoint       string   `json:"endpoint,omitempty"`
	Description    string   `json:"description,omitempty"`
	Approved       bool     `json:"approved"`
	AllowedTools   []string `json:"allowed_tools,omitempty"`
	TenantScopes   []string `json:"tenant_scopes,omitempty"`
	SandboxAllowed bool     `json:"sandbox_allowed,omitempty"`
}

type ToolPolicy struct {
	ID              string                 `json:"id"`
	Tool            string                 `json:"tool"`
	ServerID        string                 `json:"server_id,omitempty"`
	Approved        bool                   `json:"approved"`
	SandboxRequired bool                   `json:"sandbox_required,omitempty"`
	SideEffect      *SideEffectPolicy      `json:"side_effect,omitempty"`
	AdaptiveRouting *AdaptiveRoutingPolicy `json:"adaptive_routing,omitempty"`
	TenantScopes    []string               `json:"tenant_scopes,omitempty"`
	RedactFields    []string               `json:"redact_fields,omitempty"`
	InputSchema     json.RawMessage        `json:"input_schema,omitempty"`
	OutputSchema    json.RawMessage        `json:"output_schema,omitempty"`
}

type ToolGatewayBundle struct {
	Tools     []ToolPolicy           `json:"tools,omitempty"`
	Servers   []ToolServerDefinition `json:"servers,omitempty"`
	UpdatedAt *time.Time             `json:"updated_at,omitempty"`
}

type ToolCallRecord struct {
	ID          string          `json:"id"`
	At          time.Time       `json:"at"`
	TenantID    string          `json:"tenant_id,omitempty"`
	PrincipalID string          `json:"principal_id,omitempty"`
	RunID       string          `json:"run_id,omitempty"`
	WorkflowID  string          `json:"workflow_id,omitempty"`
	NodeID      string          `json:"node_id,omitempty"`
	Attempt     int             `json:"attempt,omitempty"`
	Tool        string          `json:"tool"`
	ServerID    string          `json:"server_id,omitempty"`
	ServerKind  string          `json:"server_kind,omitempty"`
	RouteID     string          `json:"route_id,omitempty"`
	Provider    string          `json:"provider,omitempty"`
	Model       string          `json:"model,omitempty"`
	Allowed     bool            `json:"allowed"`
	Sandboxed   bool            `json:"sandboxed,omitempty"`
	DurationMS  int64           `json:"duration_ms,omitempty"`
	Reason      string          `json:"reason,omitempty"`
	Input       json.RawMessage `json:"input,omitempty"`
	Output      json.RawMessage `json:"output,omitempty"`
	Error       string          `json:"error,omitempty"`
}

type ToolRuntimeContext struct {
	Tool       string               `json:"tool"`
	ServerID   string               `json:"server_id,omitempty"`
	ServerKind string               `json:"server_kind,omitempty"`
	RouteID    string               `json:"route_id,omitempty"`
	Provider   string               `json:"provider,omitempty"`
	Model      string               `json:"model,omitempty"`
	Sandboxed  bool                 `json:"sandboxed,omitempty"`
	Policy     ToolPolicy           `json:"policy"`
	Server     ToolServerDefinition `json:"server"`
}

type AdaptiveRoute struct {
	ID               string   `json:"id"`
	Provider         string   `json:"provider,omitempty"`
	Model            string   `json:"model,omitempty"`
	EstimatedTokens  int64    `json:"estimated_tokens,omitempty"`
	EstimatedDollars float64  `json:"estimated_dollars,omitempty"`
	Priority         int      `json:"priority,omitempty"`
	TenantScopes     []string `json:"tenant_scopes,omitempty"`
}

type AdaptiveRoutingPolicy struct {
	CheapFirst              bool            `json:"cheap_first,omitempty"`
	EscalateOnUncertainty   bool            `json:"escalate_on_uncertainty,omitempty"`
	UncertaintyThreshold    float64         `json:"uncertainty_threshold,omitempty"`
	EscalateOnFailure       bool            `json:"escalate_on_failure,omitempty"`
	FailureAttemptThreshold int             `json:"failure_attempt_threshold,omitempty"`
	EscalateOnRisk          bool            `json:"escalate_on_risk,omitempty"`
	RiskScoreThreshold      int             `json:"risk_score_threshold,omitempty"`
	Routes                  []AdaptiveRoute `json:"routes,omitempty"`
}

type adaptiveRoutingHints struct {
	Uncertainty float64
}

type toolRuntimeCtxKey struct{}

func WithToolRuntime(ctx context.Context, runtime ToolRuntimeContext) context.Context {
	runtime.Tool = strings.TrimSpace(runtime.Tool)
	runtime.ServerID = strings.TrimSpace(runtime.ServerID)
	runtime.ServerKind = strings.TrimSpace(runtime.ServerKind)
	runtime.RouteID = strings.TrimSpace(runtime.RouteID)
	runtime.Provider = strings.TrimSpace(runtime.Provider)
	runtime.Model = strings.TrimSpace(runtime.Model)
	return context.WithValue(ctx, toolRuntimeCtxKey{}, runtime)
}

func ToolRuntimeFrom(ctx context.Context) (ToolRuntimeContext, bool) {
	v := ctx.Value(toolRuntimeCtxKey{})
	runtime, ok := v.(ToolRuntimeContext)

	if !ok {
		return ToolRuntimeContext{}, false
	}

	runtime.Tool = strings.TrimSpace(runtime.Tool)
	runtime.ServerID = strings.TrimSpace(runtime.ServerID)
	runtime.ServerKind = strings.TrimSpace(runtime.ServerKind)
	runtime.RouteID = strings.TrimSpace(runtime.RouteID)
	runtime.Provider = strings.TrimSpace(runtime.Provider)
	runtime.Model = strings.TrimSpace(runtime.Model)

	return runtime, runtime.Tool != ""
}

type toolInvocation struct {
	RunID              string
	WorkflowID         string
	NodeID             string
	Attempt            int
	Tool               string
	RequiredCapability string
	InputSchema        json.RawMessage
	OutputSchema       json.RawMessage
	Handler            NodeFunc
}

func cloneToolGatewayBundle(bundle ToolGatewayBundle) ToolGatewayBundle {
	out := ToolGatewayBundle{
		Tools:   make([]ToolPolicy, 0, len(bundle.Tools)),
		Servers: make([]ToolServerDefinition, 0, len(bundle.Servers)),
	}

	if bundle.UpdatedAt != nil {
		t := bundle.UpdatedAt.UTC()
		out.UpdatedAt = &t
	}

	for _, tool := range bundle.Tools {
		out.Tools = append(out.Tools, cloneToolPolicy(tool))
	}

	for _, server := range bundle.Servers {
		out.Servers = append(out.Servers, cloneToolServerDefinition(server))
	}

	return out
}

func cloneToolPolicy(policy ToolPolicy) ToolPolicy {
	out := policy
	out.ID = strings.TrimSpace(policy.ID)
	out.Tool = strings.TrimSpace(policy.Tool)
	out.ServerID = strings.TrimSpace(policy.ServerID)
	out.SideEffect = cloneSideEffectPolicy(policy.SideEffect)
	out.AdaptiveRouting = cloneAdaptiveRoutingPolicy(policy.AdaptiveRouting)
	out.TenantScopes = append([]string(nil), policy.TenantScopes...)
	out.RedactFields = append([]string(nil), policy.RedactFields...)
	out.InputSchema = cloneRaw(policy.InputSchema)
	out.OutputSchema = cloneRaw(policy.OutputSchema)

	return out
}

func cloneToolServerDefinition(server ToolServerDefinition) ToolServerDefinition {
	out := server
	out.ID = strings.TrimSpace(server.ID)
	out.Kind = strings.TrimSpace(server.Kind)
	out.Endpoint = strings.TrimSpace(server.Endpoint)
	out.Description = strings.TrimSpace(server.Description)
	out.AllowedTools = append([]string(nil), server.AllowedTools...)
	out.TenantScopes = append([]string(nil), server.TenantScopes...)

	return out
}

func cloneToolCallRecord(record ToolCallRecord) ToolCallRecord {
	out := record
	out.ID = strings.TrimSpace(record.ID)
	out.At = record.At.UTC()
	out.TenantID = strings.TrimSpace(record.TenantID)
	out.PrincipalID = strings.TrimSpace(record.PrincipalID)
	out.RunID = strings.TrimSpace(record.RunID)
	out.WorkflowID = strings.TrimSpace(record.WorkflowID)
	out.NodeID = strings.TrimSpace(record.NodeID)
	out.Tool = strings.TrimSpace(record.Tool)
	out.ServerID = strings.TrimSpace(record.ServerID)
	out.ServerKind = strings.TrimSpace(record.ServerKind)
	out.RouteID = strings.TrimSpace(record.RouteID)
	out.Provider = strings.TrimSpace(record.Provider)
	out.Model = strings.TrimSpace(record.Model)
	out.Reason = strings.TrimSpace(record.Reason)
	out.Input = cloneRaw(record.Input)
	out.Output = cloneRaw(record.Output)
	out.Error = strings.TrimSpace(record.Error)

	return out
}

func cloneAdaptiveRoutingPolicy(policy *AdaptiveRoutingPolicy) *AdaptiveRoutingPolicy {
	if policy == nil {
		return nil
	}
	out := *policy
	if len(policy.Routes) > 0 {
		out.Routes = make([]AdaptiveRoute, 0, len(policy.Routes))
		for _, route := range policy.Routes {
			cloned := route
			cloned.ID = strings.TrimSpace(route.ID)
			cloned.Provider = strings.TrimSpace(route.Provider)
			cloned.Model = strings.TrimSpace(route.Model)
			cloned.TenantScopes = append([]string(nil), route.TenantScopes...)
			out.Routes = append(out.Routes, cloned)
		}
	}
	return &out
}

func normalizeAdaptiveRoutingPolicy(policy *AdaptiveRoutingPolicy) (*AdaptiveRoutingPolicy, error) {
	if policy == nil {
		return nil, nil
	}
	out := cloneAdaptiveRoutingPolicy(policy)
	if out.UncertaintyThreshold < 0 {
		out.UncertaintyThreshold = 0
	}
	if out.UncertaintyThreshold > 1 {
		out.UncertaintyThreshold = 1
	}
	if out.FailureAttemptThreshold <= 0 {
		out.FailureAttemptThreshold = 2
	}
	if out.RiskScoreThreshold <= 0 {
		out.RiskScoreThreshold = 50
	}
	seen := make(map[string]struct{}, len(out.Routes))
	for i := range out.Routes {
		out.Routes[i].ID = strings.TrimSpace(out.Routes[i].ID)
		out.Routes[i].Provider = strings.TrimSpace(out.Routes[i].Provider)
		out.Routes[i].Model = strings.TrimSpace(out.Routes[i].Model)
		out.Routes[i].TenantScopes = dedupeSortedStrings(out.Routes[i].TenantScopes)
		if out.Routes[i].ID == "" {
			out.Routes[i].ID = "route-" + strconv.Itoa(i+1)
		}
		if _, ok := seen[out.Routes[i].ID]; ok {
			return nil, fmt.Errorf("duplicate adaptive route id %q", out.Routes[i].ID)
		}
		seen[out.Routes[i].ID] = struct{}{}
	}
	return out, nil
}

func (b *ToolGatewayBundle) NormalizeAndValidate() error {
	if b == nil {
		return fmt.Errorf("tool gateway bundle is nil")
	}

	toolSeen := map[string]struct{}{}
	tools := make([]ToolPolicy, 0, len(b.Tools))

	for _, policy := range b.Tools {
		policy.ID = strings.TrimSpace(policy.ID)
		policy.Tool = strings.TrimSpace(policy.Tool)
		policy.ServerID = strings.TrimSpace(policy.ServerID)

		if policy.Tool == "" {
			return fmt.Errorf("tool policy tool is required")
		}

		if policy.ID == "" {
			policy.ID = "tool-" + strings.ReplaceAll(policy.Tool, ".", "-")
		}

		if _, exists := toolSeen[policy.Tool]; exists {
			return fmt.Errorf("duplicate tool policy for %q", policy.Tool)
		}

		toolSeen[policy.Tool] = struct{}{}
		policy.TenantScopes = dedupeSortedStrings(policy.TenantScopes)
		policy.RedactFields = dedupeSortedStrings(policy.RedactFields)
		policy.SideEffect = normalizeSideEffectPolicy(policy.SideEffect)
		adaptiveRouting, err := normalizeAdaptiveRoutingPolicy(policy.AdaptiveRouting)
		if err != nil {
			return err
		}
		policy.AdaptiveRouting = adaptiveRouting

		if err := validateSchemaDocument(policy.InputSchema, "tool "+policy.Tool+" input_schema"); err != nil {
			return err
		}

		if err := validateSchemaDocument(policy.OutputSchema, "tool "+policy.Tool+" output_schema"); err != nil {
			return err
		}

		tools = append(tools, cloneToolPolicy(policy))
	}

	b.Tools = tools
	serverSeen := map[string]struct{}{}
	servers := make([]ToolServerDefinition, 0, len(b.Servers))

	for _, server := range b.Servers {
		server.ID = strings.TrimSpace(server.ID)
		if server.ID == "" {
			return fmt.Errorf("tool server id is required")
		}

		if _, exists := serverSeen[server.ID]; exists {
			return fmt.Errorf("duplicate tool server %q", server.ID)
		}

		serverSeen[server.ID] = struct{}{}
		if server.Kind == "" {
			server.Kind = "mcp"
		}

		server.AllowedTools = dedupeSortedStrings(server.AllowedTools)
		server.TenantScopes = dedupeSortedStrings(server.TenantScopes)
		servers = append(servers, cloneToolServerDefinition(server))
	}

	b.Servers = servers
	now := time.Now().UTC()

	if b.UpdatedAt == nil {
		b.UpdatedAt = &now
	} else {
		t := b.UpdatedAt.UTC()
		b.UpdatedAt = &t
	}

	return nil
}

func dedupeSortedStrings(items []string) []string {
	if len(items) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(items))
	out := make([]string, 0, len(items))

	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		if _, ok := seen[item]; ok {
			continue
		}

		seen[item] = struct{}{}
		out = append(out, item)
	}

	sort.Strings(out)
	return out
}

func toolCallRecordKey(id string) string {
	return "tool_gateway/calls/" + url.PathEscape(strings.TrimSpace(id))
}

func (r *Runner) SaveToolGatewayBundle(bundle ToolGatewayBundle) error {
	if err := bundle.NormalizeAndValidate(); err != nil {
		return err
	}

	if err := putJSONKV(r.store, toolGatewayBundleKey, bundle); err != nil {
		return err
	}

	r.toolMu.Lock()
	r.toolGateway = &bundle
	r.toolMu.Unlock()
	return nil
}

func (r *Runner) GetToolGatewayBundle() (ToolGatewayBundle, bool, error) {
	r.toolMu.RLock()
	if r.toolGateway != nil {
		out := cloneToolGatewayBundle(*r.toolGateway)
		r.toolMu.RUnlock()
		return out, true, nil
	}

	r.toolMu.RUnlock()
	var bundle ToolGatewayBundle
	ok, err := getJSONKV(r.store, toolGatewayBundleKey, &bundle)

	if err != nil || !ok {
		return ToolGatewayBundle{}, ok, err
	}

	if err := bundle.NormalizeAndValidate(); err != nil {
		return ToolGatewayBundle{}, true, err
	}

	r.toolMu.Lock()
	r.toolGateway = &bundle
	r.toolMu.Unlock()

	return cloneToolGatewayBundle(bundle), true, nil
}

func (r *Runner) resolveToolPolicy(ctx context.Context, tool string) (ToolPolicy, error) {
	bundle, ok, err := r.GetToolGatewayBundle()
	if err != nil {
		return ToolPolicy{}, err
	}

	tenantID := effectiveTenantFromContext(ctx)
	if ok {
		for _, policy := range bundle.Tools {
			if policy.Tool != tool {
				continue
			}

			if len(policy.TenantScopes) > 0 && !containsString(policy.TenantScopes, tenantID) {
				return ToolPolicy{}, fmt.Errorf("%w: tool %q is not scoped to tenant %q", ErrToolNotApproved, tool, tenantID)
			}

			if !policy.Approved {
				return ToolPolicy{}, fmt.Errorf("%w: tool %q is disabled", ErrToolNotApproved, tool)
			}

			if policy.ServerID == "" {
				policy.ServerID = defaultToolServerID
			}

			return cloneToolPolicy(policy), nil
		}
	}

	return ToolPolicy{
		ID:       "implicit-" + strings.ReplaceAll(tool, ".", "-"),
		Tool:     tool,
		Approved: true,
		ServerID: defaultToolServerID,
	}, nil
}

func (r *Runner) resolveToolServer(ctx context.Context, policy ToolPolicy) (ToolServerDefinition, error) {
	serverID := strings.TrimSpace(policy.ServerID)
	if serverID == "" {
		serverID = defaultToolServerID
	}

	if serverID == defaultToolServerID {
		return ToolServerDefinition{
			ID:             defaultToolServerID,
			Kind:           "local",
			Approved:       true,
			SandboxAllowed: true,
		}, nil
	}

	bundle, ok, err := r.GetToolGatewayBundle()
	if err != nil {
		return ToolServerDefinition{}, err
	}

	if !ok {
		return ToolServerDefinition{}, fmt.Errorf("%w: server %q is not registered", ErrToolServerNotApproved, serverID)
	}

	tenantID := effectiveTenantFromContext(ctx)
	for _, server := range bundle.Servers {
		if server.ID != serverID {
			continue
		}

		if !server.Approved {
			return ToolServerDefinition{}, fmt.Errorf("%w: server %q is disabled", ErrToolServerNotApproved, serverID)
		}

		if len(server.TenantScopes) > 0 && !containsString(server.TenantScopes, tenantID) {
			return ToolServerDefinition{}, fmt.Errorf("%w: server %q is not scoped to tenant %q", ErrToolServerNotApproved, serverID, tenantID)
		}

		if len(server.AllowedTools) > 0 && !containsString(server.AllowedTools, policy.Tool) {
			return ToolServerDefinition{}, fmt.Errorf("%w: server %q does not allow tool %q", ErrToolServerNotApproved, serverID, policy.Tool)
		}

		return cloneToolServerDefinition(server), nil
	}

	return ToolServerDefinition{}, fmt.Errorf("%w: server %q is not registered", ErrToolServerNotApproved, serverID)
}

func containsString(items []string, target string) bool {
	target = strings.TrimSpace(target)

	for _, item := range items {
		if strings.TrimSpace(item) == target {
			return true
		}
	}

	return false
}

func (r *Runner) authorizeToolExecution(ctx context.Context, inv toolInvocation) error {
	bundle, ok, err := r.GetAuthorizationPolicy()
	if err != nil {
		return err
	}

	if !ok {
		bundle = AuthorizationPolicyBundle{}
	}

	principal, _ := PrincipalFrom(ctx)
	principal.Normalize()
	check := evaluateAuthorization(bundle, AuthorizationRequest{
		Action:             PolicyActionToolExecute,
		RunID:              strings.TrimSpace(inv.RunID),
		TenantID:           effectiveTenantFromContext(ctx),
		WorkflowID:         strings.TrimSpace(inv.WorkflowID),
		NodeID:             strings.TrimSpace(inv.NodeID),
		Tool:               strings.TrimSpace(inv.Tool),
		RequiredCapability: strings.TrimSpace(inv.RequiredCapability),
		Principal:          principal,
	})

	if check.Allowed {
		return nil
	}

	return &AuthorizationError{Report: WorkflowAuthorizationReport{
		Mode:       PolicyModeEnforce,
		RunID:      strings.TrimSpace(inv.RunID),
		TenantID:   effectiveTenantFromContext(ctx),
		WorkflowID: strings.TrimSpace(inv.WorkflowID),
		Principal:  principal,
		Allowed:    false,
		Reason:     check.Reason,
		Checks:     []AuthorizationCheck{check},
	}}
}

func (r *Runner) saveToolCallRecord(ctx context.Context, record ToolCallRecord) {
	record.ID = strings.TrimSpace(record.ID)
	if record.ID == "" {
		record.ID = "tool-call-" + time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + NewTraceID()
	}

	if record.At.IsZero() {
		record.At = time.Now().UTC()
	} else {
		record.At = record.At.UTC()
	}

	record.TenantID = strings.TrimSpace(record.TenantID)
	record.PrincipalID = strings.TrimSpace(record.PrincipalID)
	record.RunID = strings.TrimSpace(record.RunID)
	record.WorkflowID = strings.TrimSpace(record.WorkflowID)
	record.NodeID = strings.TrimSpace(record.NodeID)
	record.Tool = strings.TrimSpace(record.Tool)
	record.ServerID = strings.TrimSpace(record.ServerID)
	record.ServerKind = strings.TrimSpace(record.ServerKind)
	record.Reason = strings.TrimSpace(record.Reason)
	record.Error = strings.TrimSpace(record.Error)

	if record.Tool == "" {
		return
	}

	if record.TenantID == "" {
		record.TenantID = effectiveTenantFromContext(ctx)
	}

	if record.PrincipalID == "" {
		if principal, ok := PrincipalFrom(ctx); ok {
			record.PrincipalID = strings.TrimSpace(principal.ID)
		}
	}

	if err := putJSONKV(r.store, toolCallRecordKey(record.ID), record); err != nil {
		return
	}

	ids, err := indexIDs(r.store, toolCallIndexKey)
	if err != nil {
		return
	}

	ids = append(ids, record.ID)
	_ = putJSONKV(r.store, toolCallIndexKey, ids)
}

func (r *Runner) ListToolCallRecords(ctx context.Context, runID, tool string, limit int) ([]ToolCallRecord, error) {
	if limit <= 0 {
		limit = 100
	}

	tenantID := effectiveTenantFromContext(ctx)
	runID = strings.TrimSpace(runID)
	tool = strings.TrimSpace(tool)
	ids, err := indexIDs(r.store, toolCallIndexKey)

	if err != nil {
		return nil, err
	}

	out := make([]ToolCallRecord, 0, min(limit, len(ids)))
	for i := len(ids) - 1; i >= 0; i-- {
		var rec ToolCallRecord
		ok, err := getJSONKV(r.store, toolCallRecordKey(ids[i]), &rec)
		if err != nil || !ok {
			continue
		}

		if tenantID != "" && strings.TrimSpace(rec.TenantID) != tenantID {
			continue
		}

		if runID != "" && strings.TrimSpace(rec.RunID) != runID {
			continue
		}

		if tool != "" && strings.TrimSpace(rec.Tool) != tool {
			continue
		}

		out = append(out, cloneToolCallRecord(rec))
		if len(out) >= limit {
			break
		}
	}

	return out, nil
}

func (r *Runner) invokeTool(ctx context.Context, inv toolInvocation, input json.RawMessage) (json.RawMessage, error) {
	if inv.Handler == nil {
		return nil, fmt.Errorf("tool %q has no handler", strings.TrimSpace(inv.Tool))
	}

	start := time.Now().UTC()
	policy, err := r.resolveToolPolicy(ctx, inv.Tool)

	if err != nil {
		r.logToolCall(ctx, inv, ToolServerDefinition{}, policy, cloneRaw(input), nil, false, false, time.Since(start), err, AdaptiveRoute{})
		return nil, err
	}

	server, err := r.resolveToolServer(ctx, policy)
	if err != nil {
		r.logToolCall(ctx, inv, ToolServerDefinition{}, policy, cloneRaw(input), nil, false, false, time.Since(start), err, AdaptiveRoute{})
		return nil, err
	}

	if err := r.authorizeToolExecution(ctx, inv); err != nil {
		r.logToolCall(ctx, inv, server, policy, cloneRaw(input), nil, false, false, time.Since(start), err, AdaptiveRoute{})
		return nil, err
	}

	if err := validateSchemaValue(inv.InputSchema, input, "node input"); err != nil {
		err = fmt.Errorf("%w: %s", ErrToolSchemaInvalid, err.Error())
		r.logToolCall(ctx, inv, server, policy, cloneRaw(input), nil, false, false, time.Since(start), err, AdaptiveRoute{})
		return nil, err
	}

	if err := validateSchemaValue(policy.InputSchema, input, "tool input"); err != nil {
		err = fmt.Errorf("%w: %s", ErrToolSchemaInvalid, err.Error())
		r.logToolCall(ctx, inv, server, policy, cloneRaw(input), nil, false, false, time.Since(start), err, AdaptiveRoute{})
		return nil, err
	}

	sandboxed := policy.SandboxRequired
	if riskDecision, ok := RiskDecisionFrom(ctx); ok && riskDecision.Action == RiskActionSandbox {
		sandboxed = true
	}

	if sandboxed && !server.SandboxAllowed && server.ID != defaultToolServerID {
		err = fmt.Errorf("%w: server %q does not allow sandbox execution", ErrToolServerNotApproved, server.ID)
		r.logToolCall(ctx, inv, server, policy, cloneRaw(input), nil, false, sandboxed, time.Since(start), err, AdaptiveRoute{})
		return nil, err
	}

	selectedRoute, err := r.selectAdaptiveRoute(ctx, inv, policy, input)
	if err != nil {
		r.logToolCall(ctx, inv, server, policy, cloneRaw(input), nil, false, sandboxed, time.Since(start), err, AdaptiveRoute{})
		return nil, err
	}

	ctx = WithToolRuntime(ctx, ToolRuntimeContext{
		Tool:       inv.Tool,
		ServerID:   server.ID,
		ServerKind: server.Kind,
		RouteID:    strings.TrimSpace(selectedRoute.ID),
		Provider:   strings.TrimSpace(selectedRoute.Provider),
		Model:      strings.TrimSpace(selectedRoute.Model),
		Sandboxed:  sandboxed,
		Policy:     cloneToolPolicy(policy),
		Server:     cloneToolServerDefinition(server),
	})

	if sideEffect := normalizeSideEffectPolicy(policy.SideEffect); sideEffect != nil && sideEffect.Enabled {
		output, err := r.executeSideEffect(ctx, inv, server, policy, input)
		r.logToolCall(ctx, inv, server, policy, cloneRaw(input), cloneRaw(output), true, sandboxed, time.Since(start), err, selectedRoute)
		return output, err
	}

	output, err := inv.Handler(ctx, cloneRaw(input))
	if err != nil {
		r.logToolCall(ctx, inv, server, policy, cloneRaw(input), cloneRaw(output), true, sandboxed, time.Since(start), err, selectedRoute)
		return output, err
	}

	if err := validateSchemaValue(policy.OutputSchema, output, "tool output"); err != nil {
		err = fmt.Errorf("%w: %s", ErrToolSchemaInvalid, err.Error())
		r.logToolCall(ctx, inv, server, policy, cloneRaw(input), cloneRaw(output), true, sandboxed, time.Since(start), err, selectedRoute)
		return nil, err
	}

	if err := validateSchemaValue(inv.OutputSchema, output, "node output"); err != nil {
		err = fmt.Errorf("%w: %s", ErrToolSchemaInvalid, err.Error())
		r.logToolCall(ctx, inv, server, policy, cloneRaw(input), cloneRaw(output), true, sandboxed, time.Since(start), err, selectedRoute)
		return nil, err
	}

	r.logToolCall(ctx, inv, server, policy, cloneRaw(input), cloneRaw(output), true, sandboxed, time.Since(start), nil, selectedRoute)
	return output, nil
}

func (r *Runner) logToolCall(ctx context.Context, inv toolInvocation, server ToolServerDefinition, policy ToolPolicy, input, output json.RawMessage, allowed bool, sandboxed bool, dur time.Duration, callErr error, route AdaptiveRoute) {
	record := ToolCallRecord{
		At:         time.Now().UTC(),
		TenantID:   effectiveTenantFromContext(ctx),
		RunID:      strings.TrimSpace(inv.RunID),
		WorkflowID: strings.TrimSpace(inv.WorkflowID),
		NodeID:     strings.TrimSpace(inv.NodeID),
		Attempt:    inv.Attempt,
		Tool:       strings.TrimSpace(inv.Tool),
		ServerID:   strings.TrimSpace(server.ID),
		ServerKind: strings.TrimSpace(server.Kind),
		RouteID:    strings.TrimSpace(route.ID),
		Provider:   strings.TrimSpace(route.Provider),
		Model:      strings.TrimSpace(route.Model),
		Allowed:    allowed && callErr == nil,
		Sandboxed:  sandboxed,
		DurationMS: dur.Milliseconds(),
		Reason: func() string {
			if callErr != nil {
				return callErr.Error()
			}
			if sandboxed {
				return "sandboxed tool execution"
			}
			return "tool executed"
		}(),
		Input:  redactJSON(input, combinedRedactFields(policy)),
		Output: redactJSON(output, combinedRedactFields(policy)),
	}

	if callErr != nil {
		record.Error = callErr.Error()
	}

	r.saveToolCallRecord(ctx, record)
	r.appendAuditRecord(ctx, AuditRecord{
		TenantID:     record.TenantID,
		Action:       "tool.call",
		ResourceType: "tool",
		ResourceID:   record.Tool,
		RunID:        record.RunID,
		WorkflowID:   record.WorkflowID,
		Outcome: func() AuditOutcome {
			if record.Allowed {
				return AuditOutcomeAllowed
			}
			return AuditOutcomeDenied
		}(),
		Reason: record.Reason,
	})
}

func (r *Runner) adaptiveRouteCandidates(ctx context.Context, inv toolInvocation, policy ToolPolicy, input json.RawMessage) ([]AdaptiveRoute, []string, error) {
	adaptive := policy.AdaptiveRouting

	if adaptive == nil || len(adaptive.Routes) == 0 {
		return nil, nil, nil
	}

	runBudget := BudgetPolicy{}
	runUsage := BudgetUsage{}
	if inv.RunID != "" {
		if run, ok := r.store.GetRun(inv.RunID); ok {
			runBudget = run.RunBudget
			runUsage = run.BudgetUsage

			if runBudget == (BudgetPolicy{}) {
				def := r.getDefaultRunBudget()
				tenantBudget, _ := r.getTenantBudget(effectiveTenantFromContext(ctx))
				runBudget = effectiveBudget(def, tenantBudget, run.RunBudget)
			}
		}
	}

	hints := routingHintsFromInput(input)
	triggers := make([]string, 0, 3)

	if adaptive.EscalateOnUncertainty && hints.Uncertainty >= adaptive.UncertaintyThreshold {
		triggers = append(triggers, "uncertainty")
	}

	if adaptive.EscalateOnFailure && inv.Attempt >= adaptive.FailureAttemptThreshold {
		triggers = append(triggers, "failure")
	}

	if adaptive.EscalateOnRisk {
		if decision, ok := RiskDecisionFrom(ctx); ok {
			if decision.Score >= adaptive.RiskScoreThreshold || decision.Action == RiskActionSandbox || decision.Action == RiskActionRequireApproval {
				triggers = append(triggers, "risk")
			}
		}
	}

	tenantID := effectiveTenantFromContext(ctx)
	candidates := make([]AdaptiveRoute, 0, len(adaptive.Routes))
	for _, route := range adaptive.Routes {
		if len(route.TenantScopes) > 0 && !containsString(route.TenantScopes, tenantID) {
			continue
		}

		if runBudget.MaxTokens > 0 && route.EstimatedTokens > 0 && runUsage.Tokens+route.EstimatedTokens > runBudget.MaxTokens {
			continue
		}

		if runBudget.MaxDollars > 0 && route.EstimatedDollars > 0 && runUsage.Dollars+route.EstimatedDollars > runBudget.MaxDollars {
			continue
		}

		if limiter := RateLimiterFrom(ctx); limiter != nil {
			decision, err := limiter.Decide(ctx, RateLimitRequest{
				TenantID:         tenantID,
				Provider:         route.Provider,
				Kind:             RateLimitTool,
				EstimatedTokens:  route.EstimatedTokens,
				EstimatedDollars: route.EstimatedDollars,
			})

			if err != nil {
				return nil, nil, err
			}

			if !decision.Allowed {
				continue
			}
		}
		candidates = append(candidates, route)
	}

	if len(candidates) == 0 {
		return nil, triggers, ErrBudgetExceeded
	}

	return candidates, triggers, nil
}

func (r *Runner) legacyAdaptiveRouteSelection(policy ToolPolicy, candidates []AdaptiveRoute, input json.RawMessage, triggers []string) (AdaptiveRoute, error) {
	adaptive := policy.AdaptiveRouting
	if adaptive == nil || len(candidates) == 0 {
		return AdaptiveRoute{}, nil
	}
	escalate := len(triggers) > 0

	sort.Slice(candidates, func(i, j int) bool {
		left := candidates[i]
		right := candidates[j]
		if escalate {
			if left.Priority != right.Priority {
				return left.Priority > right.Priority
			}

			if left.EstimatedDollars != right.EstimatedDollars {
				return left.EstimatedDollars < right.EstimatedDollars
			}

			if left.EstimatedTokens != right.EstimatedTokens {
				return left.EstimatedTokens < right.EstimatedTokens
			}

			return left.ID < right.ID
		}

		if adaptive.CheapFirst {
			if left.EstimatedDollars != right.EstimatedDollars {
				return left.EstimatedDollars < right.EstimatedDollars
			}

			if left.EstimatedTokens != right.EstimatedTokens {
				return left.EstimatedTokens < right.EstimatedTokens
			}

			if left.Priority != right.Priority {
				return left.Priority < right.Priority
			}

			return left.ID < right.ID
		}

		if left.Priority != right.Priority {
			return left.Priority > right.Priority
		}

		if left.EstimatedDollars != right.EstimatedDollars {
			return left.EstimatedDollars < right.EstimatedDollars
		}

		return left.ID < right.ID
	})

	return candidates[0], nil
}

func (r *Runner) selectAdaptiveRoute(ctx context.Context, inv toolInvocation, policy ToolPolicy, input json.RawMessage) (AdaptiveRoute, error) {
	candidates, triggers, err := r.adaptiveRouteCandidates(ctx, inv, policy, input)
	if err != nil {
		return AdaptiveRoute{}, err
	}
	if len(candidates) == 0 {
		return AdaptiveRoute{}, nil
	}
	if decision, used, err := r.evaluateBrainDecision(ctx, inv, policy, candidates, input, triggers); err != nil {
		return AdaptiveRoute{}, err
	} else if used {
		return decision.Selected, nil
	}
	return r.legacyAdaptiveRouteSelection(policy, candidates, input, triggers)
}

func routingHintsFromInput(input json.RawMessage) adaptiveRoutingHints {
	if len(input) == 0 {
		return adaptiveRoutingHints{}
	}

	var payload map[string]any
	if err := json.Unmarshal(input, &payload); err != nil {
		return adaptiveRoutingHints{}
	}

	var hints adaptiveRoutingHints
	if routing, ok := payload["routing"].(map[string]any); ok {
		if uncertainty, ok := routing["uncertainty"].(float64); ok {
			hints.Uncertainty = uncertainty
		}
		return hints
	}

	if uncertainty, ok := payload["uncertainty"].(float64); ok {
		hints.Uncertainty = uncertainty
	}

	return hints
}

func combinedRedactFields(policy ToolPolicy) []string {
	fields := []string{
		"password", "passwd", "secret", "token", "api_key", "apikey",
		"authorization", "private_key", "access_token", "refresh_token",
	}

	fields = append(fields, policy.RedactFields...)
	return dedupeSortedStrings(fields)
}

func redactJSON(raw json.RawMessage, fields []string) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}

	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return cloneRaw(raw)
	}

	redacted := redactValue(value, fields)
	b, err := json.Marshal(redacted)

	if err != nil {
		return cloneRaw(raw)
	}

	return json.RawMessage(b)
}

func redactValue(value any, fields []string) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, inner := range typed {
			if shouldRedactKey(key, fields) {
				out[key] = "[REDACTED]"
				continue
			}

			out[key] = redactValue(inner, fields)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, inner := range typed {
			out[i] = redactValue(inner, fields)
		}
		return out
	default:
		return value
	}
}

func shouldRedactKey(key string, fields []string) bool {
	key = strings.ToLower(strings.TrimSpace(key))
	for _, field := range fields {
		field = strings.ToLower(strings.TrimSpace(field))

		if field != "" && strings.Contains(key, field) {
			return true
		}
	}
	return false
}

func validateSchemaDocument(raw json.RawMessage, name string) error {
	if len(raw) == 0 {
		return nil
	}

	var schema any
	if err := json.Unmarshal(raw, &schema); err != nil {
		return fmt.Errorf("%s: invalid json schema: %w", name, err)
	}

	return nil
}

func validateSchemaValue(schemaRaw json.RawMessage, valueRaw json.RawMessage, name string) error {
	if len(schemaRaw) == 0 {
		return nil
	}

	var schema map[string]any
	if err := json.Unmarshal(schemaRaw, &schema); err != nil {
		return fmt.Errorf("%s schema invalid: %w", name, err)
	}

	var value any
	if len(valueRaw) == 0 {
		value = nil
	} else if err := json.Unmarshal(valueRaw, &value); err != nil {
		return fmt.Errorf("%s is invalid json: %w", name, err)
	}

	return validateJSONSchema(schema, value, name)
}

func validateJSONSchema(schema map[string]any, value any, path string) error {
	if len(schema) == 0 {
		return nil
	}

	if rawType, ok := schema["type"].(string); ok {
		switch rawType {
		case "object":
			obj, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("%s must be object", path)
			}

			required := schemaStringSlice(schema["required"])
			for _, key := range required {
				if _, ok := obj[key]; !ok {
					return fmt.Errorf("%s missing required field %q", path, key)
				}
			}

			props, _ := schema["properties"].(map[string]any)
			additionalAllowed := true
			if additional, exists := schema["additionalProperties"].(bool); exists {
				additionalAllowed = additional
			}

			for key, inner := range obj {
				propSchema, hasProp := props[key]
				if !hasProp {
					if !additionalAllowed {
						return fmt.Errorf("%s contains unknown field %q", path, key)
					}
					continue
				}

				propMap, ok := propSchema.(map[string]any)
				if !ok {
					continue
				}

				if err := validateJSONSchema(propMap, inner, path+"."+key); err != nil {
					return err
				}
			}
		case "array":
			arr, ok := value.([]any)
			if !ok {
				return fmt.Errorf("%s must be array", path)
			}

			if itemSchema, ok := schema["items"].(map[string]any); ok {
				for i, inner := range arr {
					if err := validateJSONSchema(itemSchema, inner, path+"["+strconv.Itoa(i)+"]"); err != nil {
						return err
					}
				}
			}
		case "string":
			if _, ok := value.(string); !ok {
				return fmt.Errorf("%s must be string", path)
			}
		case "integer":
			num, ok := value.(float64)
			if !ok || num != float64(int64(num)) {
				return fmt.Errorf("%s must be integer", path)
			}
		case "number":
			if _, ok := value.(float64); !ok {
				return fmt.Errorf("%s must be number", path)
			}
		case "boolean":
			if _, ok := value.(bool); !ok {
				return fmt.Errorf("%s must be boolean", path)
			}
		case "null":
			if value != nil {
				return fmt.Errorf("%s must be null", path)
			}
		}
	}

	if enumVals, ok := schema["enum"].([]any); ok && len(enumVals) > 0 {
		for _, candidate := range enumVals {
			if valuesEquivalent(candidate, value) {
				return nil
			}
		}
		return fmt.Errorf("%s must match enum", path)
	}

	return nil
}

func schemaStringSlice(v any) []string {
	items, ok := v.([]any)

	if !ok {
		return nil
	}

	out := make([]string, 0, len(items))
	for _, item := range items {
		if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
			out = append(out, s)
		}
	}

	return out
}

func valuesEquivalent(a, b any) bool {
	ab, errA := json.Marshal(a)
	bb, errB := json.Marshal(b)
	return errA == nil && errB == nil && string(ab) == string(bb)
}
