package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

var ErrAuthorizationDenied = errors.New("authorization denied")

const authzPolicyBundleKey = "authz/policy_bundle"

type PolicyEffect string
type PolicyAction string
type PolicyMode string

const (
	PolicyEffectAllow PolicyEffect = "allow"
	PolicyEffectDeny  PolicyEffect = "deny"
)

const (
	PolicyActionRunStart    PolicyAction = "run_start"
	PolicyActionToolExecute PolicyAction = "tool_execute"
)

const (
	PolicyModeEnforce  PolicyMode = "enforce"
	PolicyModeSimulate PolicyMode = "simulate"
)

type Principal struct {
	ID           string            `json:"id"`
	Type         string            `json:"type,omitempty"`
	TenantID     string            `json:"tenant_id,omitempty"`
	TenantScopes []string          `json:"tenant_scopes,omitempty"`
	Roles        []string          `json:"roles,omitempty"`
	Capabilities []string          `json:"capabilities,omitempty"`
	Attributes   map[string]string `json:"attributes,omitempty"`
}

type AuthorizationRule struct {
	ID                    string         `json:"id"`
	Description           string         `json:"description,omitempty"`
	Effect                PolicyEffect   `json:"effect"`
	Actions               []PolicyAction `json:"actions,omitempty"`
	PrincipalIDs          []string       `json:"principal_ids,omitempty"`
	Roles                 []string       `json:"roles,omitempty"`
	PrincipalCapabilities []string       `json:"principal_capabilities,omitempty"`
	Tenants               []string       `json:"tenants,omitempty"`
	Workflows             []string       `json:"workflows,omitempty"`
	Tools                 []string       `json:"tools,omitempty"`
	NodeIDs               []string       `json:"node_ids,omitempty"`
	RequiredCapabilities  []string       `json:"required_capabilities,omitempty"`
}

type AuthorizationPolicyBundle struct {
	ID          string              `json:"id,omitempty"`
	Description string              `json:"description,omitempty"`
	UpdatedAt   *time.Time          `json:"updated_at,omitempty"`
	Rules       []AuthorizationRule `json:"rules,omitempty"`
}

type AuthorizationRequest struct {
	Action             PolicyAction `json:"action"`
	RunID              string       `json:"run_id,omitempty"`
	TenantID           string       `json:"tenant_id,omitempty"`
	WorkflowID         string       `json:"workflow_id,omitempty"`
	NodeID             string       `json:"node_id,omitempty"`
	Tool               string       `json:"tool,omitempty"`
	RequiredCapability string       `json:"required_capability,omitempty"`
	Principal          Principal    `json:"principal"`
}

type AuthorizationCheck struct {
	Action             PolicyAction `json:"action"`
	Allowed            bool         `json:"allowed"`
	RunID              string       `json:"run_id,omitempty"`
	TenantID           string       `json:"tenant_id,omitempty"`
	WorkflowID         string       `json:"workflow_id,omitempty"`
	NodeID             string       `json:"node_id,omitempty"`
	Tool               string       `json:"tool,omitempty"`
	RequiredCapability string       `json:"required_capability,omitempty"`
	Reason             string       `json:"reason,omitempty"`
	MatchedAllowRules  []string     `json:"matched_allow_rules,omitempty"`
	MatchedDenyRules   []string     `json:"matched_deny_rules,omitempty"`
	BuiltinDenials     []string     `json:"builtin_denials,omitempty"`
}

type WorkflowAuthorizationReport struct {
	Allowed    bool                 `json:"allowed"`
	Mode       PolicyMode           `json:"mode"`
	RunID      string               `json:"run_id,omitempty"`
	TenantID   string               `json:"tenant_id,omitempty"`
	WorkflowID string               `json:"workflow_id,omitempty"`
	Reason     string               `json:"reason,omitempty"`
	Principal  Principal            `json:"principal"`
	Checks     []AuthorizationCheck `json:"checks,omitempty"`
}

type AuthorizationError struct {
	Report WorkflowAuthorizationReport
}

func (e *AuthorizationError) Error() string {
	if strings.TrimSpace(e.Report.Reason) != "" {
		return e.Report.Reason
	}

	return ErrAuthorizationDenied.Error()
}

func (e *AuthorizationError) Unwrap() error {
	return ErrAuthorizationDenied
}

type ctxKeyPrincipal struct{}

func WithPrincipal(ctx context.Context, principal Principal) context.Context {
	principal.Normalize()
	return context.WithValue(ctx, ctxKeyPrincipal{}, principal)
}

func PrincipalFrom(ctx context.Context) (Principal, bool) {
	v := ctx.Value(ctxKeyPrincipal{})
	p, ok := v.(Principal)

	if !ok {
		return Principal{}, false
	}

	p.Normalize()
	return p, true
}

func (p *Principal) Normalize() {
	if p == nil {
		return
	}

	p.ID = strings.TrimSpace(p.ID)
	p.Type = strings.TrimSpace(p.Type)
	p.TenantID = strings.TrimSpace(p.TenantID)
	p.TenantScopes = normalizeStrings(p.TenantScopes)
	p.Roles = normalizeStrings(p.Roles)
	p.Capabilities = normalizeStrings(p.Capabilities)

	if p.TenantID != "" && len(p.TenantScopes) == 0 {
		p.TenantScopes = []string{p.TenantID}
	}

	if len(p.Attributes) == 0 {
		p.Attributes = nil
		return
	}

	out := make(map[string]string, len(p.Attributes))
	for k, v := range p.Attributes {
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if k == "" {
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		p.Attributes = nil
		return
	}
	p.Attributes = out
}

func (r *AuthorizationRule) NormalizeAndValidate() error {
	if r == nil {
		return fmt.Errorf("authorization rule is nil")
	}

	r.ID = strings.TrimSpace(r.ID)
	r.Description = strings.TrimSpace(r.Description)
	r.Actions = normalizePolicyActions(r.Actions)
	r.PrincipalIDs = normalizeStrings(r.PrincipalIDs)
	r.Roles = normalizeStrings(r.Roles)
	r.PrincipalCapabilities = normalizeStrings(r.PrincipalCapabilities)
	r.Tenants = normalizeStrings(r.Tenants)
	r.Workflows = normalizeStrings(r.Workflows)
	r.Tools = normalizeStrings(r.Tools)
	r.NodeIDs = normalizeStrings(r.NodeIDs)
	r.RequiredCapabilities = normalizeStrings(r.RequiredCapabilities)

	if r.ID == "" {
		return fmt.Errorf("authorization rule id is required")
	}

	switch r.Effect {
	case PolicyEffectAllow, PolicyEffectDeny:
	default:
		return fmt.Errorf("authorization rule %q has invalid effect %q", r.ID, r.Effect)
	}

	return nil
}

func (b *AuthorizationPolicyBundle) NormalizeAndValidate() error {
	if b == nil {
		return fmt.Errorf("authorization policy bundle is nil")
	}

	b.ID = strings.TrimSpace(b.ID)
	b.Description = strings.TrimSpace(b.Description)

	if b.UpdatedAt != nil {
		t := b.UpdatedAt.UTC()
		b.UpdatedAt = &t
	}

	seen := make(map[string]struct{}, len(b.Rules))
	for i := range b.Rules {
		if err := b.Rules[i].NormalizeAndValidate(); err != nil {
			return err
		}

		if _, ok := seen[b.Rules[i].ID]; ok {
			return fmt.Errorf("duplicate authorization rule id %q", b.Rules[i].ID)
		}

		seen[b.Rules[i].ID] = struct{}{}
	}

	sort.Slice(b.Rules, func(i, j int) bool {
		return b.Rules[i].ID < b.Rules[j].ID
	})

	return nil
}

func cloneAuthorizationPolicyBundle(bundle AuthorizationPolicyBundle) AuthorizationPolicyBundle {
	out := bundle
	if bundle.UpdatedAt != nil {
		t := bundle.UpdatedAt.UTC()
		out.UpdatedAt = &t
	}

	if len(bundle.Rules) == 0 {
		out.Rules = nil
		return out
	}

	out.Rules = make([]AuthorizationRule, len(bundle.Rules))
	for i, rule := range bundle.Rules {
		out.Rules[i] = AuthorizationRule{
			ID:                    rule.ID,
			Description:           rule.Description,
			Effect:                rule.Effect,
			Actions:               append([]PolicyAction(nil), rule.Actions...),
			PrincipalIDs:          append([]string(nil), rule.PrincipalIDs...),
			Roles:                 append([]string(nil), rule.Roles...),
			PrincipalCapabilities: append([]string(nil), rule.PrincipalCapabilities...),
			Tenants:               append([]string(nil), rule.Tenants...),
			Workflows:             append([]string(nil), rule.Workflows...),
			Tools:                 append([]string(nil), rule.Tools...),
			NodeIDs:               append([]string(nil), rule.NodeIDs...),
			RequiredCapabilities:  append([]string(nil), rule.RequiredCapabilities...),
		}
	}
	return out
}

func normalizePolicyActions(values []PolicyAction) []PolicyAction {
	if len(values) == 0 {
		return nil
	}

	seen := map[PolicyAction]struct{}{}
	out := make([]PolicyAction, 0, len(values))
	for _, v := range values {
		v = PolicyAction(strings.TrimSpace(string(v)))
		if v == "" {
			continue
		}

		if _, ok := seen[v]; ok {
			continue
		}

		seen[v] = struct{}{}
		out = append(out, v)
	}

	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func normalizeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}

		if _, ok := seen[v]; ok {
			continue
		}

		seen[v] = struct{}{}
		out = append(out, v)
	}

	sort.Strings(out)
	return out
}

func (p Principal) hasCapability(capability string) bool {
	capability = strings.TrimSpace(capability)
	if capability == "" {
		return false
	}

	for _, item := range p.Capabilities {
		if item == capability {
			return true
		}
	}

	return false
}

func evaluateAuthorization(bundle AuthorizationPolicyBundle, req AuthorizationRequest) AuthorizationCheck {
	req.Action = PolicyAction(strings.TrimSpace(string(req.Action)))
	req.RunID = strings.TrimSpace(req.RunID)
	req.TenantID = strings.TrimSpace(req.TenantID)
	req.WorkflowID = strings.TrimSpace(req.WorkflowID)
	req.NodeID = strings.TrimSpace(req.NodeID)
	req.Tool = strings.TrimSpace(req.Tool)
	req.RequiredCapability = strings.TrimSpace(req.RequiredCapability)
	req.Principal.Normalize()

	check := AuthorizationCheck{
		Action:             req.Action,
		RunID:              req.RunID,
		TenantID:           req.TenantID,
		WorkflowID:         req.WorkflowID,
		NodeID:             req.NodeID,
		Tool:               req.Tool,
		RequiredCapability: req.RequiredCapability,
	}

	for _, scope := range req.Principal.TenantScopes {
		if req.TenantID == "" {
			break
		}

		if matchPattern(req.TenantID, scope) {
			goto capabilityCheck
		}
	}

	if req.TenantID != "" && len(req.Principal.TenantScopes) > 0 {
		check.BuiltinDenials = append(check.BuiltinDenials, fmt.Sprintf("principal tenant scope does not include tenant %q", req.TenantID))
	}

capabilityCheck:
	if req.RequiredCapability != "" && !req.Principal.hasCapability(req.RequiredCapability) {
		check.BuiltinDenials = append(check.BuiltinDenials, fmt.Sprintf("principal lacks required capability %q", req.RequiredCapability))
	}

	anyAllow := false
	for _, rule := range bundle.Rules {
		if !rule.matchesAction(req.Action) {
			continue
		}

		if rule.Effect == PolicyEffectAllow {
			anyAllow = true
		}

		if !rule.matches(req) {
			continue
		}

		switch rule.Effect {
		case PolicyEffectAllow:
			check.MatchedAllowRules = append(check.MatchedAllowRules, rule.ID)
		case PolicyEffectDeny:
			check.MatchedDenyRules = append(check.MatchedDenyRules, rule.ID)
		}
	}

	switch {
	case len(check.BuiltinDenials) > 0:
		check.Allowed = false
		check.Reason = strings.Join(check.BuiltinDenials, "; ")
	case len(check.MatchedDenyRules) > 0:
		check.Allowed = false
		check.Reason = "denied by policy rule(s): " + strings.Join(check.MatchedDenyRules, ", ")
	case anyAllow && len(check.MatchedAllowRules) == 0:
		check.Allowed = false
		check.Reason = "no allow rule matched"
	case len(check.MatchedAllowRules) > 0:
		check.Allowed = true
		check.Reason = "allowed by policy rule(s): " + strings.Join(check.MatchedAllowRules, ", ")
	default:
		check.Allowed = true
		check.Reason = "allowed"
	}

	return check
}

func (r AuthorizationRule) matchesAction(action PolicyAction) bool {
	if len(r.Actions) == 0 {
		return true
	}

	for _, item := range r.Actions {
		if item == action {
			return true
		}
	}
	return false
}

func (r AuthorizationRule) matches(req AuthorizationRequest) bool {
	if len(r.PrincipalIDs) > 0 && !matchesAnyPattern(req.Principal.ID, r.PrincipalIDs) {
		return false
	}

	if len(r.Roles) > 0 && !matchesAnyMember(req.Principal.Roles, r.Roles) {
		return false
	}

	if len(r.PrincipalCapabilities) > 0 && !matchesAnyMember(req.Principal.Capabilities, r.PrincipalCapabilities) {
		return false
	}

	if len(r.Tenants) > 0 && !matchesAnyPattern(req.TenantID, r.Tenants) {
		return false
	}

	if len(r.Workflows) > 0 && !matchesAnyPattern(req.WorkflowID, r.Workflows) {
		return false
	}

	if len(r.Tools) > 0 && !matchesAnyPattern(req.Tool, r.Tools) {
		return false
	}

	if len(r.NodeIDs) > 0 && !matchesAnyPattern(req.NodeID, r.NodeIDs) {
		return false
	}

	if len(r.RequiredCapabilities) > 0 && !matchesAnyPattern(req.RequiredCapability, r.RequiredCapabilities) {
		return false
	}

	return true
}

func matchesAnyMember(values []string, patterns []string) bool {
	for _, value := range values {
		if matchesAnyPattern(value, patterns) {
			return true
		}
	}

	return false
}

func matchesAnyPattern(value string, patterns []string) bool {
	value = strings.TrimSpace(value)
	if len(patterns) == 0 {
		return true
	}

	for _, pattern := range patterns {
		if matchPattern(value, pattern) {
			return true
		}
	}

	return false
}

func matchPattern(value, pattern string) bool {
	value = strings.TrimSpace(value)
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return false
	}

	if pattern == "*" {
		return true
	}

	if !strings.Contains(pattern, "*") {
		return value == pattern
	}

	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return value == pattern
	}

	if parts[0] != "" && !strings.HasPrefix(value, parts[0]) {
		return false
	}

	if last := parts[len(parts)-1]; last != "" && !strings.HasSuffix(value, last) {
		return false
	}

	pos := 0
	for i, part := range parts {
		if part == "" {
			continue
		}

		idx := strings.Index(value[pos:], part)
		if idx < 0 {
			return false
		}

		if i == 0 && idx != 0 {
			return false
		}

		pos += idx + len(part)
	}

	return true
}

func (r *Runner) SaveAuthorizationPolicy(bundle AuthorizationPolicyBundle) error {
	if err := bundle.NormalizeAndValidate(); err != nil {
		return err
	}

	now := time.Now().UTC()
	if bundle.UpdatedAt == nil {
		bundle.UpdatedAt = &now
	} else {
		t := bundle.UpdatedAt.UTC()
		bundle.UpdatedAt = &t
	}

	if err := putJSONKV(r.store, authzPolicyBundleKey, bundle); err != nil {
		return err
	}

	r.policyMu.Lock()
	r.policyBundle = &bundle
	r.policyMu.Unlock()
	return nil
}

func (r *Runner) GetAuthorizationPolicy() (AuthorizationPolicyBundle, bool, error) {
	r.policyMu.RLock()
	if r.policyBundle != nil {
		out := cloneAuthorizationPolicyBundle(*r.policyBundle)
		r.policyMu.RUnlock()
		return out, true, nil
	}

	r.policyMu.RUnlock()

	var bundle AuthorizationPolicyBundle
	ok, err := getJSONKV(r.store, authzPolicyBundleKey, &bundle)
	if err != nil || !ok {
		return bundle, ok, err
	}

	if err := bundle.NormalizeAndValidate(); err != nil {
		return AuthorizationPolicyBundle{}, true, err
	}

	r.policyMu.Lock()
	r.policyBundle = &bundle
	r.policyMu.Unlock()
	return cloneAuthorizationPolicyBundle(bundle), true, nil
}

func (r *Runner) EvaluateRunSpecAuthorization(ctx context.Context, runID string, specJSON []byte, reg *HandlerRegistry) (WorkflowAuthorizationReport, error) {
	g, spec, err := ParseWorkflowSpecJSON(specJSON)
	if err != nil {
		return WorkflowAuthorizationReport{}, err
	}

	exec, err := CompileSpecToExecutable(spec, g, reg)
	if err != nil {
		return WorkflowAuthorizationReport{}, err
	}

	return r.EvaluateWorkflowAuthorization(ctx, runID, exec)
}

func (r *Runner) EvaluateWorkflowAuthorization(ctx context.Context, runID string, g WorkflowGraph) (WorkflowAuthorizationReport, error) {
	bundle, ok, err := r.GetAuthorizationPolicy()
	if err != nil {
		return WorkflowAuthorizationReport{}, err
	}

	if !ok {
		bundle = AuthorizationPolicyBundle{}
	}

	principal, _ := PrincipalFrom(ctx)
	principal.Normalize()

	tenantID := strings.TrimSpace(TenantIDFrom(ctx))
	if tenantID == "" {
		tenantID = principal.TenantID
	}

	workflowID := strings.TrimSpace(g.ID)
	report := WorkflowAuthorizationReport{
		Mode:       PolicyModeEnforce,
		RunID:      strings.TrimSpace(runID),
		TenantID:   tenantID,
		WorkflowID: workflowID,
		Principal:  principal,
	}

	runCheck := evaluateAuthorization(bundle, AuthorizationRequest{
		Action:     PolicyActionRunStart,
		RunID:      report.RunID,
		TenantID:   tenantID,
		WorkflowID: workflowID,
		Principal:  principal,
	})
	report.Checks = append(report.Checks, runCheck)

	allowed := runCheck.Allowed
	reason := runCheck.Reason

	for _, node := range g.Nodes {
		if strings.TrimSpace(node.Topic) == "" && strings.TrimSpace(node.RequiredCapability) == "" {
			continue
		}

		check := evaluateAuthorization(bundle, AuthorizationRequest{
			Action:             PolicyActionToolExecute,
			RunID:              report.RunID,
			TenantID:           tenantID,
			WorkflowID:         workflowID,
			NodeID:             node.NodeID,
			Tool:               node.Topic,
			RequiredCapability: node.RequiredCapability,
			Principal:          principal,
		})
		report.Checks = append(report.Checks, check)
		if !check.Allowed && allowed {
			allowed = false
			reason = check.Reason
		}
	}

	report.Allowed = allowed
	if allowed {
		report.Reason = "authorized"
	} else {
		report.Reason = reason
	}

	return report, nil
}

func (r *Runner) authorizeWorkflow(ctx context.Context, runID string, g WorkflowGraph) (report *WorkflowAuthorizationReport, err error) {
	principal, _ := PrincipalFrom(ctx)
	ctx, span := r.startSpan(ctx, "driftq.authz.evaluate",
		append(workflowSpanAttributes(runID, g.ID, effectiveTenantFromContext(ctx)), principalSpanAttributes(principal)...)...,
	)
	defer func() {
		if report != nil {
			if r.obs != nil {
				r.obs.observeAuthorization(*report)
			}
			r.finishSpan(span, err,
				attribute.Bool("driftq.authz.allowed", report.Allowed),
				attribute.String("driftq.authz.mode", string(report.Mode)),
			)
			return
		}
		r.finishSpan(span, err)
	}()

	reportValue, evalErr := r.EvaluateWorkflowAuthorization(ctx, runID, g)
	report = &reportValue
	err = evalErr
	if err != nil {
		return nil, err
	}

	r.appendAuditRecord(ctx, AuditRecord{
		TenantID:     report.TenantID,
		PrincipalID:  report.Principal.ID,
		Action:       "authorization.evaluate",
		ResourceType: "workflow",
		ResourceID:   report.WorkflowID,
		RunID:        report.RunID,
		WorkflowID:   report.WorkflowID,
		Outcome: func() AuditOutcome {
			if report.Allowed {
				return AuditOutcomeAllowed
			}
			return AuditOutcomeDenied
		}(),
		Reason: report.Reason,
	})

	if !report.Allowed {
		return report, &AuthorizationError{Report: *report}
	}

	return report, nil
}
