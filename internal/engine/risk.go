package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

var (
	ErrRiskBlocked          = errors.New("risk blocked")
	ErrRiskApprovalRequired = errors.New("risk approval required")
)

const riskPolicyKey = "risk/policy"

type RiskAction string
type RiskSeverity string
type RiskCategory string

const (
	RiskActionAllow           RiskAction = "allow"
	RiskActionSandbox         RiskAction = "sandbox"
	RiskActionRequireApproval RiskAction = "require_approval"
	RiskActionBlock           RiskAction = "block"
)

const (
	RiskSeverityLow      RiskSeverity = "low"
	RiskSeverityMedium   RiskSeverity = "medium"
	RiskSeverityHigh     RiskSeverity = "high"
	RiskSeverityCritical RiskSeverity = "critical"
)

const (
	RiskCategoryPromptInjection   RiskCategory = "prompt_injection"
	RiskCategorySuspiciousChain   RiskCategory = "suspicious_tool_chain"
	RiskCategoryUnusualDataMotion RiskCategory = "unusual_data_movement"
)

type RiskThresholds struct {
	SandboxScore  int `json:"sandbox_score,omitempty"`
	ApprovalScore int `json:"approval_score,omitempty"`
	BlockScore    int `json:"block_score,omitempty"`
}

type RiskPolicy struct {
	ID          string         `json:"id,omitempty"`
	Description string         `json:"description,omitempty"`
	UpdatedAt   *time.Time     `json:"updated_at,omitempty"`
	Thresholds  RiskThresholds `json:"thresholds,omitempty"`
}

type RiskFinding struct {
	Category RiskCategory `json:"category"`
	Severity RiskSeverity `json:"severity"`
	Score    int          `json:"score"`
	Summary  string       `json:"summary"`
	Evidence []string     `json:"evidence,omitempty"`
}

type WorkflowRiskReport struct {
	Allowed    bool           `json:"allowed"`
	Action     RiskAction     `json:"action"`
	Score      int            `json:"score"`
	Reason     string         `json:"reason,omitempty"`
	RunID      string         `json:"run_id,omitempty"`
	TenantID   string         `json:"tenant_id,omitempty"`
	WorkflowID string         `json:"workflow_id,omitempty"`
	Principal  Principal      `json:"principal"`
	Thresholds RiskThresholds `json:"thresholds"`
	Findings   []RiskFinding  `json:"findings,omitempty"`
}

type RuntimeRiskDecision struct {
	Action   RiskAction    `json:"action"`
	Score    int           `json:"score"`
	Findings []RiskFinding `json:"findings,omitempty"`
}

type RiskError struct {
	Report WorkflowRiskReport
}

func (e *RiskError) Error() string {
	if strings.TrimSpace(e.Report.Reason) != "" {
		return e.Report.Reason
	}

	switch e.Report.Action {
	case RiskActionRequireApproval:
		return ErrRiskApprovalRequired.Error()
	default:
		return ErrRiskBlocked.Error()
	}
}

func (e *RiskError) Unwrap() error {
	switch e.Report.Action {
	case RiskActionRequireApproval:
		return ErrRiskApprovalRequired
	default:
		return ErrRiskBlocked
	}
}

type ctxKeyRiskDecision struct{}

func WithRiskDecision(ctx context.Context, decision RuntimeRiskDecision) context.Context {
	return context.WithValue(ctx, ctxKeyRiskDecision{}, cloneRuntimeRiskDecision(decision))
}

func RiskDecisionFrom(ctx context.Context) (RuntimeRiskDecision, bool) {
	v := ctx.Value(ctxKeyRiskDecision{})
	decision, ok := v.(RuntimeRiskDecision)
	if !ok {
		return RuntimeRiskDecision{}, false
	}

	return cloneRuntimeRiskDecision(decision), true
}

func defaultRiskPolicy() RiskPolicy {
	return RiskPolicy{
		Thresholds: RiskThresholds{
			SandboxScore:  30,
			ApprovalScore: 60,
			BlockScore:    90,
		},
	}
}

func (p *RiskPolicy) NormalizeAndValidate() error {
	if p == nil {
		return fmt.Errorf("risk policy is nil")
	}

	p.ID = strings.TrimSpace(p.ID)
	p.Description = strings.TrimSpace(p.Description)
	if p.UpdatedAt != nil {
		t := p.UpdatedAt.UTC()
		p.UpdatedAt = &t
	}

	def := defaultRiskPolicy()
	if p.Thresholds.SandboxScore <= 0 {
		p.Thresholds.SandboxScore = def.Thresholds.SandboxScore
	}

	if p.Thresholds.ApprovalScore <= 0 {
		p.Thresholds.ApprovalScore = def.Thresholds.ApprovalScore
	}

	if p.Thresholds.BlockScore <= 0 {
		p.Thresholds.BlockScore = def.Thresholds.BlockScore
	}

	if p.Thresholds.SandboxScore >= p.Thresholds.ApprovalScore {
		return fmt.Errorf("risk policy sandbox_score must be lower than approval_score")
	}

	if p.Thresholds.ApprovalScore >= p.Thresholds.BlockScore {
		return fmt.Errorf("risk policy approval_score must be lower than block_score")
	}

	return nil
}

func cloneRiskPolicy(policy RiskPolicy) RiskPolicy {
	out := policy
	if policy.UpdatedAt != nil {
		t := policy.UpdatedAt.UTC()
		out.UpdatedAt = &t
	}

	return out
}

func cloneRuntimeRiskDecision(decision RuntimeRiskDecision) RuntimeRiskDecision {
	out := decision
	out.Findings = cloneRiskFindings(decision.Findings)
	return out
}

func cloneRiskFindings(findings []RiskFinding) []RiskFinding {
	if len(findings) == 0 {
		return nil
	}

	out := make([]RiskFinding, len(findings))
	for i, finding := range findings {
		out[i] = RiskFinding{
			Category: finding.Category,
			Severity: finding.Severity,
			Score:    finding.Score,
			Summary:  finding.Summary,
			Evidence: append([]string(nil), finding.Evidence...),
		}
	}

	return out
}

func (r *Runner) SaveRiskPolicy(policy RiskPolicy) error {
	if err := policy.NormalizeAndValidate(); err != nil {
		return err
	}

	now := time.Now().UTC()
	if policy.UpdatedAt == nil {
		policy.UpdatedAt = &now
	} else {
		t := policy.UpdatedAt.UTC()
		policy.UpdatedAt = &t
	}

	if err := putJSONKV(r.store, riskPolicyKey, policy); err != nil {
		return err
	}

	r.riskMu.Lock()
	r.riskPolicy = &policy
	r.riskMu.Unlock()
	return nil
}

func (r *Runner) GetRiskPolicy() (RiskPolicy, bool, error) {
	r.riskMu.RLock()
	if r.riskPolicy != nil {
		out := cloneRiskPolicy(*r.riskPolicy)
		r.riskMu.RUnlock()
		return out, true, nil
	}
	r.riskMu.RUnlock()

	var policy RiskPolicy
	ok, err := getJSONKV(r.store, riskPolicyKey, &policy)
	if err != nil || !ok {
		return policy, ok, err
	}

	if err := policy.NormalizeAndValidate(); err != nil {
		return RiskPolicy{}, true, err
	}

	r.riskMu.Lock()
	r.riskPolicy = &policy
	r.riskMu.Unlock()
	return cloneRiskPolicy(policy), true, nil
}

func (r *Runner) EvaluateRunSpecRisk(ctx context.Context, runID string, specJSON []byte, reg *HandlerRegistry, initialInput json.RawMessage) (WorkflowRiskReport, error) {
	g, spec, err := ParseWorkflowSpecJSON(specJSON)
	if err != nil {
		return WorkflowRiskReport{}, err
	}

	exec, err := CompileSpecToExecutable(spec, g, reg)
	if err != nil {
		return WorkflowRiskReport{}, err
	}

	return r.EvaluateWorkflowRisk(ctx, runID, exec, initialInput)
}

func (r *Runner) EvaluateWorkflowRisk(ctx context.Context, runID string, g WorkflowGraph, initialInput json.RawMessage) (WorkflowRiskReport, error) {
	policy, ok, err := r.GetRiskPolicy()
	if err != nil {
		return WorkflowRiskReport{}, err
	}

	if !ok {
		policy = defaultRiskPolicy()
	}

	if err := policy.NormalizeAndValidate(); err != nil {
		return WorkflowRiskReport{}, err
	}

	principal, _ := PrincipalFrom(ctx)
	principal.Normalize()

	tenantID := strings.TrimSpace(TenantIDFrom(ctx))
	if tenantID == "" {
		tenantID = principal.TenantID
	}

	report := WorkflowRiskReport{
		RunID:      strings.TrimSpace(runID),
		TenantID:   tenantID,
		WorkflowID: strings.TrimSpace(g.ID),
		Principal:  principal,
		Thresholds: policy.Thresholds,
	}

	findings := append([]RiskFinding(nil), detectPromptInjection(initialInput)...)
	findings = append(findings, detectSuspiciousToolChains(g)...)
	findings = append(findings, detectUnusualDataMovement(g, initialInput)...)

	sort.Slice(findings, func(i, j int) bool {
		if findings[i].Score == findings[j].Score {
			return findings[i].Summary < findings[j].Summary
		}
		return findings[i].Score > findings[j].Score
	})

	score := 0
	for _, finding := range findings {
		score += finding.Score
	}

	if score > 100 {
		score = 100
	}

	report.Score = score
	report.Findings = cloneRiskFindings(findings)
	report.Action = riskActionForScore(score, policy.Thresholds)
	report.Allowed = report.Action == RiskActionAllow || report.Action == RiskActionSandbox

	switch report.Action {
	case RiskActionAllow:
		report.Reason = "risk score below sandbox threshold"
	case RiskActionSandbox:
		report.Reason = "risk score requires sandbox execution"
	case RiskActionRequireApproval:
		report.Reason = "risk score requires manual approval"
	case RiskActionBlock:
		report.Reason = "risk score requires blocking execution"
	}

	return report, nil
}

func (r *Runner) evaluateAndEnforceRisk(ctx context.Context, runID string, g WorkflowGraph, initialInput json.RawMessage) (report WorkflowRiskReport, outCtx context.Context, err error) {
	principal, _ := PrincipalFrom(ctx)
	ctx, span := r.startSpan(ctx, "driftq.risk.evaluate",
		append(workflowSpanAttributes(runID, g.ID, effectiveTenantFromContext(ctx)), principalSpanAttributes(principal)...)...,
	)
	defer func() {
		if r.obs != nil && report.Action != "" {
			r.obs.observeRisk(report)
		}
		outCtx = ctx
		if report.Action != "" {
			r.finishSpan(span, err,
				attribute.String("driftq.risk.action", string(report.Action)),
				attribute.Int("driftq.risk.score", report.Score),
				attribute.Bool("driftq.risk.allowed", report.Allowed),
			)
			return
		}
		r.finishSpan(span, err)
	}()

	report, err = r.EvaluateWorkflowRisk(ctx, runID, g, initialInput)
	if err != nil {
		return WorkflowRiskReport{}, ctx, err
	}

	if report.Action == RiskActionRequireApproval {
		if task, ok, err := r.findHumanTask(runID, "", 0, HumanTaskSourceRisk); err == nil && ok {
			switch task.Status {
			case HumanTaskApproved:
				report.Action = RiskActionAllow
				report.Allowed = true
				report.Reason = "manual risk approval granted"
			case HumanTaskRejected, HumanTaskTimedOut, HumanTaskCanceled:
				report.Action = RiskActionBlock
				report.Allowed = false
				report.Reason = "manual risk approval rejected"
			case HumanTaskPending:
				report.Reason = "waiting for manual risk approval"
			}
		}
	}

	r.appendAuditRecord(ctx, AuditRecord{
		TenantID:     report.TenantID,
		PrincipalID:  report.Principal.ID,
		Action:       "risk.evaluate",
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

	ctx = WithRiskDecision(ctx, RuntimeRiskDecision{
		Action:   report.Action,
		Score:    report.Score,
		Findings: report.Findings,
	})

	if !report.Allowed {
		return report, ctx, &RiskError{Report: report}
	}

	return report, ctx, nil
}

func riskActionForScore(score int, thresholds RiskThresholds) RiskAction {
	switch {
	case score >= thresholds.BlockScore:
		return RiskActionBlock
	case score >= thresholds.ApprovalScore:
		return RiskActionRequireApproval
	case score >= thresholds.SandboxScore:
		return RiskActionSandbox
	default:
		return RiskActionAllow
	}
}

func detectPromptInjection(initialInput json.RawMessage) []RiskFinding {
	values := collectJSONStrings(initialInput)
	if len(values) == 0 && len(initialInput) > 0 {
		values = []string{string(initialInput)}
	}

	patterns := []string{
		"ignore previous instructions",
		"ignore prior instructions",
		"ignore system prompt",
		"reveal system prompt",
		"developer instructions",
		"bypass safety",
		"disable guardrails",
		"jailbreak",
		"exfiltrate",
		"leak secrets",
		"reveal secrets",
		"override policy",
	}

	evidence := []string{}
	seen := map[string]struct{}{}
	for _, value := range values {
		sample := strings.TrimSpace(value)
		if sample == "" {
			continue
		}

		lower := strings.ToLower(sample)
		for _, pattern := range patterns {
			if !strings.Contains(lower, pattern) {
				continue
			}

			snippet := sample
			if len(snippet) > 96 {
				snippet = snippet[:96]
			}

			if _, ok := seen[snippet]; ok {
				break
			}
			seen[snippet] = struct{}{}
			evidence = append(evidence, snippet)
			break
		}
	}

	if len(evidence) == 0 {
		return nil
	}

	score := 45
	if len(evidence) > 1 {
		score += 10
	}

	if len(evidence) > 2 {
		score += 10
	}

	if score > 65 {
		score = 65
	}

	return []RiskFinding{{
		Category: RiskCategoryPromptInjection,
		Severity: severityForScore(score),
		Score:    score,
		Summary:  "input contains prompt-injection-shaped instructions",
		Evidence: evidence,
	}}
}

func detectSuspiciousToolChains(g WorkflowGraph) []RiskFinding {
	pairs := workflowTopicPairs(g)
	findings := make([]RiskFinding, 0, 2)
	seen := map[string]struct{}{}

	for _, pair := range pairs {
		fromProfile := classifyTopic(pair.FromTopic)
		toProfile := classifyTopic(pair.ToTopic)

		if fromProfile.Network && toProfile.Exec {
			summary := "workflow chains network retrieval into command execution"
			if _, ok := seen[summary]; !ok {
				seen[summary] = struct{}{}
				findings = append(findings, RiskFinding{
					Category: RiskCategorySuspiciousChain,
					Severity: RiskSeverityHigh,
					Score:    45,
					Summary:  summary,
					Evidence: []string{pair.FromTopic + " -> " + pair.ToTopic},
				})
			}
		}
	}

	if len(findings) > 0 {
		return findings
	}

	hasNetwork := false
	hasExec := false
	for _, node := range g.Nodes {
		profile := classifyTopic(node.Topic)
		hasNetwork = hasNetwork || profile.Network
		hasExec = hasExec || profile.Exec
	}

	if hasNetwork && hasExec {
		return []RiskFinding{{
			Category: RiskCategorySuspiciousChain,
			Severity: RiskSeverityMedium,
			Score:    35,
			Summary:  "workflow mixes outbound network access with command execution",
		}}
	}

	return nil
}

func detectUnusualDataMovement(g WorkflowGraph, initialInput json.RawMessage) []RiskFinding {
	findings := []RiskFinding{}
	seen := map[string]struct{}{}
	pairs := workflowTopicPairs(g)

	for _, pair := range pairs {
		fromProfile := classifyTopic(pair.FromTopic)
		toProfile := classifyTopic(pair.ToTopic)
		if (fromProfile.Secret || fromProfile.Database || fromProfile.Filesystem) && toProfile.Network {
			summary := "workflow moves internal or sensitive data toward outbound tools"
			score := 65
			if fromProfile.Secret {
				score = 75
			}

			if _, ok := seen[summary]; !ok {
				seen[summary] = struct{}{}
				findings = append(findings, RiskFinding{
					Category: RiskCategoryUnusualDataMotion,
					Severity: severityForScore(score),
					Score:    score,
					Summary:  summary,
					Evidence: []string{pair.FromTopic + " -> " + pair.ToTopic},
				})
			}
		}
	}

	sensitiveKeys := collectSensitiveJSONKeys(initialInput)
	if len(sensitiveKeys) > 0 && workflowHasOutboundTopic(g) {
		summary := "workflow combines sensitive input fields with outbound delivery tools"
		if _, ok := seen[summary]; !ok {
			sort.Strings(sensitiveKeys)
			findings = append(findings, RiskFinding{
				Category: RiskCategoryUnusualDataMotion,
				Severity: RiskSeverityHigh,
				Score:    40,
				Summary:  summary,
				Evidence: sensitiveKeys,
			})
		}
	}

	return findings
}

func severityForScore(score int) RiskSeverity {
	switch {
	case score >= 75:
		return RiskSeverityCritical
	case score >= 45:
		return RiskSeverityHigh
	case score >= 25:
		return RiskSeverityMedium
	default:
		return RiskSeverityLow
	}
}

type workflowTopicPair struct {
	FromTopic string
	ToTopic   string
}

func workflowTopicPairs(g WorkflowGraph) []workflowTopicPair {
	if len(g.Edges) > 0 {
		byNode := map[string]string{}
		for _, node := range g.Nodes {
			byNode[node.NodeID] = strings.TrimSpace(node.Topic)
		}

		out := []workflowTopicPair{}
		for _, edge := range g.Edges {
			fromTopic := byNode[edge.From]
			toTopic := byNode[edge.To]

			if fromTopic == "" || toTopic == "" {
				continue
			}

			out = append(out, workflowTopicPair{FromTopic: fromTopic, ToTopic: toTopic})
		}
		return out
	}

	out := make([]workflowTopicPair, 0, len(g.Nodes))
	for i := 0; i+1 < len(g.Nodes); i++ {
		fromTopic := strings.TrimSpace(g.Nodes[i].Topic)
		toTopic := strings.TrimSpace(g.Nodes[i+1].Topic)
		if fromTopic == "" || toTopic == "" {
			continue
		}
		out = append(out, workflowTopicPair{FromTopic: fromTopic, ToTopic: toTopic})
	}
	return out
}

type topicProfile struct {
	Network    bool
	Exec       bool
	Filesystem bool
	Database   bool
	Secret     bool
}

func classifyTopic(topic string) topicProfile {
	topic = strings.ToLower(strings.TrimSpace(topic))
	return topicProfile{
		Network:    containsAny(topic, "http", "https", "web", "fetch", "request", "upload", "download", "post", "send", "email", "smtp", "slack", "webhook"),
		Exec:       containsAny(topic, "shell", "exec", "cmd", "powershell", "bash", "terminal", "process"),
		Filesystem: containsAny(topic, "file", "fs", "disk", "blob", "bucket", "s3", "storage"),
		Database:   containsAny(topic, "db", "sql", "query", "warehouse", "table", "record"),
		Secret:     containsAny(topic, "secret", "vault", "credential", "token", "private_key", "api_key", "passwd", "password"),
	}
}

func workflowHasOutboundTopic(g WorkflowGraph) bool {
	for _, node := range g.Nodes {
		if classifyTopic(node.Topic).Network {
			return true
		}
	}
	return false
}

func collectJSONStrings(raw json.RawMessage) []string {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil
	}

	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil
	}

	out := []string{}
	collectStringsRecursive(v, &out)
	return out
}

func collectStringsRecursive(v any, out *[]string) {
	switch cur := v.(type) {
	case string:
		*out = append(*out, cur)
	case []any:
		for _, item := range cur {
			collectStringsRecursive(item, out)
		}
	case map[string]any:
		for _, item := range cur {
			collectStringsRecursive(item, out)
		}
	}
}

func collectSensitiveJSONKeys(raw json.RawMessage) []string {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil
	}

	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil
	}

	seen := map[string]struct{}{}
	collectSensitiveKeysRecursive(v, seen)
	if len(seen) == 0 {
		return nil
	}

	out := make([]string, 0, len(seen))
	for key := range seen {
		out = append(out, key)
	}
	return out
}

func collectSensitiveKeysRecursive(v any, seen map[string]struct{}) {
	switch cur := v.(type) {
	case []any:
		for _, item := range cur {
			collectSensitiveKeysRecursive(item, seen)
		}
	case map[string]any:
		for key, item := range cur {
			lower := strings.ToLower(strings.TrimSpace(key))
			if containsAny(lower, "password", "secret", "token", "credential", "api_key", "private_key", "ssn", "pii") {
				seen[key] = struct{}{}
			}
			collectSensitiveKeysRecursive(item, seen)
		}
	}
}

func containsAny(value string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}
