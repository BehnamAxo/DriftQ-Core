package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"
)

const (
	workflowReleaseVersionIndexPrefix = "workflow_releases/versions/by_workflow/"
)

type WorkflowEnvironment string

const (
	WorkflowEnvironmentStaging WorkflowEnvironment = "staging"
	WorkflowEnvironmentProd    WorkflowEnvironment = "prod"
)

type WorkflowReleaseVersion struct {
	ID                    string                     `json:"id"`
	WorkflowID            string                     `json:"workflow_id"`
	Spec                  json.RawMessage            `json:"spec"`
	PromptSnapshot        json.RawMessage            `json:"prompt_snapshot,omitempty"`
	ModelSnapshot         json.RawMessage            `json:"model_snapshot,omitempty"`
	ToolGatewaySnapshot   *ToolGatewayBundle         `json:"tool_gateway_snapshot,omitempty"`
	AuthorizationSnapshot *AuthorizationPolicyBundle `json:"authorization_snapshot,omitempty"`
	RiskSnapshot          *RiskPolicy                `json:"risk_snapshot,omitempty"`
	SourceRunID           string                     `json:"source_run_id,omitempty"`
	EvalRunID             string                     `json:"eval_run_id,omitempty"`
	CreatedAt             *time.Time                 `json:"created_at,omitempty"`
	UpdatedAt             *time.Time                 `json:"updated_at,omitempty"`
}

type WorkflowReleaseChannel struct {
	WorkflowID                   string              `json:"workflow_id"`
	Environment                  WorkflowEnvironment `json:"environment"`
	ActiveVersionID              string              `json:"active_version_id,omitempty"`
	PreviousVersionID            string              `json:"previous_version_id,omitempty"`
	CanaryVersionID              string              `json:"canary_version_id,omitempty"`
	CanaryPercent                int                 `json:"canary_percent,omitempty"`
	ShadowVersionIDs             []string            `json:"shadow_version_ids,omitempty"`
	AutoRollbackOnEvalRegression bool                `json:"auto_rollback_on_eval_regression,omitempty"`
	LastEvalRunID                string              `json:"last_eval_run_id,omitempty"`
	CreatedAt                    *time.Time          `json:"created_at,omitempty"`
	UpdatedAt                    *time.Time          `json:"updated_at,omitempty"`
}

type WorkflowReleaseResolution struct {
	WorkflowID       string              `json:"workflow_id"`
	Environment      WorkflowEnvironment `json:"environment"`
	PrimaryVersionID string              `json:"primary_version_id"`
	ShadowVersionIDs []string            `json:"shadow_version_ids,omitempty"`
	Canary           bool                `json:"canary"`
	Reason           string              `json:"reason,omitempty"`
}

type WorkflowReleaseDiffDetail struct {
	Changed bool            `json:"changed"`
	From    json.RawMessage `json:"from,omitempty"`
	To      json.RawMessage `json:"to,omitempty"`
}

type WorkflowReleaseDiff struct {
	WorkflowID     string                    `json:"workflow_id"`
	FromVersionID  string                    `json:"from_version_id"`
	ToVersionID    string                    `json:"to_version_id"`
	Spec           WorkflowReleaseDiffDetail `json:"spec"`
	Prompt         WorkflowReleaseDiffDetail `json:"prompt"`
	Model          WorkflowReleaseDiffDetail `json:"model"`
	Tool           WorkflowReleaseDiffDetail `json:"tool"`
	Policy         WorkflowReleaseDiffDetail `json:"policy"`
	ChangedDomains []string                  `json:"changed_domains,omitempty"`
}

func workflowReleaseVersionKey(id string) string {
	return "workflow_releases/versions/" + strings.TrimSpace(id)
}

func workflowReleaseVersionIndexKey(workflowID string) string {
	return workflowReleaseVersionIndexPrefix + strings.TrimSpace(workflowID)
}

func workflowReleaseChannelKey(workflowID string, env WorkflowEnvironment) string {
	return "workflow_releases/channels/" + strings.TrimSpace(workflowID) + "/" + strings.TrimSpace(string(env))
}

func normalizeWorkflowEnvironment(env WorkflowEnvironment) (WorkflowEnvironment, error) {
	env = WorkflowEnvironment(strings.ToLower(strings.TrimSpace(string(env))))
	switch env {
	case WorkflowEnvironmentStaging, WorkflowEnvironmentProd:
		return env, nil
	default:
		return "", fmt.Errorf("invalid environment %q", env)
	}
}

func (v *WorkflowReleaseVersion) NormalizeAndValidate() error {
	if v == nil {
		return fmt.Errorf("workflow release version is nil")
	}

	v.ID = strings.TrimSpace(v.ID)
	v.WorkflowID = strings.TrimSpace(v.WorkflowID)
	v.SourceRunID = strings.TrimSpace(v.SourceRunID)
	v.EvalRunID = strings.TrimSpace(v.EvalRunID)
	v.Spec = cloneRaw(v.Spec)
	v.PromptSnapshot = cloneRaw(v.PromptSnapshot)
	v.ModelSnapshot = cloneRaw(v.ModelSnapshot)

	if v.ID == "" {
		return fmt.Errorf("workflow release version id is required")
	}

	if len(v.Spec) == 0 {
		return fmt.Errorf("workflow release version spec is required")
	}

	g, _, err := ParseWorkflowSpecJSON(v.Spec)
	if err != nil {
		return fmt.Errorf("workflow release version spec invalid: %w", err)
	}

	if v.WorkflowID == "" {
		v.WorkflowID = strings.TrimSpace(g.ID)
	}

	if v.WorkflowID == "" {
		return fmt.Errorf("workflow release workflow_id is required")
	}

	if strings.TrimSpace(g.ID) != "" && strings.TrimSpace(g.ID) != v.WorkflowID {
		return fmt.Errorf("workflow release workflow_id %q does not match spec id %q", v.WorkflowID, strings.TrimSpace(g.ID))
	}

	if v.ToolGatewaySnapshot != nil {
		normalized := cloneToolGatewayBundle(*v.ToolGatewaySnapshot)
		if err := normalized.NormalizeAndValidate(); err != nil {
			return fmt.Errorf("tool gateway snapshot invalid: %w", err)
		}
		v.ToolGatewaySnapshot = &normalized
	}

	if v.AuthorizationSnapshot != nil {
		normalized := cloneAuthorizationPolicyBundle(*v.AuthorizationSnapshot)
		if err := normalized.NormalizeAndValidate(); err != nil {
			return fmt.Errorf("authorization snapshot invalid: %w", err)
		}
		v.AuthorizationSnapshot = &normalized
	}

	if v.RiskSnapshot != nil {
		normalized := cloneRiskPolicy(*v.RiskSnapshot)
		if err := normalized.NormalizeAndValidate(); err != nil {
			return fmt.Errorf("risk snapshot invalid: %w", err)
		}
		v.RiskSnapshot = &normalized
	}

	now := time.Now().UTC()
	if v.CreatedAt == nil || v.CreatedAt.IsZero() {
		v.CreatedAt = &now
	}

	v.UpdatedAt = &now
	return nil
}

func (c *WorkflowReleaseChannel) NormalizeAndValidate() error {
	if c == nil {
		return fmt.Errorf("workflow release channel is nil")
	}

	c.WorkflowID = strings.TrimSpace(c.WorkflowID)
	if c.WorkflowID == "" {
		return fmt.Errorf("workflow_id is required")
	}

	env, err := normalizeWorkflowEnvironment(c.Environment)
	if err != nil {
		return err
	}

	c.Environment = env
	c.ActiveVersionID = strings.TrimSpace(c.ActiveVersionID)
	c.PreviousVersionID = strings.TrimSpace(c.PreviousVersionID)
	c.CanaryVersionID = strings.TrimSpace(c.CanaryVersionID)
	c.LastEvalRunID = strings.TrimSpace(c.LastEvalRunID)

	if c.CanaryPercent < 0 || c.CanaryPercent > 100 {
		return fmt.Errorf("canary_percent must be between 0 and 100")
	}

	if c.CanaryVersionID == "" {
		c.CanaryPercent = 0
	}

	c.ShadowVersionIDs = dedupeSortedStrings(c.ShadowVersionIDs)
	now := time.Now().UTC()

	if c.CreatedAt == nil || c.CreatedAt.IsZero() {
		c.CreatedAt = &now
	}

	c.UpdatedAt = &now
	return nil
}

func cloneWorkflowReleaseVersion(v WorkflowReleaseVersion) WorkflowReleaseVersion {
	out := v
	out.ID = strings.TrimSpace(v.ID)
	out.WorkflowID = strings.TrimSpace(v.WorkflowID)
	out.Spec = cloneRaw(v.Spec)
	out.PromptSnapshot = cloneRaw(v.PromptSnapshot)
	out.ModelSnapshot = cloneRaw(v.ModelSnapshot)
	out.SourceRunID = strings.TrimSpace(v.SourceRunID)
	out.EvalRunID = strings.TrimSpace(v.EvalRunID)

	if v.ToolGatewaySnapshot != nil {
		normalized := cloneToolGatewayBundle(*v.ToolGatewaySnapshot)
		out.ToolGatewaySnapshot = &normalized
	}

	if v.AuthorizationSnapshot != nil {
		normalized := cloneAuthorizationPolicyBundle(*v.AuthorizationSnapshot)
		out.AuthorizationSnapshot = &normalized
	}

	if v.RiskSnapshot != nil {
		normalized := cloneRiskPolicy(*v.RiskSnapshot)
		out.RiskSnapshot = &normalized
	}

	if v.CreatedAt != nil {
		ts := v.CreatedAt.UTC()
		out.CreatedAt = &ts
	}

	if v.UpdatedAt != nil {
		ts := v.UpdatedAt.UTC()
		out.UpdatedAt = &ts
	}

	return out
}

func cloneWorkflowReleaseChannel(c WorkflowReleaseChannel) WorkflowReleaseChannel {
	out := c
	out.WorkflowID = strings.TrimSpace(c.WorkflowID)
	out.Environment = WorkflowEnvironment(strings.TrimSpace(string(c.Environment)))
	out.ActiveVersionID = strings.TrimSpace(c.ActiveVersionID)
	out.PreviousVersionID = strings.TrimSpace(c.PreviousVersionID)
	out.CanaryVersionID = strings.TrimSpace(c.CanaryVersionID)
	out.ShadowVersionIDs = append([]string(nil), c.ShadowVersionIDs...)
	out.LastEvalRunID = strings.TrimSpace(c.LastEvalRunID)

	if c.CreatedAt != nil {
		ts := c.CreatedAt.UTC()
		out.CreatedAt = &ts
	}

	if c.UpdatedAt != nil {
		ts := c.UpdatedAt.UTC()
		out.UpdatedAt = &ts
	}

	return out
}

func (r *Runner) SaveWorkflowReleaseVersion(version WorkflowReleaseVersion) error {
	if version.ToolGatewaySnapshot == nil {
		if bundle, ok, err := r.GetToolGatewayBundle(); err == nil && ok {
			normalized := cloneToolGatewayBundle(bundle)
			version.ToolGatewaySnapshot = &normalized
		}
	}

	if version.AuthorizationSnapshot == nil {
		if bundle, ok, err := r.GetAuthorizationPolicy(); err == nil && ok {
			normalized := cloneAuthorizationPolicyBundle(bundle)
			version.AuthorizationSnapshot = &normalized
		}
	}

	if version.RiskSnapshot == nil {
		if policy, ok, err := r.GetRiskPolicy(); err == nil && ok {
			normalized := cloneRiskPolicy(policy)
			version.RiskSnapshot = &normalized
		}
	}

	if err := version.NormalizeAndValidate(); err != nil {
		return err
	}

	if err := putJSONKV(r.store, workflowReleaseVersionKey(version.ID), version); err != nil {
		return err
	}

	if err := upsertIndexID(r.store, workflowReleaseVersionIndexKey(version.WorkflowID), version.ID); err != nil {
		return err
	}

	r.appendAuditRecord(context.Background(), AuditRecord{
		Action:       "workflow_release.version.save",
		ResourceType: "workflow_release_version",
		ResourceID:   version.ID,
		WorkflowID:   version.WorkflowID,
		Outcome:      AuditOutcomeAllowed,
		Reason:       "workflow release version saved",
	})

	return nil
}

func (r *Runner) GetWorkflowReleaseVersion(id string) (WorkflowReleaseVersion, bool, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return WorkflowReleaseVersion{}, false, nil
	}

	var version WorkflowReleaseVersion
	ok, err := getJSONKV(r.store, workflowReleaseVersionKey(id), &version)

	if err != nil || !ok {
		return WorkflowReleaseVersion{}, ok, err
	}

	return cloneWorkflowReleaseVersion(version), true, nil
}

func (r *Runner) ListWorkflowReleaseVersions(workflowID string) ([]WorkflowReleaseVersion, error) {
	workflowID = strings.TrimSpace(workflowID)
	if workflowID == "" {
		return nil, fmt.Errorf("workflow_id is required")
	}

	ids, err := indexIDs(r.store, workflowReleaseVersionIndexKey(workflowID))
	if err != nil {
		return nil, err
	}

	out := make([]WorkflowReleaseVersion, 0, len(ids))
	for _, id := range ids {
		version, ok, err := r.GetWorkflowReleaseVersion(id)
		if err != nil || !ok {
			continue
		}
		out = append(out, version)
	}

	sort.Slice(out, func(i, j int) bool {
		left := out[i].ID
		right := out[j].ID
		if out[i].CreatedAt != nil && out[j].CreatedAt != nil && !out[i].CreatedAt.Equal(*out[j].CreatedAt) {
			return out[i].CreatedAt.Before(*out[j].CreatedAt)
		}
		return left < right
	})
	return out, nil
}

func (r *Runner) SaveWorkflowReleaseChannel(channel WorkflowReleaseChannel) error {
	if err := channel.NormalizeAndValidate(); err != nil {
		return err
	}

	for _, versionID := range append(append([]string{}, channel.ActiveVersionID, channel.PreviousVersionID, channel.CanaryVersionID), channel.ShadowVersionIDs...) {
		versionID = strings.TrimSpace(versionID)
		if versionID == "" {
			continue
		}

		version, ok, err := r.GetWorkflowReleaseVersion(versionID)
		if err != nil {
			return err
		}

		if !ok {
			return fmt.Errorf("workflow release version %q not found", versionID)
		}

		if version.WorkflowID != channel.WorkflowID {
			return fmt.Errorf("workflow release version %q belongs to workflow %q, not %q", versionID, version.WorkflowID, channel.WorkflowID)
		}
	}

	if err := putJSONKV(r.store, workflowReleaseChannelKey(channel.WorkflowID, channel.Environment), channel); err != nil {
		return err
	}

	return nil
}

func (r *Runner) GetWorkflowReleaseChannel(workflowID string, env WorkflowEnvironment) (WorkflowReleaseChannel, bool, error) {
	workflowID = strings.TrimSpace(workflowID)
	if workflowID == "" {
		return WorkflowReleaseChannel{}, false, nil
	}

	normalizedEnv, err := normalizeWorkflowEnvironment(env)
	if err != nil {
		return WorkflowReleaseChannel{}, false, err
	}

	var channel WorkflowReleaseChannel
	ok, err := getJSONKV(r.store, workflowReleaseChannelKey(workflowID, normalizedEnv), &channel)

	if err != nil || !ok {
		return WorkflowReleaseChannel{}, ok, err
	}

	return cloneWorkflowReleaseChannel(channel), true, nil
}

func (r *Runner) PromoteWorkflowRelease(ctx context.Context, workflowID string, env WorkflowEnvironment, versionID string) (WorkflowReleaseChannel, error) {
	workflowID = strings.TrimSpace(workflowID)
	versionID = strings.TrimSpace(versionID)
	if workflowID == "" || versionID == "" {
		return WorkflowReleaseChannel{}, fmt.Errorf("workflow_id and version_id are required")
	}

	version, ok, err := r.GetWorkflowReleaseVersion(versionID)
	if err != nil {
		return WorkflowReleaseChannel{}, err
	}

	if !ok {
		return WorkflowReleaseChannel{}, fmt.Errorf("workflow release version %q not found", versionID)
	}

	if version.WorkflowID != workflowID {
		return WorkflowReleaseChannel{}, fmt.Errorf("workflow release version %q belongs to workflow %q, not %q", versionID, version.WorkflowID, workflowID)
	}

	channel, ok, err := r.GetWorkflowReleaseChannel(workflowID, env)
	if err != nil {
		return WorkflowReleaseChannel{}, err
	}

	if !ok {
		channel = WorkflowReleaseChannel{
			WorkflowID:  workflowID,
			Environment: env,
			CreatedAt:   nil,
			UpdatedAt:   nil,
		}
	}

	if strings.TrimSpace(channel.ActiveVersionID) != "" && strings.TrimSpace(channel.ActiveVersionID) != versionID {
		channel.PreviousVersionID = channel.ActiveVersionID
	}

	channel.ActiveVersionID = versionID
	channel.CanaryVersionID = ""
	channel.CanaryPercent = 0
	channel.LastEvalRunID = version.EvalRunID

	if err := r.SaveWorkflowReleaseChannel(channel); err != nil {
		return WorkflowReleaseChannel{}, err
	}

	r.appendAuditRecord(ctx, AuditRecord{
		TenantID:     effectiveTenantFromContext(ctx),
		Action:       "workflow_release.promote",
		ResourceType: "workflow_release_channel",
		ResourceID:   workflowID + "/" + string(env),
		WorkflowID:   workflowID,
		Outcome:      AuditOutcomeAllowed,
		Reason:       "workflow release promoted",
	})

	saved, _, err := r.GetWorkflowReleaseChannel(workflowID, env)
	return saved, err
}

func (r *Runner) RollbackWorkflowRelease(ctx context.Context, workflowID string, env WorkflowEnvironment, versionID string) (WorkflowReleaseChannel, error) {
	channel, ok, err := r.GetWorkflowReleaseChannel(workflowID, env)
	if err != nil {
		return WorkflowReleaseChannel{}, err
	}

	if !ok {
		return WorkflowReleaseChannel{}, fmt.Errorf("workflow release channel %q/%q not found", workflowID, env)
	}

	versionID = strings.TrimSpace(versionID)
	if versionID == "" {
		versionID = strings.TrimSpace(channel.PreviousVersionID)
	}

	if versionID == "" {
		return WorkflowReleaseChannel{}, fmt.Errorf("rollback target version is required")
	}

	version, ok, err := r.GetWorkflowReleaseVersion(versionID)
	if err != nil {
		return WorkflowReleaseChannel{}, err
	}

	if !ok {
		return WorkflowReleaseChannel{}, fmt.Errorf("workflow release version %q not found", versionID)
	}

	if version.WorkflowID != strings.TrimSpace(workflowID) {
		return WorkflowReleaseChannel{}, fmt.Errorf("workflow release version %q belongs to workflow %q, not %q", versionID, version.WorkflowID, workflowID)
	}

	channel.PreviousVersionID = channel.ActiveVersionID
	channel.ActiveVersionID = versionID
	channel.CanaryVersionID = ""
	channel.CanaryPercent = 0

	if err := r.SaveWorkflowReleaseChannel(channel); err != nil {
		return WorkflowReleaseChannel{}, err
	}

	r.appendAuditRecord(ctx, AuditRecord{
		TenantID:     effectiveTenantFromContext(ctx),
		Action:       "workflow_release.rollback",
		ResourceType: "workflow_release_channel",
		ResourceID:   workflowID + "/" + string(env),
		WorkflowID:   workflowID,
		Outcome:      AuditOutcomeAllowed,
		Reason:       "workflow release rolled back",
	})

	saved, _, err := r.GetWorkflowReleaseChannel(workflowID, env)
	return saved, err
}

func stableReleasePercentBucket(parts ...string) int {
	h := fnv.New32a()

	for _, part := range parts {
		_, _ = h.Write([]byte(strings.TrimSpace(part)))
		_, _ = h.Write([]byte{0})
	}

	return int(h.Sum32() % 100)
}

func (r *Runner) ResolveWorkflowRelease(workflowID string, env WorkflowEnvironment, runID string) (WorkflowReleaseResolution, error) {
	channel, ok, err := r.GetWorkflowReleaseChannel(workflowID, env)
	if err != nil {
		return WorkflowReleaseResolution{}, err
	}

	if !ok {
		return WorkflowReleaseResolution{}, fmt.Errorf("workflow release channel %q/%q not found", workflowID, env)
	}

	if strings.TrimSpace(channel.ActiveVersionID) == "" && strings.TrimSpace(channel.CanaryVersionID) == "" {
		return WorkflowReleaseResolution{}, fmt.Errorf("workflow release channel %q/%q has no active or canary version", workflowID, env)
	}

	resolution := WorkflowReleaseResolution{
		WorkflowID:       workflowID,
		Environment:      channel.Environment,
		PrimaryVersionID: channel.ActiveVersionID,
		ShadowVersionIDs: append([]string(nil), channel.ShadowVersionIDs...),
	}

	if strings.TrimSpace(channel.CanaryVersionID) != "" && channel.CanaryPercent > 0 {
		bucketInput := strings.TrimSpace(runID)

		if bucketInput == "" {
			bucketInput = "resolution"
		}

		if stableReleasePercentBucket(workflowID, string(env), bucketInput) < channel.CanaryPercent {
			resolution.PrimaryVersionID = channel.CanaryVersionID
			resolution.Canary = true
			resolution.Reason = fmt.Sprintf("selected canary version at %d%% rollout", channel.CanaryPercent)
		}
	}

	if resolution.PrimaryVersionID == "" {
		resolution.PrimaryVersionID = channel.CanaryVersionID
	}

	if resolution.Reason == "" {
		resolution.Reason = "selected active version"
	}

	return resolution, nil
}

func marshalReleaseSnapshot(value any) json.RawMessage {
	if value == nil {
		return nil
	}

	switch typed := value.(type) {
	case json.RawMessage:
		return cloneRaw(typed)
	default:
		b, err := json.Marshal(value)
		if err != nil {
			return nil
		}
		var compacted bytes.Buffer
		if err := json.Compact(&compacted, b); err != nil {
			return json.RawMessage(b)
		}
		return json.RawMessage(compacted.String())
	}
}

func buildPolicySnapshot(version WorkflowReleaseVersion) json.RawMessage {
	payload := map[string]any{}
	if version.AuthorizationSnapshot != nil {
		payload["authorization"] = version.AuthorizationSnapshot
	}

	if version.RiskSnapshot != nil {
		payload["risk"] = version.RiskSnapshot
	}

	if len(payload) == 0 {
		return nil
	}

	return marshalReleaseSnapshot(payload)
}

func releaseDiffDetail(from, to json.RawMessage) WorkflowReleaseDiffDetail {
	if len(from) == 0 && len(to) == 0 {
		return WorkflowReleaseDiffDetail{}
	}

	changed := !jsonEqual(from, to)
	return WorkflowReleaseDiffDetail{
		Changed: changed,
		From:    cloneRaw(from),
		To:      cloneRaw(to),
	}
}

func (r *Runner) DiffWorkflowReleaseVersions(workflowID, fromVersionID, toVersionID string) (WorkflowReleaseDiff, error) {
	fromVersion, ok, err := r.GetWorkflowReleaseVersion(fromVersionID)
	if err != nil {
		return WorkflowReleaseDiff{}, err
	}

	if !ok {
		return WorkflowReleaseDiff{}, fmt.Errorf("workflow release version %q not found", fromVersionID)
	}

	toVersion, ok, err := r.GetWorkflowReleaseVersion(toVersionID)
	if err != nil {
		return WorkflowReleaseDiff{}, err
	}

	if !ok {
		return WorkflowReleaseDiff{}, fmt.Errorf("workflow release version %q not found", toVersionID)
	}

	workflowID = strings.TrimSpace(workflowID)
	if workflowID == "" {
		workflowID = fromVersion.WorkflowID
	}

	if fromVersion.WorkflowID != workflowID || toVersion.WorkflowID != workflowID {
		return WorkflowReleaseDiff{}, fmt.Errorf("workflow release diff versions must belong to workflow %q", workflowID)
	}

	diff := WorkflowReleaseDiff{
		WorkflowID:    workflowID,
		FromVersionID: fromVersionID,
		ToVersionID:   toVersionID,
		Spec:          releaseDiffDetail(fromVersion.Spec, toVersion.Spec),
		Prompt:        releaseDiffDetail(fromVersion.PromptSnapshot, toVersion.PromptSnapshot),
		Model:         releaseDiffDetail(fromVersion.ModelSnapshot, toVersion.ModelSnapshot),
		Tool:          releaseDiffDetail(marshalReleaseSnapshot(fromVersion.ToolGatewaySnapshot), marshalReleaseSnapshot(toVersion.ToolGatewaySnapshot)),
		Policy:        releaseDiffDetail(buildPolicySnapshot(fromVersion), buildPolicySnapshot(toVersion)),
	}

	if diff.Spec.Changed {
		diff.ChangedDomains = append(diff.ChangedDomains, "spec")
	}

	if diff.Prompt.Changed {
		diff.ChangedDomains = append(diff.ChangedDomains, "prompt")
	}

	if diff.Model.Changed {
		diff.ChangedDomains = append(diff.ChangedDomains, "model")
	}

	if diff.Tool.Changed {
		diff.ChangedDomains = append(diff.ChangedDomains, "tool")
	}

	if diff.Policy.Changed {
		diff.ChangedDomains = append(diff.ChangedDomains, "policy")
	}

	return diff, nil
}

func releaseRunWorkflowID(version WorkflowReleaseVersion) string {
	return strings.TrimSpace(version.WorkflowID) + "@" + strings.TrimSpace(version.ID)
}

func (r *Runner) compileWorkflowReleaseVersion(version WorkflowReleaseVersion, tenantID string) (WorkflowGraph, error) {
	reg := r.HandlerRegistryForTenant(tenantID)
	if reg == nil {
		return WorkflowGraph{}, fmt.Errorf("handler registry is required")
	}

	g, spec, err := ParseWorkflowSpecJSON(version.Spec)
	if err != nil {
		return WorkflowGraph{}, err
	}

	exec, err := CompileSpecToExecutable(spec, g, reg)
	if err != nil {
		return WorkflowGraph{}, err
	}

	exec.ID = releaseRunWorkflowID(version)
	return exec, nil
}

func cloneReleaseExecutionContext(ctx context.Context) context.Context {
	out := context.Background()
	if tenantID := effectiveTenantFromContext(ctx); tenantID != "" {
		out = WithTenantID(out, tenantID)
	}

	if principal, ok := PrincipalFrom(ctx); ok {
		out = WithPrincipal(out, principal)
	}

	if traceID := TraceIDFrom(ctx); traceID != "" {
		out = WithTraceID(out, traceID)
	}

	return out
}

func (r *Runner) launchShadowWorkflowRelease(ctx context.Context, baseRunID string, version WorkflowReleaseVersion, initialInput json.RawMessage) {
	shadowRunID := strings.TrimSpace(baseRunID) + "~shadow~" + strings.TrimSpace(version.ID)
	shadowCtx := cloneReleaseExecutionContext(ctx)
	shadowCtx = WithSideEffectMode(shadowCtx, SideEffectModeDryRun)

	go func() {
		tenantID := effectiveTenantFromContext(shadowCtx)
		exec, err := r.compileWorkflowReleaseVersion(version, tenantID)

		if err != nil {
			r.logger.Error("shadow workflow compile failed", "run_id", shadowRunID, "workflow_id", version.WorkflowID, "version_id", version.ID, "err", err)
			return
		}

		if err := r.runDAG(shadowCtx, shadowRunID, exec, cloneRaw(initialInput), cloneRaw(version.Spec)); err != nil {
			r.logger.Error("shadow workflow run failed", "run_id", shadowRunID, "workflow_id", version.WorkflowID, "version_id", version.ID, "err", err)
			return
		}
	}()
}

func (r *Runner) RunWorkflowRelease(ctx context.Context, runID, workflowID string, env WorkflowEnvironment, initialInput json.RawMessage) (WorkflowReleaseResolution, error) {
	resolution, err := r.ResolveWorkflowRelease(workflowID, env, runID)
	if err != nil {
		return WorkflowReleaseResolution{}, err
	}

	primary, ok, err := r.GetWorkflowReleaseVersion(resolution.PrimaryVersionID)
	if err != nil {
		return WorkflowReleaseResolution{}, err
	}

	if !ok {
		return WorkflowReleaseResolution{}, fmt.Errorf("workflow release version %q not found", resolution.PrimaryVersionID)
	}

	for _, shadowVersionID := range resolution.ShadowVersionIDs {
		shadowVersion, ok, err := r.GetWorkflowReleaseVersion(shadowVersionID)
		if err != nil || !ok {
			continue
		}
		r.launchShadowWorkflowRelease(ctx, runID, shadowVersion, initialInput)
	}

	tenantID := effectiveTenantFromContext(ctx)
	exec, err := r.compileWorkflowReleaseVersion(primary, tenantID)

	if err != nil {
		return WorkflowReleaseResolution{}, err
	}

	if err := r.runDAG(ctx, runID, exec, cloneRaw(initialInput), cloneRaw(primary.Spec)); err != nil {
		return WorkflowReleaseResolution{}, err
	}

	return resolution, nil
}

func (r *Runner) FinalizeWorkflowCanaryWithEvalGate(ctx context.Context, evalRunID, workflowID string, env WorkflowEnvironment) (WorkflowReleaseChannel, string, error) {
	report, ok, err := r.GetEvalReport(evalRunID)
	if err != nil {
		return WorkflowReleaseChannel{}, "", err
	}

	if !ok {
		return WorkflowReleaseChannel{}, "", fmt.Errorf("eval report %q not found", evalRunID)
	}

	if report.Status != "completed" {
		return WorkflowReleaseChannel{}, "", fmt.Errorf("eval report %q status=%s (need completed)", evalRunID, report.Status)
	}

	channel, ok, err := r.GetWorkflowReleaseChannel(workflowID, env)
	if err != nil {
		return WorkflowReleaseChannel{}, "", err
	}

	if !ok {
		return WorkflowReleaseChannel{}, "", fmt.Errorf("workflow release channel %q/%q not found", workflowID, env)
	}

	if strings.TrimSpace(channel.CanaryVersionID) == "" {
		return WorkflowReleaseChannel{}, "", fmt.Errorf("workflow release channel %q/%q has no canary version", workflowID, env)
	}

	channel.LastEvalRunID = strings.TrimSpace(evalRunID)
	action := "promoted"

	if report.Passed {
		channel.PreviousVersionID = channel.ActiveVersionID
		channel.ActiveVersionID = channel.CanaryVersionID
		channel.CanaryVersionID = ""
		channel.CanaryPercent = 0
	} else {
		action = "held"
		if channel.AutoRollbackOnEvalRegression {
			action = "rolled_back"
			channel.CanaryVersionID = ""
			channel.CanaryPercent = 0
		} else {
			return WorkflowReleaseChannel{}, "", fmt.Errorf("eval report %q did not pass canary promotion gate", evalRunID)
		}
	}

	if err := r.SaveWorkflowReleaseChannel(channel); err != nil {
		return WorkflowReleaseChannel{}, "", err
	}

	r.appendAuditRecord(ctx, AuditRecord{
		TenantID:     effectiveTenantFromContext(ctx),
		Action:       "workflow_release.canary.finalize",
		ResourceType: "workflow_release_channel",
		ResourceID:   workflowID + "/" + string(env),
		WorkflowID:   workflowID,
		Outcome:      AuditOutcomeAllowed,
		Reason:       "workflow release canary " + action,
	})

	saved, _, err := r.GetWorkflowReleaseChannel(workflowID, env)
	return saved, action, err
}
