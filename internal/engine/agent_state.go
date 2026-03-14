package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

var (
	ErrAgentStateNotFound          = errors.New("agent state not found")
	ErrAgentStateAccessDenied      = errors.New("agent state access denied")
	ErrAgentStateReplayWriteDenied = errors.New("agent state writes are blocked during replay")
	ErrAgentStateVersionConflict   = errors.New("agent state version conflict")
)

var agentStateIDRe = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

type AgentStateSnapshot struct {
	TenantID      string          `json:"tenant_id"`
	AgentID       string          `json:"agent_id"`
	Version       int             `json:"version"`
	ParentVersion int             `json:"parent_version,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	State         json.RawMessage `json:"state"`
	TraceID       string          `json:"trace_id,omitempty"`
	PrincipalID   string          `json:"principal_id,omitempty"`
	RunID         string          `json:"run_id,omitempty"`
	WorkflowID    string          `json:"workflow_id,omitempty"`
	NodeID        string          `json:"node_id,omitempty"`
	Attempt       int             `json:"attempt,omitempty"`
}

type AgentStateReadOptions struct {
	Version int `json:"version,omitempty"`
}

type AgentStateWriteRequest struct {
	AgentID         string          `json:"agent_id"`
	State           json.RawMessage `json:"state"`
	ExpectedVersion int             `json:"expected_version,omitempty"`
}

type AgentStateAccessor interface {
	ReadAgentState(ctx context.Context, agentID string, opts AgentStateReadOptions) (AgentStateSnapshot, error)
	WriteAgentState(ctx context.Context, req AgentStateWriteRequest) (AgentStateSnapshot, error)
	ListAgentStateLineage(ctx context.Context, agentID string, limit int) ([]AgentStateSnapshot, error)
}

type agentStateCtxKey struct{}

func WithAgentStateContext(ctx context.Context, accessor AgentStateAccessor) context.Context {
	if accessor == nil {
		return ctx
	}
	return context.WithValue(ctx, agentStateCtxKey{}, accessor)
}

func AgentStateFrom(ctx context.Context) AgentStateAccessor {
	v := ctx.Value(agentStateCtxKey{})
	accessor, _ := v.(AgentStateAccessor)
	return accessor
}

type ctxKeyAgentID struct{}

func WithAgentID(ctx context.Context, agentID string) context.Context {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyAgentID{}, agentID)
}

func AgentIDFrom(ctx context.Context) string {
	v := ctx.Value(ctxKeyAgentID{})
	agentID, _ := v.(string)
	return strings.TrimSpace(agentID)
}

type ExecutionRef struct {
	RunID      string
	WorkflowID string
	NodeID     string
	Attempt    int
}

type executionRefCtxKey struct{}

func WithExecutionRef(ctx context.Context, ref ExecutionRef) context.Context {
	if strings.TrimSpace(ref.RunID) == "" {
		return ctx
	}
	ref.RunID = strings.TrimSpace(ref.RunID)
	ref.WorkflowID = strings.TrimSpace(ref.WorkflowID)
	ref.NodeID = strings.TrimSpace(ref.NodeID)
	return context.WithValue(ctx, executionRefCtxKey{}, ref)
}

func ExecutionRefFrom(ctx context.Context) (ExecutionRef, bool) {
	v := ctx.Value(executionRefCtxKey{})
	ref, ok := v.(ExecutionRef)
	if !ok {
		return ExecutionRef{}, false
	}

	ref.RunID = strings.TrimSpace(ref.RunID)
	ref.WorkflowID = strings.TrimSpace(ref.WorkflowID)
	ref.NodeID = strings.TrimSpace(ref.NodeID)

	return ref, ref.RunID != ""
}

type ReplayScope struct {
	SourceRunID string     `json:"source_run_id"`
	Mode        ReplayMode `json:"mode"`
	StartedAt   time.Time  `json:"started_at"`
}

type replayScopeCtxKey struct{}

func WithReplayScope(ctx context.Context, sourceRunID string, mode ReplayMode) context.Context {
	sourceRunID = strings.TrimSpace(sourceRunID)
	if sourceRunID == "" {
		return ctx
	}

	return context.WithValue(ctx, replayScopeCtxKey{}, ReplayScope{
		SourceRunID: sourceRunID,
		Mode:        mode,
		StartedAt:   time.Now().UTC(),
	})
}

func ReplayScopeFrom(ctx context.Context) (ReplayScope, bool) {
	v := ctx.Value(replayScopeCtxKey{})
	scope, ok := v.(ReplayScope)
	if !ok {
		return ReplayScope{}, false
	}

	scope.SourceRunID = strings.TrimSpace(scope.SourceRunID)
	if scope.SourceRunID == "" {
		return ReplayScope{}, false
	}

	if scope.StartedAt.IsZero() {
		scope.StartedAt = time.Now().UTC()
	}

	return scope, true
}

func normalizeAgentStateID(agentID string) (string, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return "", fmt.Errorf("%w: agent id is required", ErrAgentStateAccessDenied)
	}

	if !agentStateIDRe.MatchString(agentID) {
		return "", fmt.Errorf("%w: agent id %q must match %s", ErrAgentStateAccessDenied, agentID, agentStateIDRe.String())
	}

	return agentID, nil
}

func agentStateKeyPart(v string) string {
	return url.PathEscape(strings.TrimSpace(v))
}

func agentStateLatestKey(tenantID, agentID string) string {
	return "agent_state/latest/" + agentStateKeyPart(tenantID) + "/" + agentStateKeyPart(agentID)
}

func agentStateSnapshotKey(tenantID, agentID string, version int) string {
	return fmt.Sprintf("agent_state/snapshots/%s/%s/%012d", agentStateKeyPart(tenantID), agentStateKeyPart(agentID), version)
}

func cloneAgentStateSnapshot(s AgentStateSnapshot) AgentStateSnapshot {
	return AgentStateSnapshot{
		TenantID:      strings.TrimSpace(s.TenantID),
		AgentID:       strings.TrimSpace(s.AgentID),
		Version:       s.Version,
		ParentVersion: s.ParentVersion,
		CreatedAt:     s.CreatedAt.UTC(),
		State:         cloneRaw(s.State),
		TraceID:       strings.TrimSpace(s.TraceID),
		PrincipalID:   strings.TrimSpace(s.PrincipalID),
		RunID:         strings.TrimSpace(s.RunID),
		WorkflowID:    strings.TrimSpace(s.WorkflowID),
		NodeID:        strings.TrimSpace(s.NodeID),
		Attempt:       s.Attempt,
	}
}

func (r *Runner) readAgentStateVersion(tenantID, agentID string, version int) (AgentStateSnapshot, bool, error) {
	var snapshot AgentStateSnapshot
	ok, err := getJSONKV(r.store, agentStateSnapshotKey(tenantID, agentID, version), &snapshot)

	if err != nil || !ok {
		return AgentStateSnapshot{}, ok, err
	}

	return cloneAgentStateSnapshot(snapshot), true, nil
}

func (r *Runner) latestAgentStateVersion(tenantID, agentID string) (int, bool, error) {
	var latest int
	ok, err := getJSONKV(r.store, agentStateLatestKey(tenantID, agentID), &latest)

	if err != nil || !ok {
		return 0, ok, err
	}

	if latest < 0 {
		latest = 0
	}

	return latest, latest > 0, nil
}

func (r *Runner) latestReplayReadableAgentState(ctx context.Context, tenantID, agentID string) (AgentStateSnapshot, bool, error) {
	scope, ok := ReplayScopeFrom(ctx)
	if !ok {
		return AgentStateSnapshot{}, false, nil
	}

	latest, ok, err := r.latestAgentStateVersion(tenantID, agentID)
	if err != nil || !ok {
		return AgentStateSnapshot{}, ok, err
	}

	boundary := scope.StartedAt.UTC()
	for version := latest; version >= 1; version-- {
		snapshot, ok, err := r.readAgentStateVersion(tenantID, agentID, version)
		if err != nil {
			return AgentStateSnapshot{}, false, err
		}

		if !ok {
			continue
		}

		if !snapshot.CreatedAt.After(boundary) {
			return snapshot, true, nil
		}
	}

	return AgentStateSnapshot{}, false, nil
}

func (r *Runner) ensureAgentStateAccess(ctx context.Context, tenantID, agentID, action string) error {
	tenantID = strings.TrimSpace(tenantID)
	agentID = strings.TrimSpace(agentID)
	action = strings.TrimSpace(action)

	if tenantID == "" {
		r.appendAuditRecord(ctx, AuditRecord{
			Action:       action,
			ResourceType: "agent_state",
			ResourceID:   agentID,
			Outcome:      AuditOutcomeDenied,
			Reason:       "missing tenant context",
		})
		return fmt.Errorf("%w: missing tenant context for agent %q", ErrTenantAccessDenied, agentID)
	}

	if callerTenantID := effectiveTenantFromContext(ctx); callerTenantID != tenantID {
		r.appendAuditRecord(ctx, AuditRecord{
			TenantID:     tenantID,
			Action:       action,
			ResourceType: "agent_state",
			ResourceID:   agentID,
			Outcome:      AuditOutcomeDenied,
			Reason:       fmt.Sprintf("caller tenant %q cannot access tenant %q", callerTenantID, tenantID),
		})

		return fmt.Errorf("%w: caller tenant %q cannot access tenant %q", ErrTenantAccessDenied, callerTenantID, tenantID)
	}

	if scopedAgentID := AgentIDFrom(ctx); scopedAgentID != "" && scopedAgentID != agentID {
		r.appendAuditRecord(ctx, AuditRecord{
			TenantID:     tenantID,
			Action:       action,
			ResourceType: "agent_state",
			ResourceID:   agentID,
			Outcome:      AuditOutcomeDenied,
			Reason:       fmt.Sprintf("agent scope %q cannot access agent %q", scopedAgentID, agentID),
		})

		return fmt.Errorf("%w: agent scope %q cannot access agent %q", ErrAgentStateAccessDenied, scopedAgentID, agentID)
	}

	if principal, hasPrincipal := PrincipalFrom(ctx); hasPrincipal && len(principal.TenantScopes) > 0 {
		allowed := false
		for _, scope := range principal.TenantScopes {
			if matchPattern(tenantID, scope) {
				allowed = true
				break
			}
		}

		if !allowed {
			r.appendAuditRecord(ctx, AuditRecord{
				TenantID:     tenantID,
				PrincipalID:  principal.ID,
				Action:       action,
				ResourceType: "agent_state",
				ResourceID:   agentID,
				Outcome:      AuditOutcomeDenied,
				Reason:       fmt.Sprintf("principal tenant scope does not include tenant %q", tenantID),
			})
			return fmt.Errorf("%w: principal tenant scope does not include tenant %q", ErrTenantAccessDenied, tenantID)
		}
	}

	r.appendAuditRecord(ctx, AuditRecord{
		TenantID:     tenantID,
		Action:       action,
		ResourceType: "agent_state",
		ResourceID:   agentID,
		Outcome:      AuditOutcomeAllowed,
		Reason:       "agent state access allowed",
	})
	return nil
}

func (r *Runner) ReadAgentState(ctx context.Context, agentID string, opts AgentStateReadOptions) (snapshot AgentStateSnapshot, err error) {
	agentID, err = normalizeAgentStateID(agentID)
	if err != nil {
		return AgentStateSnapshot{}, err
	}

	tenantID := effectiveTenantFromContext(ctx)
	ctx, span := r.startSpan(ctx, "driftq.agent_state.read", attributeStrings(
		"driftq.tenant_id", tenantID,
		"driftq.agent_id", agentID,
	)...)

	defer func() { r.finishSpan(span, err) }()

	if err := r.ensureAgentStateAccess(ctx, tenantID, agentID, "agent_state.read"); err != nil {
		return AgentStateSnapshot{}, err
	}

	if opts.Version > 0 {
		snapshot, ok, err := r.readAgentStateVersion(tenantID, agentID, opts.Version)
		if err != nil {
			return AgentStateSnapshot{}, err
		}

		if !ok {
			return AgentStateSnapshot{}, fmt.Errorf("%w: agent=%q version=%d", ErrAgentStateNotFound, agentID, opts.Version)
		}

		return snapshot, nil
	}

	if snapshot, ok, err := r.latestReplayReadableAgentState(ctx, tenantID, agentID); err != nil {
		return AgentStateSnapshot{}, err
	} else if ok {
		return snapshot, nil
	}

	latest, ok, err := r.latestAgentStateVersion(tenantID, agentID)
	if err != nil {
		return AgentStateSnapshot{}, err
	}

	if !ok {
		return AgentStateSnapshot{}, fmt.Errorf("%w: agent=%q", ErrAgentStateNotFound, agentID)
	}

	snapshot, ok, err = r.readAgentStateVersion(tenantID, agentID, latest)
	if err != nil {
		return AgentStateSnapshot{}, err
	}

	if !ok {
		return AgentStateSnapshot{}, fmt.Errorf("%w: agent=%q", ErrAgentStateNotFound, agentID)
	}

	return snapshot, nil
}

func (r *Runner) WriteAgentState(ctx context.Context, req AgentStateWriteRequest) (snapshot AgentStateSnapshot, err error) {
	req.AgentID, err = normalizeAgentStateID(req.AgentID)
	if err != nil {
		return AgentStateSnapshot{}, err
	}

	req.State = cloneRaw(req.State)
	if len(req.State) == 0 {
		req.State = json.RawMessage(`{}`)
	}

	if !json.Valid(req.State) {
		return AgentStateSnapshot{}, fmt.Errorf("agent state must be valid json")
	}

	tenantID := effectiveTenantFromContext(ctx)
	ctx, span := r.startSpan(ctx, "driftq.agent_state.write", attributeStrings(
		"driftq.tenant_id", tenantID,
		"driftq.agent_id", req.AgentID,
	)...)

	defer func() { r.finishSpan(span, err) }()

	if err := r.ensureAgentStateAccess(ctx, tenantID, req.AgentID, "agent_state.write"); err != nil {
		return AgentStateSnapshot{}, err
	}

	if _, ok := ReplayScopeFrom(ctx); ok {
		return AgentStateSnapshot{}, ErrAgentStateReplayWriteDenied
	}

	parentVersion := 0
	if latest, ok, err := r.latestAgentStateVersion(tenantID, req.AgentID); err != nil {
		return AgentStateSnapshot{}, err
	} else if ok {
		parentVersion = latest
	}

	if req.ExpectedVersion > 0 && req.ExpectedVersion != parentVersion {
		return AgentStateSnapshot{}, fmt.Errorf("%w: expected=%d latest=%d", ErrAgentStateVersionConflict, req.ExpectedVersion, parentVersion)
	}

	snapshot = AgentStateSnapshot{
		TenantID:      tenantID,
		AgentID:       req.AgentID,
		Version:       parentVersion + 1,
		ParentVersion: parentVersion,
		CreatedAt:     time.Now().UTC(),
		State:         cloneRaw(req.State),
		TraceID:       TraceIDFrom(ctx),
	}

	if principal, ok := PrincipalFrom(ctx); ok {
		snapshot.PrincipalID = strings.TrimSpace(principal.ID)
	}

	if execRef, ok := ExecutionRefFrom(ctx); ok {
		snapshot.RunID = execRef.RunID
		snapshot.WorkflowID = execRef.WorkflowID
		snapshot.NodeID = execRef.NodeID
		snapshot.Attempt = execRef.Attempt
	}

	if err := putJSONKV(r.store, agentStateSnapshotKey(tenantID, req.AgentID, snapshot.Version), snapshot); err != nil {
		return AgentStateSnapshot{}, err
	}

	if err := putJSONKV(r.store, agentStateLatestKey(tenantID, req.AgentID), snapshot.Version); err != nil {
		return AgentStateSnapshot{}, err
	}

	return cloneAgentStateSnapshot(snapshot), nil
}

func (r *Runner) ListAgentStateLineage(ctx context.Context, agentID string, limit int) ([]AgentStateSnapshot, error) {
	var err error
	agentID, err = normalizeAgentStateID(agentID)
	if err != nil {
		return nil, err
	}

	tenantID := effectiveTenantFromContext(ctx)
	ctx, span := r.startSpan(ctx, "driftq.agent_state.lineage", attributeStrings(
		"driftq.tenant_id", tenantID,
		"driftq.agent_id", agentID,
	)...)
	defer func() { r.finishSpan(span, err) }()

	if limit <= 0 {
		limit = 100
	}

	if limit > 500 {
		limit = 500
	}

	if err = r.ensureAgentStateAccess(ctx, tenantID, agentID, "agent_state.lineage"); err != nil {
		return nil, err
	}

	latest, ok, err := r.latestAgentStateVersion(tenantID, agentID)
	if err != nil {
		return nil, err
	}

	if !ok {
		return []AgentStateSnapshot{}, nil
	}

	out := make([]AgentStateSnapshot, 0, min(limit, latest))
	for version := latest; version >= 1 && len(out) < limit; version-- {
		snapshot, ok, err := r.readAgentStateVersion(tenantID, agentID, version)
		if err != nil {
			return nil, err
		}

		if !ok {
			continue
		}
		out = append(out, snapshot)
	}

	return out, nil
}

func attributeStrings(kvs ...string) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(kvs)/2)
	for i := 0; i+1 < len(kvs); i += 2 {
		if strings.TrimSpace(kvs[i+1]) == "" {
			continue
		}

		out = append(out, attribute.String(kvs[i], kvs[i+1]))
	}

	return out
}
