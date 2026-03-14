package engine

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

var (
	ErrTenantAccessDenied  = fmt.Errorf("tenant access denied")
	ErrTenantQuotaExceeded = fmt.Errorf("tenant quota exceeded")
)

const auditIndexKey = "audit/index"

type AuditOutcome string

const (
	AuditOutcomeAllowed AuditOutcome = "allowed"
	AuditOutcomeDenied  AuditOutcome = "denied"
)

type AuditRecord struct {
	ID           string       `json:"id"`
	At           time.Time    `json:"at"`
	TenantID     string       `json:"tenant_id,omitempty"`
	PrincipalID  string       `json:"principal_id,omitempty"`
	Action       string       `json:"action"`
	ResourceType string       `json:"resource_type,omitempty"`
	ResourceID   string       `json:"resource_id,omitempty"`
	RunID        string       `json:"run_id,omitempty"`
	WorkflowID   string       `json:"workflow_id,omitempty"`
	Outcome      AuditOutcome `json:"outcome"`
	Reason       string       `json:"reason,omitempty"`
}

func auditRecordKey(id string) string {
	return "audit/records/" + strings.TrimSpace(id)
}

func effectiveTenantFromContext(ctx context.Context) string {
	tenantID := strings.TrimSpace(TenantIDFrom(ctx))
	if tenantID != "" {
		return tenantID
	}

	principal, ok := PrincipalFrom(ctx)
	if !ok {
		return ""
	}

	return strings.TrimSpace(principal.TenantID)
}

func (r *Runner) appendAuditRecord(ctx context.Context, record AuditRecord) {
	record.ID = strings.TrimSpace(record.ID)
	if record.ID == "" {
		record.ID = "audit-" + time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + NewTraceID()
	}

	if record.At.IsZero() {
		record.At = time.Now().UTC()
	} else {
		record.At = record.At.UTC()
	}

	record.TenantID = strings.TrimSpace(record.TenantID)
	record.PrincipalID = strings.TrimSpace(record.PrincipalID)
	record.Action = strings.TrimSpace(record.Action)
	record.ResourceType = strings.TrimSpace(record.ResourceType)
	record.ResourceID = strings.TrimSpace(record.ResourceID)
	record.RunID = strings.TrimSpace(record.RunID)
	record.WorkflowID = strings.TrimSpace(record.WorkflowID)
	record.Reason = strings.TrimSpace(record.Reason)

	if record.Action == "" {
		return
	}

	if record.TenantID == "" {
		record.TenantID = effectiveTenantFromContext(ctx)
	}

	if record.PrincipalID == "" {
		if principal, ok := PrincipalFrom(ctx); ok {
			record.PrincipalID = principal.ID
		}
	}

	if err := putJSONKV(r.store, auditRecordKey(record.ID), record); err != nil {
		return
	}

	ids, err := indexIDs(r.store, auditIndexKey)
	if err != nil {
		return
	}

	ids = append(ids, record.ID)
	_ = putJSONKV(r.store, auditIndexKey, ids)
}

func (r *Runner) ListAuditRecords(tenantID, runID string, limit int) ([]AuditRecord, error) {
	if limit <= 0 {
		limit = 100
	}

	ids, err := indexIDs(r.store, auditIndexKey)
	if err != nil {
		return nil, err
	}

	tenantID = strings.TrimSpace(tenantID)
	runID = strings.TrimSpace(runID)

	out := make([]AuditRecord, 0, min(limit, len(ids)))
	for i := len(ids) - 1; i >= 0; i-- {
		var rec AuditRecord
		ok, err := getJSONKV(r.store, auditRecordKey(ids[i]), &rec)
		if err != nil || !ok {
			continue
		}

		if tenantID != "" && rec.TenantID != tenantID {
			continue
		}

		if runID != "" && rec.RunID != runID {
			continue
		}

		out = append(out, rec)
		if len(out) >= limit {
			break
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].At.After(out[j].At) })
	return out, nil
}

func (r *Runner) SetTenantHandlerRegistry(tenantID string, reg *HandlerRegistry) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tenantRegistries == nil {
		r.tenantRegistries = make(map[string]*HandlerRegistry)
	}

	if reg == nil {
		delete(r.tenantRegistries, tenantID)
		return
	}

	r.tenantRegistries[tenantID] = reg
}

func (r *Runner) HandlerRegistryForTenant(tenantID string) *HandlerRegistry {
	tenantID = strings.TrimSpace(tenantID)

	r.mu.RLock()
	defer r.mu.RUnlock()

	if tenantID != "" && r.tenantRegistries != nil {
		if reg := r.tenantRegistries[tenantID]; reg != nil {
			return reg
		}
	}

	return r.registry
}

func (r *Runner) SetTenantActiveRunCap(tenantID string, cap int) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" || cap < 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tenantRunCaps == nil {
		r.tenantRunCaps = make(map[string]int)
	}

	if cap == 0 {
		delete(r.tenantRunCaps, tenantID)
		return
	}

	r.tenantRunCaps[tenantID] = cap
}

func (r *Runner) tenantActiveRunCap(tenantID string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if tenantID == "" || r.tenantRunCaps == nil {
		return 0
	}

	return r.tenantRunCaps[tenantID]
}

func (r *Runner) activeRunsForTenant(tenantID, excludeRunID string) int {
	tenantID = strings.TrimSpace(tenantID)
	excludeRunID = strings.TrimSpace(excludeRunID)
	if tenantID == "" {
		return 0
	}

	count := 0
	for _, runID := range r.store.ListRuns() {
		if runID == excludeRunID {
			continue
		}

		run, ok := r.store.GetRun(runID)
		if !ok || strings.TrimSpace(run.TenantID) != tenantID {
			continue
		}

		switch run.Status {
		case RunStatusQueued, RunStatusRunning, RunStatusWaiting:
			count++
		}
	}
	return count
}

func (r *Runner) enforceTenantRunQuota(ctx context.Context, tenantID, runID, workflowID string) (err error) {
	ctx, span := r.startSpan(ctx, "driftq.governance.check", workflowSpanAttributes(runID, workflowID, tenantID)...)
	defer func() {
		if r.obs != nil {
			r.obs.observeGovernance("tenant.quota.active_runs", err == nil)
		}
		r.finishSpan(span, err,
			attribute.String("driftq.governance.action", "tenant.quota.active_runs"),
		)
	}()

	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return nil
	}

	cap := r.tenantActiveRunCap(tenantID)
	if cap <= 0 {
		return nil
	}

	active := r.activeRunsForTenant(tenantID, runID)
	if active < cap {
		return nil
	}

	r.appendAuditRecord(ctx, AuditRecord{
		TenantID:     tenantID,
		Action:       "tenant.quota.active_runs",
		ResourceType: "run",
		ResourceID:   strings.TrimSpace(runID),
		RunID:        strings.TrimSpace(runID),
		WorkflowID:   strings.TrimSpace(workflowID),
		Outcome:      AuditOutcomeDenied,
		Reason:       fmt.Sprintf("tenant active run cap reached (%d)", cap),
	})

	return fmt.Errorf("%w: active run cap reached for tenant %q", ErrTenantQuotaExceeded, tenantID)
}

func (r *Runner) ensureRunTenantAccess(ctx context.Context, run Run, action string) (err error) {
	ctx, span := r.startSpan(ctx, "driftq.governance.check", workflowSpanAttributes(run.RunID, run.WorkflowID, run.TenantID)...)
	defer func() {
		if r.obs != nil {
			r.obs.observeGovernance(action, err == nil)
		}
		r.finishSpan(span, err,
			attribute.String("driftq.governance.action", strings.TrimSpace(action)),
			attribute.String("driftq.resource_type", "run"),
		)
	}()

	resourceTenantID := strings.TrimSpace(run.TenantID)
	if resourceTenantID == "" {
		return nil
	}

	callerTenantID := effectiveTenantFromContext(ctx)
	principal, hasPrincipal := PrincipalFrom(ctx)

	if callerTenantID == "" {
		r.appendAuditRecord(ctx, AuditRecord{
			TenantID:     resourceTenantID,
			Action:       action,
			ResourceType: "run",
			ResourceID:   run.RunID,
			RunID:        run.RunID,
			WorkflowID:   run.WorkflowID,
			Outcome:      AuditOutcomeDenied,
			Reason:       "missing caller tenant context",
		})
		return fmt.Errorf("%w: missing tenant context for run %q", ErrTenantAccessDenied, run.RunID)
	}

	if callerTenantID != resourceTenantID {
		r.appendAuditRecord(ctx, AuditRecord{
			TenantID:     resourceTenantID,
			Action:       action,
			ResourceType: "run",
			ResourceID:   run.RunID,
			RunID:        run.RunID,
			WorkflowID:   run.WorkflowID,
			Outcome:      AuditOutcomeDenied,
			Reason:       fmt.Sprintf("caller tenant %q cannot access tenant %q", callerTenantID, resourceTenantID),
		})
		return fmt.Errorf("%w: caller tenant %q cannot access tenant %q", ErrTenantAccessDenied, callerTenantID, resourceTenantID)
	}

	if hasPrincipal && len(principal.TenantScopes) > 0 {
		allowed := false
		for _, scope := range principal.TenantScopes {
			if matchPattern(resourceTenantID, scope) {
				allowed = true
				break
			}
		}

		if !allowed {
			r.appendAuditRecord(ctx, AuditRecord{
				TenantID:     resourceTenantID,
				PrincipalID:  principal.ID,
				Action:       action,
				ResourceType: "run",
				ResourceID:   run.RunID,
				RunID:        run.RunID,
				WorkflowID:   run.WorkflowID,
				Outcome:      AuditOutcomeDenied,
				Reason:       fmt.Sprintf("principal tenant scope does not include tenant %q", resourceTenantID),
			})

			return fmt.Errorf("%w: principal tenant scope does not include tenant %q", ErrTenantAccessDenied, resourceTenantID)
		}
	}

	r.appendAuditRecord(ctx, AuditRecord{
		TenantID:     resourceTenantID,
		Action:       action,
		ResourceType: "run",
		ResourceID:   run.RunID,
		RunID:        run.RunID,
		WorkflowID:   run.WorkflowID,
		Outcome:      AuditOutcomeAllowed,
		Reason:       "tenant access allowed",
	})
	return nil
}

func (r *Runner) ensureArtifactTenantAccess(ctx context.Context, meta ArtifactMeta, action string) error {
	tenantID := strings.TrimSpace(meta.TenantID)
	if tenantID == "" && strings.TrimSpace(meta.RunID) != "" {
		if run, ok := r.store.GetRun(meta.RunID); ok {
			tenantID = strings.TrimSpace(run.TenantID)
		}
	}

	if tenantID == "" {
		return nil
	}

	run := Run{RunID: meta.RunID, WorkflowID: meta.WorkflowID, TenantID: tenantID}
	return r.ensureRunTenantAccess(ctx, run, action)
}

func debugTenantIDFromRequest(r *http.Request) string {
	tenantID := strings.TrimSpace(r.Header.Get("X-Tenant-Id"))
	if tenantID != "" {
		return tenantID
	}

	return strings.TrimSpace(r.URL.Query().Get("tenant_id"))
}

func debugContextFromRequest(r *http.Request, traceID string) context.Context {
	traceID = strings.TrimSpace(traceID)
	ctx := r.Context()
	if traceID != "" {
		ctx = WithTraceID(ctx, traceID)
	}

	if tenantID := debugTenantIDFromRequest(r); tenantID != "" {
		ctx = WithTenantID(ctx, tenantID)
	}
	return ctx
}
