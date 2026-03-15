package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

const replayBranchIndexKey = "replay_branches/index"

type ReplayBranchRequest struct {
	SourceRunID          string          `json:"source_run_id"`
	BranchRunID          string          `json:"branch_run_id,omitempty"`
	BranchName           string          `json:"branch_name,omitempty"`
	FromStep             string          `json:"from_step,omitempty"`
	Mode                 ReplayMode      `json:"mode,omitempty"`
	SpecOverride         json.RawMessage `json:"spec_override,omitempty"`
	InitialInputOverride json.RawMessage `json:"initial_input_override,omitempty"`
	SideEffectMode       SideEffectMode  `json:"side_effect_mode,omitempty"`
}

type ReplayBranchRecord struct {
	ID                          string         `json:"id"`
	CreatedAt                   time.Time      `json:"created_at"`
	UpdatedAt                   time.Time      `json:"updated_at"`
	TenantID                    string         `json:"tenant_id,omitempty"`
	RootRunID                   string         `json:"root_run_id"`
	SourceRunID                 string         `json:"source_run_id"`
	BranchRunID                 string         `json:"branch_run_id"`
	BranchName                  string         `json:"branch_name,omitempty"`
	BaseWorkflowID              string         `json:"base_workflow_id,omitempty"`
	BranchWorkflowID            string         `json:"branch_workflow_id,omitempty"`
	FromStep                    string         `json:"from_step,omitempty"`
	Mode                        ReplayMode     `json:"mode"`
	SideEffectMode              SideEffectMode `json:"side_effect_mode,omitempty"`
	SpecOverrideApplied         bool           `json:"spec_override_applied,omitempty"`
	InitialInputOverrideApplied bool           `json:"initial_input_override_applied,omitempty"`
	RunStatus                   RunStatus      `json:"run_status,omitempty"`
	TerminalReason              string         `json:"terminal_reason,omitempty"`
}

type ReplayTimelineView struct {
	RootRunID string               `json:"root_run_id"`
	RootRun   Run                  `json:"root_run"`
	Branches  []ReplayBranchRecord `json:"branches,omitempty"`
}

func replayBranchKey(id string) string {
	return "replay_branches/items/" + url.PathEscape(strings.TrimSpace(id))
}

func replayBranchRunLookupKey(runID string) string {
	return "replay_branches/by_run/" + url.PathEscape(strings.TrimSpace(runID))
}

func replayBranchRootIndexKey(runID string) string {
	return "replay_branches/root/" + url.PathEscape(strings.TrimSpace(runID))
}

func replayBranchID(branchRunID string) string {
	return "replay-branch-" + sanitizeEvalRunID(branchRunID)
}

func branchRunID(sourceRunID, branchName string) string {
	parts := []string{sanitizeEvalRunID(sourceRunID), "branch"}
	if name := strings.TrimSpace(branchName); name != "" {
		parts = append(parts, sanitizeEvalRunID(name))
	}
	parts = append(parts, time.Now().UTC().Format("20060102T150405.000000000Z"))
	return strings.Join(parts, "~")
}

func branchWorkflowID(baseWorkflowID, branchRunID string) string {
	baseWorkflowID = strings.TrimSpace(baseWorkflowID)
	if baseWorkflowID == "" {
		baseWorkflowID = "workflow"
	}
	return baseWorkflowID + "~branch~" + sanitizeEvalRunID(branchRunID)
}

func normalizeReplayBranchRecord(record ReplayBranchRecord) ReplayBranchRecord {
	out := record
	out.ID = strings.TrimSpace(out.ID)
	out.TenantID = strings.TrimSpace(out.TenantID)
	out.RootRunID = strings.TrimSpace(out.RootRunID)
	out.SourceRunID = strings.TrimSpace(out.SourceRunID)
	out.BranchRunID = strings.TrimSpace(out.BranchRunID)
	out.BranchName = strings.TrimSpace(out.BranchName)
	out.BaseWorkflowID = strings.TrimSpace(out.BaseWorkflowID)
	out.BranchWorkflowID = strings.TrimSpace(out.BranchWorkflowID)
	out.FromStep = strings.TrimSpace(out.FromStep)
	out.TerminalReason = strings.TrimSpace(out.TerminalReason)
	if out.CreatedAt.IsZero() {
		out.CreatedAt = time.Now().UTC()
	}
	out.UpdatedAt = time.Now().UTC()
	return out
}

func validateReplayMode(mode ReplayMode) error {
	switch mode {
	case ReplayLive, ReplayTimeTravel:
		return nil
	default:
		return fmt.Errorf("unknown replay mode: %q", mode)
	}
}

func (r *Runner) saveReplayBranch(record ReplayBranchRecord) error {
	record = normalizeReplayBranchRecord(record)
	if record.ID == "" {
		record.ID = replayBranchID(record.BranchRunID)
	}

	if err := putJSONKV(r.store, replayBranchKey(record.ID), record); err != nil {
		return err
	}

	if err := r.store.PutKV(replayBranchRunLookupKey(record.BranchRunID), record.ID); err != nil {
		return err
	}

	if err := upsertIndexID(r.store, replayBranchIndexKey, record.ID); err != nil {
		return err
	}

	return upsertIndexID(r.store, replayBranchRootIndexKey(record.RootRunID), record.ID)
}

func (r *Runner) GetReplayBranch(ctx context.Context, branchID string) (ReplayBranchRecord, bool, error) {
	var record ReplayBranchRecord
	ok, err := getJSONKV(r.store, replayBranchKey(branchID), &record)

	if err != nil || !ok {
		return ReplayBranchRecord{}, ok, err
	}

	record = normalizeReplayBranchRecord(record)
	if record.TenantID != "" {
		if err := r.ensureRunTenantAccess(ctx, Run{
			RunID:      record.BranchRunID,
			WorkflowID: record.BaseWorkflowID,
			TenantID:   record.TenantID,
		}, "replay.branch"); err != nil {
			return ReplayBranchRecord{}, false, err
		}
	}

	return record, true, nil
}

func (r *Runner) GetReplayBranchByRun(ctx context.Context, runID string) (ReplayBranchRecord, bool, error) {
	branchID, ok := r.store.GetKV(replayBranchRunLookupKey(runID))

	if !ok || strings.TrimSpace(branchID) == "" {
		return ReplayBranchRecord{}, false, nil
	}

	return r.GetReplayBranch(ctx, branchID)
}

func (r *Runner) replayTimelineRootRunID(ctx context.Context, runID string) (string, error) {
	runID = strings.TrimSpace(runID)

	if runID == "" {
		return "", fmt.Errorf("run id is required")
	}

	record, ok, err := r.GetReplayBranchByRun(ctx, runID)

	if err != nil {
		return "", err
	}

	if ok {
		return record.RootRunID, nil
	}

	return runID, nil
}

func (r *Runner) ListReplayBranches(ctx context.Context, runID string) ([]ReplayBranchRecord, error) {
	rootRunID, err := r.replayTimelineRootRunID(ctx, runID)
	if err != nil {
		return nil, err
	}

	ids, err := indexIDs(r.store, replayBranchRootIndexKey(rootRunID))

	if err != nil {
		return nil, err
	}

	out := make([]ReplayBranchRecord, 0, len(ids))

	for _, id := range ids {
		record, ok, err := r.GetReplayBranch(ctx, id)
		if err != nil || !ok {
			continue
		}
		out = append(out, record)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].BranchRunID < out[j].BranchRunID
		}

		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})

	return out, nil
}

func (r *Runner) BuildReplayTimeline(ctx context.Context, runID string) (ReplayTimelineView, error) {
	rootRunID, err := r.replayTimelineRootRunID(ctx, runID)
	if err != nil {
		return ReplayTimelineView{}, err
	}

	rootRun, ok := r.store.GetRun(rootRunID)
	if !ok {
		return ReplayTimelineView{}, ErrRunNotFound
	}

	if err := r.ensureRunTenantAccess(ctx, rootRun, "replay.timeline"); err != nil {
		return ReplayTimelineView{}, err
	}

	branches, err := r.ListReplayBranches(ctx, rootRunID)
	if err != nil {
		return ReplayTimelineView{}, err
	}

	return ReplayTimelineView{
		RootRunID: rootRunID,
		RootRun:   rootRun,
		Branches:  branches,
	}, nil
}

func replayInitialInputForRun(source Run, fallback json.RawMessage) json.RawMessage {
	if len(source.InitialInput) > 0 {
		return cloneRaw(source.InitialInput)
	}

	return cloneRaw(fallback)
}

func replayBranchCache(entries map[string]replayCacheEntry, suffix map[string]struct{}, includeFromStep bool, fromStep string) map[string]replayCacheEntry {
	if len(entries) == 0 {
		return nil
	}

	if len(suffix) == 0 {
		out := make(map[string]replayCacheEntry, len(entries))
		for nodeID, entry := range entries {
			out[nodeID] = entry
		}
		return out
	}

	out := make(map[string]replayCacheEntry, len(entries))
	for nodeID, entry := range entries {
		if _, ok := suffix[nodeID]; ok {
			continue
		}
		out[nodeID] = entry
	}

	if includeFromStep && strings.TrimSpace(fromStep) != "" {
		if entry, ok := entries[fromStep]; ok {
			out[fromStep] = entry
		}
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func (r *Runner) buildReplayBranchCache(sourceRunID string, exec WorkflowGraph, fromStep string, mode ReplayMode) (map[string]replayCacheEntry, error) {
	sourceRunID = strings.TrimSpace(sourceRunID)
	fromStep = strings.TrimSpace(fromStep)
	if mode == ReplayLive && fromStep == "" {
		return nil, nil
	}

	cache := r.buildReplayCacheFromRun(sourceRunID)
	if fromStep == "" {
		if mode == ReplayTimeTravel {
			return cache, nil
		}
		return nil, nil
	}

	if !graphHasNode(exec, fromStep) {
		return nil, fmt.Errorf("replay: unknown from-step %q", fromStep)
	}

	suffix := downstreamNodes(exec, fromStep)
	includeFromStep := mode == ReplayTimeTravel && nodeHasSucceededAttempt(r.store.ListNodeExecutions(sourceRunID), fromStep)
	return replayBranchCache(cache, suffix, includeFromStep, fromStep), nil
}

func compileBranchWorkflow(reg *HandlerRegistry, spec json.RawMessage) (WorkflowGraph, error) {
	graph, workflowSpec, err := ParseWorkflowSpecJSON(spec)
	if err != nil {
		return WorkflowGraph{}, err
	}

	return CompileSpecToExecutable(workflowSpec, graph, reg)
}

func (r *Runner) CreateReplayBranch(ctx context.Context, req ReplayBranchRequest) (ReplayBranchRecord, error) {
	req.SourceRunID = strings.TrimSpace(req.SourceRunID)
	req.BranchRunID = strings.TrimSpace(req.BranchRunID)
	req.BranchName = strings.TrimSpace(req.BranchName)
	req.FromStep = strings.TrimSpace(req.FromStep)

	if req.SourceRunID == "" {
		return ReplayBranchRecord{}, fmt.Errorf("source_run_id is required")
	}

	mode := req.Mode
	if mode == "" {
		mode = ReplayTimeTravel
	}

	if err := validateReplayMode(mode); err != nil {
		return ReplayBranchRecord{}, err
	}

	sourceRun, ok := r.store.GetRun(req.SourceRunID)
	if !ok {
		return ReplayBranchRecord{}, ErrRunNotFound
	}

	if err := r.ensureRunTenantAccess(ctx, sourceRun, "replay.branch_create"); err != nil {
		return ReplayBranchRecord{}, err
	}

	if req.BranchRunID == "" {
		req.BranchRunID = branchRunID(sourceRun.RunID, req.BranchName)
	}

	spec := cloneRaw(sourceRun.Spec)
	if len(req.SpecOverride) > 0 {
		spec = cloneRaw(req.SpecOverride)
	}

	reg := r.HandlerRegistryForTenant(sourceRun.TenantID)
	if reg == nil {
		reg = r.HandlerRegistry()
	}

	if reg == nil {
		return ReplayBranchRecord{}, fmt.Errorf("replay branch: no handler registry configured")
	}

	exec, err := compileBranchWorkflow(reg, spec)
	if err != nil {
		return ReplayBranchRecord{}, fmt.Errorf("replay branch compile failed: %w", err)
	}

	initialInput := replayInitialInputForRun(sourceRun, r.initialInputFromRun(sourceRun.RunID, exec))
	if len(req.InitialInputOverride) > 0 {
		initialInput = cloneRaw(req.InitialInputOverride)
	}

	if len(req.SpecOverride) > 0 {
		exec.ID = branchWorkflowID(sourceRun.WorkflowID, req.BranchRunID)
	}

	rootRunID := sourceRun.RunID
	if sourceBranch, ok, err := r.GetReplayBranchByRun(ctx, sourceRun.RunID); err != nil {
		return ReplayBranchRecord{}, err
	} else if ok {
		rootRunID = sourceBranch.RootRunID
	}

	cache, err := r.buildReplayBranchCache(sourceRun.RunID, exec, req.FromStep, mode)
	if err != nil {
		return ReplayBranchRecord{}, err
	}

	runCtx := WithReplayScope(ctx, sourceRun.RunID, mode)
	if req.SideEffectMode != "" {
		runCtx = WithSideEffectMode(runCtx, req.SideEffectMode)
	}

	runErr := r.runDAGWithReplayCache(runCtx, req.BranchRunID, exec, initialInput, spec, cache)
	if runErr != nil && errors.Is(runErr, ErrRunAlreadyExists) {
		return ReplayBranchRecord{}, runErr
	}

	branchRun, ok := r.store.GetRun(req.BranchRunID)
	if !ok {
		if runErr != nil {
			return ReplayBranchRecord{}, runErr
		}
		return ReplayBranchRecord{}, ErrRunNotFound
	}

	record := ReplayBranchRecord{
		ID:                          replayBranchID(req.BranchRunID),
		TenantID:                    sourceRun.TenantID,
		RootRunID:                   rootRunID,
		SourceRunID:                 sourceRun.RunID,
		BranchRunID:                 branchRun.RunID,
		BranchName:                  req.BranchName,
		BaseWorkflowID:              sourceRun.WorkflowID,
		BranchWorkflowID:            branchRun.WorkflowID,
		FromStep:                    req.FromStep,
		Mode:                        mode,
		SideEffectMode:              req.SideEffectMode,
		SpecOverrideApplied:         len(req.SpecOverride) > 0,
		InitialInputOverrideApplied: len(req.InitialInputOverride) > 0,
		RunStatus:                   branchRun.Status,
		TerminalReason:              branchRun.TerminalReason,
	}

	if err := r.saveReplayBranch(record); err != nil {
		return ReplayBranchRecord{}, err
	}

	r.appendAuditRecord(ctx, AuditRecord{
		TenantID:     sourceRun.TenantID,
		Action:       "replay.branch_create",
		ResourceType: "replay_branch",
		ResourceID:   record.ID,
		RunID:        branchRun.RunID,
		WorkflowID:   branchRun.WorkflowID,
		Outcome:      AuditOutcomeAllowed,
		Reason:       "created alternate replay timeline branch",
	})

	saved, _, err := r.GetReplayBranch(ctx, record.ID)
	if err != nil {
		return ReplayBranchRecord{}, err
	}

	if runErr != nil {
		return saved, nil
	}

	return saved, nil
}
