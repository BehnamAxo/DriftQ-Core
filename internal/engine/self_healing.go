package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

const selfHealingArtifactIndexKey = "self_heal/artifacts/index"

type SelfHealingFailureNode struct {
	NodeID    string     `json:"node_id"`
	Attempt   int        `json:"attempt"`
	Error     string     `json:"error,omitempty"`
	StartedAt *time.Time `json:"started_at,omitempty"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
}

type SelfHealingReplaySuggestion struct {
	FromStep string     `json:"from_step,omitempty"`
	Mode     ReplayMode `json:"mode"`
	Reason   string     `json:"reason,omitempty"`
}

type SelfHealingRerunPlan struct {
	FromStep       string         `json:"from_step,omitempty"`
	Mode           ReplayMode     `json:"mode"`
	SideEffectMode SideEffectMode `json:"side_effect_mode,omitempty"`
	Notes          []string       `json:"notes,omitempty"`
}

type SelfHealingArtifact struct {
	ID                 string                        `json:"id"`
	CreatedAt          time.Time                     `json:"created_at"`
	UpdatedAt          time.Time                     `json:"updated_at"`
	TenantID           string                        `json:"tenant_id,omitempty"`
	RunID              string                        `json:"run_id"`
	WorkflowID         string                        `json:"workflow_id"`
	RunStatus          RunStatus                     `json:"run_status"`
	TerminalReason     string                        `json:"terminal_reason,omitempty"`
	FailureNodeID      string                        `json:"failure_node_id,omitempty"`
	FailureAttempt     int                           `json:"failure_attempt,omitempty"`
	FailureError       string                        `json:"failure_error,omitempty"`
	FailureNodes       []SelfHealingFailureNode      `json:"failure_nodes,omitempty"`
	ReplaySuggestions  []SelfHealingReplaySuggestion `json:"replay_suggestions,omitempty"`
	SaferRerun         SelfHealingRerunPlan          `json:"safer_rerun"`
	EvalDatasetID      string                        `json:"eval_dataset_id,omitempty"`
	EvalCaseID         string                        `json:"eval_case_id,omitempty"`
	EvalCaseName       string                        `json:"eval_case_name,omitempty"`
	SourceSpecCaptured bool                          `json:"source_spec_captured,omitempty"`
}

func selfHealingArtifactKey(id string) string {
	return "self_heal/artifacts/" + url.PathEscape(strings.TrimSpace(id))
}

func selfHealingArtifactID(runID string) string {
	return "self-heal-" + sanitizeEvalRunID(runID)
}

func cloneSelfHealingArtifact(artifact SelfHealingArtifact) SelfHealingArtifact {
	out := artifact
	out.ID = strings.TrimSpace(artifact.ID)
	out.TenantID = strings.TrimSpace(artifact.TenantID)
	out.RunID = strings.TrimSpace(artifact.RunID)
	out.WorkflowID = strings.TrimSpace(artifact.WorkflowID)
	out.TerminalReason = strings.TrimSpace(artifact.TerminalReason)
	out.FailureNodeID = strings.TrimSpace(artifact.FailureNodeID)
	out.FailureError = strings.TrimSpace(artifact.FailureError)
	out.EvalDatasetID = strings.TrimSpace(artifact.EvalDatasetID)
	out.EvalCaseID = strings.TrimSpace(artifact.EvalCaseID)
	out.EvalCaseName = strings.TrimSpace(artifact.EvalCaseName)
	out.FailureNodes = append([]SelfHealingFailureNode(nil), artifact.FailureNodes...)
	out.ReplaySuggestions = append([]SelfHealingReplaySuggestion(nil), artifact.ReplaySuggestions...)
	out.SaferRerun.Notes = append([]string(nil), artifact.SaferRerun.Notes...)
	return out
}

func (r *Runner) saveSelfHealingArtifact(artifact SelfHealingArtifact) error {
	artifact.ID = strings.TrimSpace(artifact.ID)

	if artifact.ID == "" {
		artifact.ID = selfHealingArtifactID(artifact.RunID)
	}

	artifact.TenantID = strings.TrimSpace(artifact.TenantID)
	artifact.RunID = strings.TrimSpace(artifact.RunID)
	artifact.WorkflowID = strings.TrimSpace(artifact.WorkflowID)
	artifact.TerminalReason = strings.TrimSpace(artifact.TerminalReason)
	artifact.FailureNodeID = strings.TrimSpace(artifact.FailureNodeID)
	artifact.FailureError = strings.TrimSpace(artifact.FailureError)
	artifact.EvalDatasetID = strings.TrimSpace(artifact.EvalDatasetID)
	artifact.EvalCaseID = strings.TrimSpace(artifact.EvalCaseID)
	artifact.EvalCaseName = strings.TrimSpace(artifact.EvalCaseName)

	if artifact.CreatedAt.IsZero() {
		artifact.CreatedAt = time.Now().UTC()
	}

	artifact.UpdatedAt = time.Now().UTC()

	if err := putJSONKV(r.store, selfHealingArtifactKey(artifact.ID), artifact); err != nil {
		return err
	}
	return upsertIndexID(r.store, selfHealingArtifactIndexKey, artifact.ID)
}

func (r *Runner) GetSelfHealingArtifact(ctx context.Context, artifactID string) (SelfHealingArtifact, bool, error) {
	artifactID = strings.TrimSpace(artifactID)
	if artifactID == "" {
		return SelfHealingArtifact{}, false, nil
	}

	var artifact SelfHealingArtifact
	ok, err := getJSONKV(r.store, selfHealingArtifactKey(artifactID), &artifact)
	if err != nil || !ok {
		return SelfHealingArtifact{}, ok, err
	}

	if strings.TrimSpace(artifact.TenantID) != "" {
		if err := r.ensureRunTenantAccess(ctx, Run{
			RunID:      artifact.RunID,
			WorkflowID: artifact.WorkflowID,
			TenantID:   artifact.TenantID,
		}, "self_heal.artifact"); err != nil {
			return SelfHealingArtifact{}, false, err
		}
	}

	return cloneSelfHealingArtifact(artifact), true, nil
}

func (r *Runner) GetSelfHealingArtifactByRun(ctx context.Context, runID string) (SelfHealingArtifact, bool, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return SelfHealingArtifact{}, false, nil
	}
	return r.GetSelfHealingArtifact(ctx, selfHealingArtifactID(runID))
}

func (r *Runner) ListSelfHealingArtifacts(ctx context.Context, runID string, limit int) ([]SelfHealingArtifact, error) {
	if limit <= 0 {
		limit = 100
	}

	runID = strings.TrimSpace(runID)
	tenantID := effectiveTenantFromContext(ctx)
	ids, err := indexIDs(r.store, selfHealingArtifactIndexKey)
	if err != nil {
		return nil, err
	}

	out := make([]SelfHealingArtifact, 0, min(limit, len(ids)))
	for i := len(ids) - 1; i >= 0; i-- {
		artifact, ok, err := r.GetSelfHealingArtifact(context.Background(), ids[i])
		if err != nil || !ok {
			continue
		}

		if tenantID != "" && strings.TrimSpace(artifact.TenantID) != tenantID {
			continue
		}

		if runID != "" && strings.TrimSpace(artifact.RunID) != runID {
			continue
		}

		out = append(out, artifact)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func latestNodeExecutionByNode(nodes []NodeExecution) map[string]NodeExecution {
	out := make(map[string]NodeExecution, len(nodes))
	for _, node := range nodes {
		existing, ok := out[node.NodeID]
		if !ok || node.Attempt > existing.Attempt {
			out[node.NodeID] = node
			continue
		}

		if node.Attempt == existing.Attempt {
			leftEnd := time.Time{}
			rightEnd := time.Time{}

			if node.EndedAt != nil {
				leftEnd = *node.EndedAt
			}

			if existing.EndedAt != nil {
				rightEnd = *existing.EndedAt
			}

			if leftEnd.After(rightEnd) {
				out[node.NodeID] = node
			}
		}
	}
	return out
}

func nodeOrderFromSpec(spec json.RawMessage) []string {
	if len(spec) == 0 {
		return nil
	}

	graph, _, err := ParseWorkflowSpecJSON(spec)
	if err != nil {
		return nil
	}

	order := make([]string, 0, len(graph.Nodes))
	for _, node := range graph.Nodes {
		order = append(order, node.NodeID)
	}

	return order
}

func failureNodesFromRun(nodes []NodeExecution, orderedNodeIDs []string) []SelfHealingFailureNode {
	latest := latestNodeExecutionByNode(nodes)
	out := make([]SelfHealingFailureNode, 0)

	appendFailure := func(node NodeExecution) {
		out = append(out, SelfHealingFailureNode{
			NodeID:    node.NodeID,
			Attempt:   node.Attempt,
			Error:     strings.TrimSpace(node.Error),
			StartedAt: node.StartedAt,
			EndedAt:   node.EndedAt,
		})
	}

	if len(orderedNodeIDs) > 0 {
		seen := make(map[string]struct{}, len(orderedNodeIDs))
		for _, nodeID := range orderedNodeIDs {
			if node, ok := latest[nodeID]; ok && node.Status == NodeStatusFailed {
				appendFailure(node)
				seen[nodeID] = struct{}{}
			}
		}

		keys := make([]string, 0, len(latest))
		for nodeID, node := range latest {
			if node.Status == NodeStatusFailed {
				if _, ok := seen[nodeID]; ok {
					continue
				}
				keys = append(keys, nodeID)
			}
		}

		sort.Strings(keys)
		for _, nodeID := range keys {
			appendFailure(latest[nodeID])
		}
		return out
	}

	keys := make([]string, 0, len(latest))
	for nodeID, node := range latest {
		if node.Status == NodeStatusFailed {
			keys = append(keys, nodeID)
		}
	}

	sort.Strings(keys)
	for _, nodeID := range keys {
		appendFailure(latest[nodeID])
	}

	return out
}

func previousSucceededCheckpoint(failureNodeID string, orderedNodeIDs []string, latest map[string]NodeExecution) string {
	if failureNodeID == "" {
		return ""
	}

	index := -1
	for i, nodeID := range orderedNodeIDs {
		if nodeID == failureNodeID {
			index = i
			break
		}
	}

	if index <= 0 {
		return ""
	}

	for i := index - 1; i >= 0; i-- {
		if node, ok := latest[orderedNodeIDs[i]]; ok && node.Status == NodeStatusSucceeded {
			return node.NodeID
		}
	}

	return ""
}

func buildReplaySuggestions(failureNodeID, checkpoint string) []SelfHealingReplaySuggestion {
	suggestions := make([]SelfHealingReplaySuggestion, 0, 3)
	if failureNodeID != "" {
		suggestions = append(suggestions,
			SelfHealingReplaySuggestion{
				FromStep: failureNodeID,
				Mode:     ReplayTimeTravel,
				Reason:   "reuse successful upstream outputs and retry from the failed step",
			},
			SelfHealingReplaySuggestion{
				FromStep: failureNodeID,
				Mode:     ReplayLive,
				Reason:   "force a fresh execution starting at the failed step",
			},
		)
	}

	if checkpoint != "" && checkpoint != failureNodeID {
		suggestions = append(suggestions, SelfHealingReplaySuggestion{
			FromStep: checkpoint,
			Mode:     ReplayTimeTravel,
			Reason:   "resume from the last succeeded checkpoint and replay downstream work",
		})
	}

	if len(suggestions) == 0 {
		suggestions = append(suggestions, SelfHealingReplaySuggestion{
			Mode:   ReplayLive,
			Reason: "rerun the workflow from the beginning",
		})
	}

	return suggestions
}

func buildSaferRerunPlan(failureNodeID, checkpoint string) SelfHealingRerunPlan {
	plan := SelfHealingRerunPlan{
		Mode:           ReplayLive,
		SideEffectMode: SideEffectModeDryRun,
		Notes: []string{
			"use dry-run side-effect mode first so recovery reruns stay safer while validating a fix",
		},
	}

	if failureNodeID != "" {
		plan.FromStep = failureNodeID
		plan.Mode = ReplayTimeTravel
		plan.Notes = append(plan.Notes, "time-travel replay keeps previously successful upstream outputs")
		return plan
	}

	if checkpoint != "" {
		plan.FromStep = checkpoint
		plan.Mode = ReplayTimeTravel
		plan.Notes = append(plan.Notes, "resume from the last successful checkpoint instead of rerunning everything")
	}
	return plan
}

func selfHealingDatasetID(workflowID string) string {
	return "self-heal-failures-" + sanitizeEvalRunID(workflowID)
}

func selfHealingCaseID(runID string) string {
	return "failure-" + sanitizeEvalRunID(runID)
}

func (r *Runner) ensureSelfHealingEvalDataset(workflowID string) (EvalDataset, error) {
	datasetID := selfHealingDatasetID(workflowID)
	if dataset, ok, err := r.GetEvalDataset(datasetID); err != nil {
		return EvalDataset{}, err
	} else if ok {
		return dataset, nil
	}

	dataset := EvalDataset{
		ID:          datasetID,
		Name:        "Self-healing failures for " + strings.TrimSpace(workflowID),
		Description: "Automatically captured failed runs for recovery, replay, and evaluation.",
		Cases:       []EvalCase{},
	}

	if err := r.SaveEvalDataset(dataset); err != nil {
		return EvalDataset{}, err
	}

	saved, _, err := r.GetEvalDataset(datasetID)
	return saved, err
}

func enrichSelfHealingEvalCase(dataset EvalDataset, evalCase EvalCase, run Run, primaryFailure SelfHealingFailureNode) (EvalDataset, EvalCase) {
	labels := map[string]string{}
	for key, value := range evalCase.Labels {
		labels[key] = value
	}

	labels["self_heal"] = "true"
	labels["terminal_reason"] = strings.TrimSpace(run.TerminalReason)

	if primaryFailure.NodeID != "" {
		labels["failure_node_id"] = primaryFailure.NodeID
	}

	if primaryFailure.Error != "" {
		labels["failure_error"] = primaryFailure.Error
	}

	evalCase.Labels = labels

	for i := range dataset.Cases {
		if dataset.Cases[i].ID == evalCase.ID {
			dataset.Cases[i] = evalCase
			break
		}
	}
	return dataset, evalCase
}

func (r *Runner) CaptureSelfHealingArtifact(ctx context.Context, runID string) (SelfHealingArtifact, error) {
	runID = strings.TrimSpace(runID)
	run, ok := r.store.GetRun(runID)
	if !ok {
		return SelfHealingArtifact{}, ErrRunNotFound
	}

	if err := r.ensureRunTenantAccess(ctx, run, "self_heal.capture"); err != nil {
		return SelfHealingArtifact{}, err
	}

	if run.Status != RunStatusFailed {
		return SelfHealingArtifact{}, fmt.Errorf("self-heal: run %q status=%s (need failed)", runID, run.Status)
	}

	nodes := r.store.ListNodeExecutions(runID)
	order := nodeOrderFromSpec(run.Spec)
	failures := failureNodesFromRun(nodes, order)
	latest := latestNodeExecutionByNode(nodes)
	primaryFailure := SelfHealingFailureNode{}

	if len(failures) > 0 {
		primaryFailure = failures[0]
	}

	checkpoint := previousSucceededCheckpoint(primaryFailure.NodeID, order, latest)
	suggestions := buildReplaySuggestions(primaryFailure.NodeID, checkpoint)
	saferRerun := buildSaferRerunPlan(primaryFailure.NodeID, checkpoint)

	artifact := SelfHealingArtifact{
		ID:                 selfHealingArtifactID(runID),
		TenantID:           run.TenantID,
		RunID:              run.RunID,
		WorkflowID:         run.WorkflowID,
		RunStatus:          run.Status,
		TerminalReason:     run.TerminalReason,
		FailureNodeID:      primaryFailure.NodeID,
		FailureAttempt:     primaryFailure.Attempt,
		FailureError:       primaryFailure.Error,
		FailureNodes:       failures,
		ReplaySuggestions:  suggestions,
		SaferRerun:         saferRerun,
		SourceSpecCaptured: len(run.Spec) > 0,
	}

	dataset, err := r.ensureSelfHealingEvalDataset(run.WorkflowID)
	if err != nil {
		return SelfHealingArtifact{}, err
	}

	caseName := "failure capture for " + run.RunID
	dataset, evalCase, err := r.CreateEvalCaseFromRun(dataset.ID, selfHealingCaseID(run.RunID), caseName, run.RunID, "")

	if err != nil {
		return SelfHealingArtifact{}, err
	}

	dataset, evalCase = enrichSelfHealingEvalCase(dataset, evalCase, run, primaryFailure)
	if err := r.SaveEvalDataset(dataset); err != nil {
		return SelfHealingArtifact{}, err
	}

	artifact.EvalDatasetID = dataset.ID
	artifact.EvalCaseID = evalCase.ID
	artifact.EvalCaseName = evalCase.Name

	if err := r.saveSelfHealingArtifact(artifact); err != nil {
		return SelfHealingArtifact{}, err
	}

	payload, _ := json.Marshal(map[string]any{
		"artifact_id":      artifact.ID,
		"failure_node_id":  artifact.FailureNodeID,
		"eval_dataset_id":  artifact.EvalDatasetID,
		"eval_case_id":     artifact.EvalCaseID,
		"safer_rerun_mode": artifact.SaferRerun.Mode,
	})

	_, _ = r.store.AppendEvent(RunEvent{
		RunID:      run.RunID,
		Type:       EventSelfHealingArtifactCreated,
		WorkflowID: run.WorkflowID,
		Payload:    payload,
	})

	r.appendAuditRecord(ctx, AuditRecord{
		TenantID:     run.TenantID,
		Action:       "self_heal.capture",
		ResourceType: "self_heal_artifact",
		ResourceID:   artifact.ID,
		RunID:        run.RunID,
		WorkflowID:   run.WorkflowID,
		Outcome:      AuditOutcomeAllowed,
		Reason:       "captured recovery artifacts from failed run",
	})

	saved, _, err := r.GetSelfHealingArtifact(ctx, artifact.ID)
	return saved, err
}

func (r *Runner) maybeCaptureSelfHealingArtifact(ctx context.Context, runID string) {
	if _, err := r.CaptureSelfHealingArtifact(ctx, runID); err != nil {
		r.logger.Error("self-heal capture failed", "run_id", runID, "err", err)
	}
}

func (r *Runner) ReplaySelfHealingArtifact(ctx context.Context, runID string) (SelfHealingArtifact, error) {
	artifact, ok, err := r.GetSelfHealingArtifactByRun(ctx, runID)
	if err != nil {
		return SelfHealingArtifact{}, err
	}

	if !ok {
		artifact, err = r.CaptureSelfHealingArtifact(ctx, runID)
		if err != nil {
			return SelfHealingArtifact{}, err
		}
	}

	replayCtx := ctx
	if artifact.SaferRerun.SideEffectMode != "" {
		replayCtx = WithSideEffectMode(replayCtx, artifact.SaferRerun.SideEffectMode)
	}

	if err := r.ReplayFrom(replayCtx, artifact.RunID, artifact.SaferRerun.FromStep, artifact.SaferRerun.Mode); err != nil {
		return artifact, err
	}

	return artifact, nil
}
