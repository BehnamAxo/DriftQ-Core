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

var (
	ErrSideEffectReceiptNotFound = errors.New("side-effect receipt not found")
	ErrSideEffectCommitPending   = errors.New("side-effect commit pending approval")
	ErrSideEffectNotCompensable  = errors.New("side-effect is not compensable")
)

const sideEffectReceiptIndexKey = "side_effects/receipts/index"

type SideEffectMode string
type SideEffectStatus string

const (
	SideEffectModeCommit     SideEffectMode = "commit"
	SideEffectModeStage      SideEffectMode = "stage"
	SideEffectModeDryRun     SideEffectMode = "dry_run"
	SideEffectModeCompensate SideEffectMode = "compensate"
)

const (
	SideEffectStatusDryRun             SideEffectStatus = "dry_run"
	SideEffectStatusStaged             SideEffectStatus = "staged"
	SideEffectStatusCommitPending      SideEffectStatus = "commit_pending_approval"
	SideEffectStatusCommitted          SideEffectStatus = "committed"
	SideEffectStatusCompensated        SideEffectStatus = "compensated"
	SideEffectStatusCompensationFailed SideEffectStatus = "compensation_failed"
)

type SideEffectPolicy struct {
	Enabled              bool   `json:"enabled,omitempty"`
	StageRequired        bool   `json:"stage_required,omitempty"`
	DryRunSupported      bool   `json:"dry_run_supported,omitempty"`
	Irreversible         bool   `json:"irreversible,omitempty"`
	ApprovalBeforeCommit bool   `json:"approval_before_commit,omitempty"`
	CompensationTool     string `json:"compensation_tool,omitempty"`
}

type SideEffectReceipt struct {
	ID                 string           `json:"id"`
	CreatedAt          time.Time        `json:"created_at"`
	UpdatedAt          time.Time        `json:"updated_at"`
	TenantID           string           `json:"tenant_id,omitempty"`
	PrincipalID        string           `json:"principal_id,omitempty"`
	RunID              string           `json:"run_id,omitempty"`
	WorkflowID         string           `json:"workflow_id,omitempty"`
	NodeID             string           `json:"node_id,omitempty"`
	Attempt            int              `json:"attempt,omitempty"`
	Tool               string           `json:"tool"`
	ServerID           string           `json:"server_id,omitempty"`
	Status             SideEffectStatus `json:"status"`
	Mode               SideEffectMode   `json:"mode"`
	Irreversible       bool             `json:"irreversible,omitempty"`
	ApprovalRequired   bool             `json:"approval_required,omitempty"`
	CompensationTool   string           `json:"compensation_tool,omitempty"`
	HumanTaskID        string           `json:"human_task_id,omitempty"`
	Input              json.RawMessage  `json:"input,omitempty"`
	Preview            json.RawMessage  `json:"preview,omitempty"`
	CommitOutput       json.RawMessage  `json:"commit_output,omitempty"`
	CompensationOutput json.RawMessage  `json:"compensation_output,omitempty"`
	Error              string           `json:"error,omitempty"`
}

type SideEffectRuntimeContext struct {
	ReceiptID        string           `json:"receipt_id,omitempty"`
	Mode             SideEffectMode   `json:"mode"`
	Policy           SideEffectPolicy `json:"policy"`
	Irreversible     bool             `json:"irreversible,omitempty"`
	ApprovalRequired bool             `json:"approval_required,omitempty"`
	CompensationTool string           `json:"compensation_tool,omitempty"`
}

type sideEffectModeCtxKey struct{}
type sideEffectRuntimeCtxKey struct{}

func WithSideEffectMode(ctx context.Context, mode SideEffectMode) context.Context {
	mode = SideEffectMode(strings.TrimSpace(string(mode)))
	if mode == "" {
		return ctx
	}

	return context.WithValue(ctx, sideEffectModeCtxKey{}, mode)
}

func SideEffectModeFrom(ctx context.Context) (SideEffectMode, bool) {
	v := ctx.Value(sideEffectModeCtxKey{})
	mode, ok := v.(SideEffectMode)
	mode = SideEffectMode(strings.TrimSpace(string(mode)))

	if !ok || mode == "" {
		return "", false
	}

	return mode, true
}

func WithSideEffectRuntime(ctx context.Context, runtime SideEffectRuntimeContext) context.Context {
	runtime.ReceiptID = strings.TrimSpace(runtime.ReceiptID)
	runtime.Mode = SideEffectMode(strings.TrimSpace(string(runtime.Mode)))
	runtime.CompensationTool = strings.TrimSpace(runtime.CompensationTool)
	return context.WithValue(ctx, sideEffectRuntimeCtxKey{}, runtime)
}

func SideEffectRuntimeFrom(ctx context.Context) (SideEffectRuntimeContext, bool) {
	v := ctx.Value(sideEffectRuntimeCtxKey{})
	runtime, ok := v.(SideEffectRuntimeContext)

	if !ok {
		return SideEffectRuntimeContext{}, false
	}

	runtime.ReceiptID = strings.TrimSpace(runtime.ReceiptID)
	runtime.Mode = SideEffectMode(strings.TrimSpace(string(runtime.Mode)))
	runtime.CompensationTool = strings.TrimSpace(runtime.CompensationTool)

	return runtime, runtime.Mode != ""
}

func sideEffectReceiptKey(id string) string {
	return "side_effects/receipts/" + url.PathEscape(strings.TrimSpace(id))
}

func cloneSideEffectPolicy(policy *SideEffectPolicy) *SideEffectPolicy {
	if policy == nil {
		return nil
	}

	out := *policy
	out.CompensationTool = strings.TrimSpace(out.CompensationTool)
	return &out
}

func cloneSideEffectReceipt(receipt SideEffectReceipt) SideEffectReceipt {
	out := receipt
	out.ID = strings.TrimSpace(receipt.ID)
	out.CreatedAt = receipt.CreatedAt.UTC()
	out.UpdatedAt = receipt.UpdatedAt.UTC()
	out.TenantID = strings.TrimSpace(receipt.TenantID)
	out.PrincipalID = strings.TrimSpace(receipt.PrincipalID)
	out.RunID = strings.TrimSpace(receipt.RunID)
	out.WorkflowID = strings.TrimSpace(receipt.WorkflowID)
	out.NodeID = strings.TrimSpace(receipt.NodeID)
	out.Tool = strings.TrimSpace(receipt.Tool)
	out.ServerID = strings.TrimSpace(receipt.ServerID)
	out.Mode = SideEffectMode(strings.TrimSpace(string(receipt.Mode)))
	out.Status = SideEffectStatus(strings.TrimSpace(string(receipt.Status)))
	out.CompensationTool = strings.TrimSpace(receipt.CompensationTool)
	out.HumanTaskID = strings.TrimSpace(receipt.HumanTaskID)
	out.Input = cloneRaw(receipt.Input)
	out.Preview = cloneRaw(receipt.Preview)
	out.CommitOutput = cloneRaw(receipt.CommitOutput)
	out.CompensationOutput = cloneRaw(receipt.CompensationOutput)
	out.Error = strings.TrimSpace(receipt.Error)
	return out
}

func normalizeSideEffectPolicy(policy *SideEffectPolicy) *SideEffectPolicy {
	if policy == nil {
		return nil
	}

	out := cloneSideEffectPolicy(policy)
	if out.CompensationTool != "" {
		out.Enabled = true
	}

	if out.StageRequired || out.DryRunSupported || out.Irreversible || out.ApprovalBeforeCommit {
		out.Enabled = true
	}

	return out
}

func (r *Runner) saveSideEffectReceipt(receipt SideEffectReceipt) error {
	receipt.ID = strings.TrimSpace(receipt.ID)
	if receipt.ID == "" {
		receipt.ID = "receipt-" + time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + NewTraceID()
	}

	if receipt.CreatedAt.IsZero() {
		receipt.CreatedAt = time.Now().UTC()
	}

	receipt.UpdatedAt = time.Now().UTC()
	receipt.TenantID = strings.TrimSpace(receipt.TenantID)
	receipt.PrincipalID = strings.TrimSpace(receipt.PrincipalID)
	receipt.RunID = strings.TrimSpace(receipt.RunID)
	receipt.WorkflowID = strings.TrimSpace(receipt.WorkflowID)
	receipt.NodeID = strings.TrimSpace(receipt.NodeID)
	receipt.Tool = strings.TrimSpace(receipt.Tool)
	receipt.ServerID = strings.TrimSpace(receipt.ServerID)
	receipt.CompensationTool = strings.TrimSpace(receipt.CompensationTool)
	receipt.HumanTaskID = strings.TrimSpace(receipt.HumanTaskID)
	receipt.Error = strings.TrimSpace(receipt.Error)

	if err := putJSONKV(r.store, sideEffectReceiptKey(receipt.ID), receipt); err != nil {
		return err
	}

	ids, err := indexIDs(r.store, sideEffectReceiptIndexKey)
	if err != nil {
		return err
	}

	for _, id := range ids {
		if id == receipt.ID {
			return nil
		}
	}

	ids = append(ids, receipt.ID)
	return putJSONKV(r.store, sideEffectReceiptIndexKey, ids)
}

func (r *Runner) GetSideEffectReceipt(ctx context.Context, receiptID string) (SideEffectReceipt, error) {
	receiptID = strings.TrimSpace(receiptID)
	if receiptID == "" {
		return SideEffectReceipt{}, ErrSideEffectReceiptNotFound
	}

	var receipt SideEffectReceipt
	ok, err := getJSONKV(r.store, sideEffectReceiptKey(receiptID), &receipt)

	if err != nil {
		return SideEffectReceipt{}, err
	}

	if !ok {
		return SideEffectReceipt{}, ErrSideEffectReceiptNotFound
	}

	if err := r.ensureAgentMemoryTenantAccessFromReceipt(ctx, receipt); err != nil {
		return SideEffectReceipt{}, err
	}

	return cloneSideEffectReceipt(receipt), nil
}

func (r *Runner) ensureAgentMemoryTenantAccessFromReceipt(ctx context.Context, receipt SideEffectReceipt) error {
	if strings.TrimSpace(receipt.TenantID) == "" {
		return nil
	}

	return r.ensureRunTenantAccess(ctx, Run{
		RunID:      receipt.RunID,
		WorkflowID: receipt.WorkflowID,
		TenantID:   receipt.TenantID,
	}, "side_effect.receipt")
}

func (r *Runner) ListSideEffectReceipts(ctx context.Context, runID string, status SideEffectStatus, limit int) ([]SideEffectReceipt, error) {
	if limit <= 0 {
		limit = 100
	}
	runID = strings.TrimSpace(runID)
	status = SideEffectStatus(strings.TrimSpace(string(status)))

	ids, err := indexIDs(r.store, sideEffectReceiptIndexKey)
	if err != nil {
		return nil, err
	}

	out := make([]SideEffectReceipt, 0, min(limit, len(ids)))
	for i := len(ids) - 1; i >= 0; i-- {
		receipt, err := r.GetSideEffectReceipt(ctx, ids[i])
		if err != nil {
			continue
		}

		if runID != "" && receipt.RunID != runID {
			continue
		}

		if status != "" && receipt.Status != status {
			continue
		}

		out = append(out, receipt)
		if len(out) >= limit {
			break
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func wrapSideEffectOutput(receipt SideEffectReceipt, preview json.RawMessage) json.RawMessage {
	out, _ := json.Marshal(map[string]any{
		"side_effect_receipt_id": receipt.ID,
		"status":                 receipt.Status,
		"mode":                   receipt.Mode,
		"irreversible":           receipt.Irreversible,
		"approval_required":      receipt.ApprovalRequired,
		"preview":                preview,
	})
	return out
}

func (r *Runner) executeSideEffect(ctx context.Context, inv toolInvocation, server ToolServerDefinition, policy ToolPolicy, input json.RawMessage) (json.RawMessage, error) {
	sideEffect := normalizeSideEffectPolicy(policy.SideEffect)
	if sideEffect == nil || !sideEffect.Enabled {
		return nil, fmt.Errorf("side effect policy is required")
	}

	mode := SideEffectModeCommit
	if override, ok := SideEffectModeFrom(ctx); ok {
		mode = override
	} else if sideEffect.StageRequired {
		mode = SideEffectModeStage
	}

	receipt := SideEffectReceipt{
		TenantID:         effectiveTenantFromContext(ctx),
		RunID:            strings.TrimSpace(inv.RunID),
		WorkflowID:       strings.TrimSpace(inv.WorkflowID),
		NodeID:           strings.TrimSpace(inv.NodeID),
		Attempt:          inv.Attempt,
		Tool:             strings.TrimSpace(inv.Tool),
		ServerID:         strings.TrimSpace(server.ID),
		Mode:             mode,
		Irreversible:     sideEffect.Irreversible,
		ApprovalRequired: sideEffect.ApprovalBeforeCommit,
		CompensationTool: sideEffect.CompensationTool,
		Input:            cloneRaw(input),
	}

	if principal, ok := PrincipalFrom(ctx); ok {
		receipt.PrincipalID = strings.TrimSpace(principal.ID)
	}

	switch mode {
	case SideEffectModeDryRun, SideEffectModeStage:
		runtimeCtx := WithSideEffectRuntime(ctx, SideEffectRuntimeContext{
			Mode:             mode,
			Irreversible:     sideEffect.Irreversible,
			ApprovalRequired: sideEffect.ApprovalBeforeCommit,
			CompensationTool: sideEffect.CompensationTool,
		})

		preview, err := inv.Handler(runtimeCtx, cloneRaw(input))
		if err != nil {
			receipt.Status = SideEffectStatusCompensationFailed
			receipt.Error = err.Error()
			_ = r.saveSideEffectReceipt(receipt)
			return nil, err
		}

		receipt.Preview = cloneRaw(preview)
		if mode == SideEffectModeDryRun {
			receipt.Status = SideEffectStatusDryRun
		} else {
			receipt.Status = SideEffectStatusStaged
		}

		_ = r.saveSideEffectReceipt(receipt)
		return wrapSideEffectOutput(receipt, receipt.Preview), nil

	case SideEffectModeCommit:
		runtimeCtx := WithSideEffectRuntime(ctx, SideEffectRuntimeContext{
			Mode:             SideEffectModeCommit,
			Irreversible:     sideEffect.Irreversible,
			ApprovalRequired: sideEffect.ApprovalBeforeCommit,
			CompensationTool: sideEffect.CompensationTool,
		})

		output, err := inv.Handler(runtimeCtx, cloneRaw(input))
		if err != nil {
			receipt.Status = SideEffectStatusCompensationFailed
			receipt.Error = err.Error()
			_ = r.saveSideEffectReceipt(receipt)
			return nil, err
		}

		receipt.Status = SideEffectStatusCommitted
		receipt.CommitOutput = cloneRaw(output)
		_ = r.saveSideEffectReceipt(receipt)
		return output, nil

	default:
		return nil, fmt.Errorf("unsupported side-effect mode %q", mode)
	}
}

func (r *Runner) findSideEffectHumanTask(receiptID string) (HumanTask, bool, error) {
	tasks, err := r.ListHumanTasks("", "", 500)
	if err != nil {
		return HumanTask{}, false, err
	}

	for _, task := range tasks {
		if task.SideEffectReceiptID == strings.TrimSpace(receiptID) {
			return task, true, nil
		}
	}

	return HumanTask{}, false, nil
}

func (r *Runner) ensureSideEffectApproval(ctx context.Context, receipt *SideEffectReceipt) error {
	if receipt == nil || !receipt.ApprovalRequired {
		return nil
	}

	task, ok, err := r.findSideEffectHumanTask(receipt.ID)
	if err != nil {
		return err
	}

	if ok {
		receipt.HumanTaskID = task.ID
		switch task.Status {
		case HumanTaskApproved:
			_ = r.saveSideEffectReceipt(*receipt)
			return nil
		case HumanTaskRejected, HumanTaskTimedOut, HumanTaskCanceled:
			receipt.Status = SideEffectStatusCompensationFailed
			receipt.Error = "side-effect commit approval rejected"
			_ = r.saveSideEffectReceipt(*receipt)
			return fmt.Errorf("%w: approval rejected", ErrSideEffectCommitPending)
		default:
			receipt.Status = SideEffectStatusCommitPending
			_ = r.saveSideEffectReceipt(*receipt)
			return &HumanApprovalPendingError{Task: task}
		}
	}

	task = HumanTask{
		RunID:               receipt.RunID,
		WorkflowID:          receipt.WorkflowID,
		NodeID:              receipt.NodeID,
		Attempt:             receipt.Attempt,
		TenantID:            receipt.TenantID,
		Mode:                HumanStepModeApproval,
		Source:              HumanTaskSourceSideEffect,
		Status:              HumanTaskPending,
		Prompt:              "approve side-effect commit for tool " + receipt.Tool,
		RequestedInput:      cloneRaw(receipt.Input),
		SideEffectReceiptID: receipt.ID,
	}

	if err := r.saveHumanTask(task); err != nil {
		return err
	}

	payload, _ := json.Marshal(task)
	_, _ = r.store.AppendEvent(RunEvent{
		RunID:      receipt.RunID,
		Type:       EventHumanTaskCreated,
		WorkflowID: receipt.WorkflowID,
		NodeID:     receipt.NodeID,
		Attempt:    receipt.Attempt,
		Payload:    payload,
	})

	r.appendAuditRecord(ctx, AuditRecord{
		TenantID:     receipt.TenantID,
		Action:       "human.request",
		ResourceType: "side_effect_receipt",
		ResourceID:   receipt.ID,
		RunID:        receipt.RunID,
		WorkflowID:   receipt.WorkflowID,
		Outcome:      AuditOutcomeDenied,
		Reason:       "side-effect commit requires approval",
	})

	receipt.HumanTaskID = task.ID
	receipt.Status = SideEffectStatusCommitPending
	_ = r.saveSideEffectReceipt(*receipt)
	return &HumanApprovalPendingError{Task: task}
}

func (r *Runner) CommitSideEffect(ctx context.Context, receiptID string) (SideEffectReceipt, error) {
	receipt, err := r.GetSideEffectReceipt(ctx, receiptID)
	if err != nil {
		return SideEffectReceipt{}, err
	}

	if receipt.Status != SideEffectStatusStaged && receipt.Status != SideEffectStatusCommitPending {
		return receipt, nil
	}

	if err := r.ensureSideEffectApproval(ctx, &receipt); err != nil {
		return receipt, err
	}

	reg := r.HandlerRegistryForTenant(receipt.TenantID)
	if reg == nil {
		return SideEffectReceipt{}, fmt.Errorf("handler registry is required")
	}

	handler, ok := reg.Lookup(receipt.Tool)
	if !ok {
		return SideEffectReceipt{}, fmt.Errorf("no handler registered for topic %q", receipt.Tool)
	}

	policy, err := r.resolveToolPolicy(WithTenantID(ctx, receipt.TenantID), receipt.Tool)
	if err != nil {
		return SideEffectReceipt{}, err
	}

	server, err := r.resolveToolServer(WithTenantID(ctx, receipt.TenantID), policy)
	if err != nil {
		return SideEffectReceipt{}, err
	}
	runtimeCtx := WithTenantID(ctx, receipt.TenantID)
	sidePolicy := normalizeSideEffectPolicy(policy.SideEffect)
	if sidePolicy == nil {
		sidePolicy = &SideEffectPolicy{Enabled: true}
	}

	runtimeCtx = WithSideEffectRuntime(runtimeCtx, SideEffectRuntimeContext{
		ReceiptID:        receipt.ID,
		Mode:             SideEffectModeCommit,
		Policy:           *sidePolicy,
		Irreversible:     receipt.Irreversible,
		ApprovalRequired: receipt.ApprovalRequired,
		CompensationTool: receipt.CompensationTool,
	})

	runtimeCtx = WithToolRuntime(runtimeCtx, ToolRuntimeContext{
		Tool:       receipt.Tool,
		ServerID:   server.ID,
		ServerKind: server.Kind,
		Sandboxed:  false,
		Policy:     cloneToolPolicy(policy),
		Server:     cloneToolServerDefinition(server),
	})

	output, err := handler(runtimeCtx, cloneRaw(receipt.Input))
	if err != nil {
		receipt.Error = err.Error()
		_ = r.saveSideEffectReceipt(receipt)
		return receipt, err
	}

	receipt.Status = SideEffectStatusCommitted
	receipt.CommitOutput = cloneRaw(output)
	receipt.Error = ""

	if err := r.saveSideEffectReceipt(receipt); err != nil {
		return SideEffectReceipt{}, err
	}

	r.appendAuditRecord(ctx, AuditRecord{
		TenantID:     receipt.TenantID,
		Action:       "side_effect.commit",
		ResourceType: "side_effect_receipt",
		ResourceID:   receipt.ID,
		RunID:        receipt.RunID,
		WorkflowID:   receipt.WorkflowID,
		Outcome:      AuditOutcomeAllowed,
		Reason:       "side-effect committed",
	})

	return receipt, nil
}

func (r *Runner) CompensateSideEffect(ctx context.Context, receiptID string) (SideEffectReceipt, error) {
	receipt, err := r.GetSideEffectReceipt(ctx, receiptID)
	if err != nil {
		return SideEffectReceipt{}, err
	}

	if strings.TrimSpace(receipt.CompensationTool) == "" {
		return receipt, ErrSideEffectNotCompensable
	}

	reg := r.HandlerRegistryForTenant(receipt.TenantID)
	if reg == nil {
		return SideEffectReceipt{}, fmt.Errorf("handler registry is required")
	}

	handler, ok := reg.Lookup(receipt.CompensationTool)
	if !ok {
		return SideEffectReceipt{}, fmt.Errorf("no handler registered for topic %q", receipt.CompensationTool)
	}

	body, _ := json.Marshal(map[string]any{
		"receipt_id": receipt.ID,
		"tool":       receipt.Tool,
		"input":      receipt.Input,
		"output":     receipt.CommitOutput,
	})

	runtimeCtx := WithTenantID(ctx, receipt.TenantID)
	runtimeCtx = WithSideEffectRuntime(runtimeCtx, SideEffectRuntimeContext{
		ReceiptID:        receipt.ID,
		Mode:             SideEffectModeCompensate,
		Irreversible:     receipt.Irreversible,
		ApprovalRequired: receipt.ApprovalRequired,
		CompensationTool: receipt.CompensationTool,
	})

	output, err := handler(runtimeCtx, body)
	if err != nil {
		receipt.Status = SideEffectStatusCompensationFailed
		receipt.Error = err.Error()
		_ = r.saveSideEffectReceipt(receipt)
		return receipt, err
	}

	receipt.Status = SideEffectStatusCompensated
	receipt.CompensationOutput = cloneRaw(output)
	receipt.Error = ""

	if err := r.saveSideEffectReceipt(receipt); err != nil {
		return SideEffectReceipt{}, err
	}

	r.appendAuditRecord(ctx, AuditRecord{
		TenantID:     receipt.TenantID,
		Action:       "side_effect.compensate",
		ResourceType: "side_effect_receipt",
		ResourceID:   receipt.ID,
		RunID:        receipt.RunID,
		WorkflowID:   receipt.WorkflowID,
		Outcome:      AuditOutcomeAllowed,
		Reason:       "side-effect compensated",
	})

	return receipt, nil
}
