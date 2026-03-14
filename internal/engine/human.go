package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

var (
	ErrHumanApprovalPending = errors.New("human approval pending")
	ErrHumanTaskNotFound    = errors.New("human task not found")
	ErrHumanTaskResolved    = errors.New("human task already resolved")
)

const humanTaskIndexKey = "human/tasks/index"

type HumanStepMode string
type HumanTimeoutAction string
type HumanTaskStatus string
type HumanTaskSource string
type HumanDecision string

const (
	HumanStepModeApproval HumanStepMode = "approval"
	HumanStepModeEdit     HumanStepMode = "edit"
)

const (
	HumanTimeoutReject  HumanTimeoutAction = "reject"
	HumanTimeoutApprove HumanTimeoutAction = "approve"
	HumanTimeoutCancel  HumanTimeoutAction = "cancel"
)

const (
	HumanTaskPending  HumanTaskStatus = "pending"
	HumanTaskApproved HumanTaskStatus = "approved"
	HumanTaskRejected HumanTaskStatus = "rejected"
	HumanTaskTimedOut HumanTaskStatus = "timed_out"
	HumanTaskCanceled HumanTaskStatus = "canceled"
)

const (
	HumanTaskSourceNode HumanTaskSource = "node"
	HumanTaskSourceRisk HumanTaskSource = "risk"
)

const (
	HumanDecisionApprove HumanDecision = "approve"
	HumanDecisionReject  HumanDecision = "reject"
)

type HumanStepSpec struct {
	Mode      HumanStepMode      `json:"mode,omitempty" yaml:"mode,omitempty"`
	Prompt    string             `json:"prompt,omitempty" yaml:"prompt,omitempty"`
	TimeoutMS int64              `json:"timeout_ms,omitempty" yaml:"timeout_ms,omitempty"`
	OnTimeout HumanTimeoutAction `json:"on_timeout,omitempty" yaml:"on_timeout,omitempty"`
}

type HumanTask struct {
	ID             string              `json:"id"`
	RunID          string              `json:"run_id"`
	WorkflowID     string              `json:"workflow_id,omitempty"`
	NodeID         string              `json:"node_id,omitempty"`
	Attempt        int                 `json:"attempt,omitempty"`
	TenantID       string              `json:"tenant_id,omitempty"`
	Mode           HumanStepMode       `json:"mode"`
	Source         HumanTaskSource     `json:"source"`
	Status         HumanTaskStatus     `json:"status"`
	Prompt         string              `json:"prompt,omitempty"`
	RequestedInput json.RawMessage     `json:"requested_input,omitempty"`
	EditedInput    json.RawMessage     `json:"edited_input,omitempty"`
	ReviewComment  string              `json:"review_comment,omitempty"`
	TimeoutAction  HumanTimeoutAction  `json:"timeout_action,omitempty"`
	CreatedAt      time.Time           `json:"created_at"`
	UpdatedAt      time.Time           `json:"updated_at"`
	ResolvedAt     *time.Time          `json:"resolved_at,omitempty"`
	ExpiresAt      *time.Time          `json:"expires_at,omitempty"`
	RiskReport     *WorkflowRiskReport `json:"risk_report,omitempty"`
}

type HumanApprovalPendingError struct {
	Task HumanTask
}

func (e *HumanApprovalPendingError) Error() string {
	if strings.TrimSpace(e.Task.NodeID) != "" {
		return "human approval pending for node " + e.Task.NodeID
	}

	return ErrHumanApprovalPending.Error()
}

func (e *HumanApprovalPendingError) Unwrap() error {
	return ErrHumanApprovalPending
}

func (s *HumanStepSpec) NormalizeAndValidate() error {
	if s == nil {
		return fmt.Errorf("human step is nil")
	}

	s.Mode = HumanStepMode(strings.TrimSpace(string(s.Mode)))
	s.Prompt = strings.TrimSpace(s.Prompt)
	s.OnTimeout = HumanTimeoutAction(strings.TrimSpace(string(s.OnTimeout)))

	if s.Mode == "" {
		s.Mode = HumanStepModeApproval
	}

	switch s.Mode {
	case HumanStepModeApproval, HumanStepModeEdit:
	default:
		return fmt.Errorf("invalid human step mode %q", s.Mode)
	}

	if s.OnTimeout == "" {
		s.OnTimeout = HumanTimeoutReject
	}

	switch s.OnTimeout {
	case HumanTimeoutReject, HumanTimeoutApprove, HumanTimeoutCancel:
	default:
		return fmt.Errorf("invalid human on_timeout %q", s.OnTimeout)
	}

	if s.TimeoutMS < 0 {
		s.TimeoutMS = 0
	}

	return nil
}

func cloneHumanStepSpec(spec *HumanStepSpec) *HumanStepSpec {
	if spec == nil {
		return nil
	}

	out := *spec
	return &out
}

func humanTaskKey(id string) string {
	return "human/tasks/" + strings.TrimSpace(id)
}

func (r *Runner) saveHumanTask(task HumanTask) error {
	task.ID = strings.TrimSpace(task.ID)
	if task.ID == "" {
		task.ID = "human-" + time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + NewTraceID()
	}

	task.RunID = strings.TrimSpace(task.RunID)
	task.WorkflowID = strings.TrimSpace(task.WorkflowID)
	task.NodeID = strings.TrimSpace(task.NodeID)
	task.TenantID = strings.TrimSpace(task.TenantID)
	task.Mode = HumanStepMode(strings.TrimSpace(string(task.Mode)))
	task.Source = HumanTaskSource(strings.TrimSpace(string(task.Source)))
	task.Status = HumanTaskStatus(strings.TrimSpace(string(task.Status)))
	task.Prompt = strings.TrimSpace(task.Prompt)
	task.ReviewComment = strings.TrimSpace(task.ReviewComment)
	task.TimeoutAction = HumanTimeoutAction(strings.TrimSpace(string(task.TimeoutAction)))

	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now().UTC()
	}

	task.UpdatedAt = time.Now().UTC()
	if task.ExpiresAt != nil {
		t := task.ExpiresAt.UTC()
		task.ExpiresAt = &t
	}

	if task.ResolvedAt != nil {
		t := task.ResolvedAt.UTC()
		task.ResolvedAt = &t
	}

	if err := putJSONKV(r.store, humanTaskKey(task.ID), task); err != nil {
		return err
	}

	ids, err := indexIDs(r.store, humanTaskIndexKey)
	if err != nil {
		return err
	}

	for _, existing := range ids {
		if existing == task.ID {
			return nil
		}
	}

	ids = append(ids, task.ID)
	return putJSONKV(r.store, humanTaskIndexKey, ids)
}

func (r *Runner) GetHumanTask(taskID string) (HumanTask, bool, error) {
	var task HumanTask
	ok, err := getJSONKV(r.store, humanTaskKey(taskID), &task)
	return task, ok, err
}

func (r *Runner) ListHumanTasks(runID string, status HumanTaskStatus, limit int) ([]HumanTask, error) {
	if limit <= 0 {
		limit = 100
	}

	runID = strings.TrimSpace(runID)
	status = HumanTaskStatus(strings.TrimSpace(string(status)))
	ids, err := indexIDs(r.store, humanTaskIndexKey)
	if err != nil {
		return nil, err
	}

	out := make([]HumanTask, 0, min(limit, len(ids)))
	for i := len(ids) - 1; i >= 0; i-- {
		task, ok, err := r.GetHumanTask(ids[i])
		if err != nil || !ok {
			continue
		}

		if runID != "" && task.RunID != runID {
			continue
		}

		if status != "" && task.Status != status {
			continue
		}

		out = append(out, task)
		if len(out) >= limit {
			break
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func (r *Runner) findHumanTask(runID, nodeID string, attempt int, source HumanTaskSource) (HumanTask, bool, error) {
	tasks, err := r.ListHumanTasks(runID, "", 500)
	if err != nil {
		return HumanTask{}, false, err
	}

	source = HumanTaskSource(strings.TrimSpace(string(source)))
	var fallback *HumanTask
	for _, task := range tasks {
		if task.NodeID != strings.TrimSpace(nodeID) {
			continue
		}

		if source != "" && task.Source != source {
			continue
		}

		if task.Attempt == attempt {
			return task, true, nil
		}

		if fallback == nil {
			copy := task
			fallback = &copy
		}
	}

	if fallback != nil {
		return *fallback, true, nil
	}

	return HumanTask{}, false, nil
}

func (r *Runner) cancelHumanTimers(runID, nodeID string, attempt int) {
	for _, timer := range r.store.ListTimers(runID) {
		if timer.NodeID != nodeID || timer.Attempt != attempt || timer.Status != TimerScheduled {
			continue
		}

		now := time.Now().UTC()
		timer.Status = TimerCanceled
		timer.FiredAt = &now
		_ = r.store.UpsertTimer(timer)
		_, _ = r.store.AppendEvent(RunEvent{
			RunID:      runID,
			Type:       EventTimerCanceled,
			WorkflowID: timer.WorkflowID,
			NodeID:     nodeID,
			Attempt:    attempt,
		})
	}
}

func (r *Runner) humanTimeoutFired(runID, nodeID string, attempt int) bool {
	for _, timer := range r.store.ListTimers(runID) {
		if timer.NodeID != nodeID || timer.Attempt != attempt {
			continue
		}

		if !strings.HasPrefix(timer.Reason, "human_timeout:") {
			continue
		}

		if timer.Status == TimerFired {
			return true
		}
	}

	return false
}

func (r *Runner) ResolveHumanTask(ctx context.Context, taskID string, decision HumanDecision, editedInput json.RawMessage, comment string, resume bool) (HumanTask, error) {
	task, ok, err := r.GetHumanTask(taskID)
	if err != nil {
		return HumanTask{}, err
	}

	if !ok {
		return HumanTask{}, ErrHumanTaskNotFound
	}

	if task.Status != HumanTaskPending {
		return HumanTask{}, ErrHumanTaskResolved
	}

	if run, ok := r.store.GetRun(task.RunID); ok {
		if err := r.ensureRunTenantAccess(ctx, run, "human.resolve"); err != nil {
			return HumanTask{}, err
		}
	}

	now := time.Now().UTC()
	task.ReviewComment = strings.TrimSpace(comment)
	task.EditedInput = cloneRaw(editedInput)
	task.UpdatedAt = now
	task.ResolvedAt = &now

	switch HumanDecision(strings.TrimSpace(string(decision))) {
	case HumanDecisionApprove:
		task.Status = HumanTaskApproved
	case HumanDecisionReject:
		task.Status = HumanTaskRejected
	default:
		return HumanTask{}, fmt.Errorf("invalid human decision %q", decision)
	}

	if err := r.saveHumanTask(task); err != nil {
		return HumanTask{}, err
	}

	if task.NodeID != "" {
		r.cancelHumanTimers(task.RunID, task.NodeID, task.Attempt)
	}

	r.appendAuditRecord(ctx, AuditRecord{
		TenantID:     task.TenantID,
		Action:       "human.resolve",
		ResourceType: "human_task",
		ResourceID:   task.ID,
		RunID:        task.RunID,
		WorkflowID:   task.WorkflowID,
		Outcome: func() AuditOutcome {
			if task.Status == HumanTaskApproved {
				return AuditOutcomeAllowed
			}
			return AuditOutcomeDenied
		}(),
		Reason: string(task.Status),
	})

	payload, _ := json.Marshal(task)
	_, _ = r.store.AppendEvent(RunEvent{
		RunID:      task.RunID,
		Type:       EventHumanTaskResolved,
		WorkflowID: task.WorkflowID,
		NodeID:     task.NodeID,
		Attempt:    task.Attempt,
		Payload:    payload,
	})

	if task.Source == HumanTaskSourceRisk && task.Status == HumanTaskRejected {
		if run, ok := r.store.GetRun(task.RunID); ok {
			end := time.Now().UTC()
			run.Status = RunStatusFailed
			run.EndedAt = &end
			run.TerminalReason = "human_rejected"
			_ = r.store.UpdateRun(run)
		}
		return task, nil
	}

	if resume {
		_, _ = r.resumeWaitingRun(ctx, task.RunID)
	}

	return task, nil
}

func (r *Runner) stageRiskApproval(ctx context.Context, runID string, g WorkflowGraph, spec json.RawMessage, initialInput json.RawMessage, report WorkflowRiskReport) (HumanTask, error) {
	if len(spec) == 0 {
		return HumanTask{}, &RiskError{Report: report}
	}

	task, ok, err := r.findHumanTask(runID, "", 0, HumanTaskSourceRisk)
	if err == nil && ok {
		return task, nil
	}

	run, ok := r.store.GetRun(runID)
	if !ok {
		run = Run{
			RunID:        runID,
			WorkflowID:   strings.TrimSpace(g.ID),
			Status:       RunStatusWaiting,
			Spec:         cloneRaw(spec),
			InitialInput: cloneRaw(initialInput),
			TenantID:     report.TenantID,
		}

		if err := r.store.CreateRun(run); err != nil {
			return HumanTask{}, err
		}

		_, _ = r.store.AppendEvent(RunEvent{
			RunID:      runID,
			Type:       EventRunCreated,
			WorkflowID: run.WorkflowID,
		})
	} else if run.Status != RunStatusWaiting {
		run.Status = RunStatusWaiting
		run.Spec = cloneRaw(spec)
		run.InitialInput = cloneRaw(initialInput)

		if err := r.store.UpdateRun(run); err != nil {
			return HumanTask{}, err
		}
	}

	task = HumanTask{
		RunID:          runID,
		WorkflowID:     strings.TrimSpace(g.ID),
		TenantID:       report.TenantID,
		Mode:           HumanStepModeApproval,
		Source:         HumanTaskSourceRisk,
		Status:         HumanTaskPending,
		Prompt:         report.Reason,
		RequestedInput: cloneRaw(initialInput),
		RiskReport:     &report,
	}

	if err := r.saveHumanTask(task); err != nil {
		return HumanTask{}, err
	}

	payload, _ := json.Marshal(task)
	_, _ = r.store.AppendEvent(RunEvent{
		RunID:      runID,
		Type:       EventHumanTaskCreated,
		WorkflowID: g.ID,
		Payload:    payload,
	})

	r.appendAuditRecord(ctx, AuditRecord{
		TenantID:     task.TenantID,
		Action:       "human.request",
		ResourceType: "human_task",
		ResourceID:   task.ID,
		RunID:        task.RunID,
		WorkflowID:   task.WorkflowID,
		Outcome:      AuditOutcomeDenied,
		Reason:       "risk escalation requires approval",
	})

	return task, nil
}

func (r *Runner) maybeHandleHumanNode(ctx context.Context, runID, workflowID string, node NodeDef, attempt int, input json.RawMessage, started time.Time) (bool, nodeResult, error) {
	if node.Human == nil {
		return false, nodeResult{}, nil
	}

	task, ok, err := r.findHumanTask(runID, node.NodeID, attempt, HumanTaskSourceNode)
	if err != nil {
		return true, nodeResult{}, err
	}

	if !ok {
		task = HumanTask{
			RunID:          runID,
			WorkflowID:     workflowID,
			NodeID:         node.NodeID,
			Attempt:        attempt,
			TenantID:       effectiveTenantFromContext(ctx),
			Mode:           node.Human.Mode,
			Source:         HumanTaskSourceNode,
			Status:         HumanTaskPending,
			Prompt:         node.Human.Prompt,
			RequestedInput: cloneRaw(input),
			TimeoutAction:  node.Human.OnTimeout,
		}

		if node.Human.TimeoutMS > 0 {
			expires := started.Add(time.Duration(node.Human.TimeoutMS) * time.Millisecond).UTC()
			task.ExpiresAt = &expires
		}

		if err := r.saveHumanTask(task); err != nil {
			return true, nodeResult{}, err
		}

		waitingNode := NodeExecution{
			RunID:      runID,
			WorkflowID: workflowID,
			NodeID:     node.NodeID,
			Attempt:    attempt,
			Status:     NodeStatusWaiting,
			StartedAt:  &started,
			Input:      cloneRaw(input),
		}

		if err := r.store.UpsertNodeExecution(waitingNode); err != nil {
			return true, nodeResult{}, err
		}

		_, _ = r.store.AppendEvent(RunEvent{
			RunID:      runID,
			Type:       EventNodeStarted,
			WorkflowID: workflowID,
			NodeID:     node.NodeID,
			Attempt:    attempt,
		})

		payload, _ := json.Marshal(task)
		_, _ = r.store.AppendEvent(RunEvent{
			RunID:      runID,
			Type:       EventHumanTaskCreated,
			WorkflowID: workflowID,
			NodeID:     node.NodeID,
			Attempt:    attempt,
			Payload:    payload,
		})

		if task.ExpiresAt != nil {
			_ = r.store.UpsertTimer(Timer{
				RunID:      runID,
				WorkflowID: workflowID,
				NodeID:     node.NodeID,
				Attempt:    attempt,
				Status:     TimerScheduled,
				FireAt:     *task.ExpiresAt,
				CreatedAt:  started,
				Reason:     "human_timeout:" + task.ID,
			})
			_, _ = r.store.AppendEvent(RunEvent{
				RunID:      runID,
				Type:       EventTimerScheduled,
				WorkflowID: workflowID,
				NodeID:     node.NodeID,
				Attempt:    attempt,
			})
		}

		r.appendAuditRecord(ctx, AuditRecord{
			TenantID:     task.TenantID,
			Action:       "human.request",
			ResourceType: "human_task",
			ResourceID:   task.ID,
			RunID:        runID,
			WorkflowID:   workflowID,
			Outcome:      AuditOutcomeDenied,
			Reason:       "node requires human approval",
		})

		return true, nodeResult{nodeID: node.NodeID, attempt: attempt, started: started, ended: started, input: cloneRaw(input), err: ErrHumanApprovalPending}, nil
	}

	taskAttempt := attempt
	if task.Attempt > 0 {
		taskAttempt = task.Attempt
	}

	if task.Status == HumanTaskPending && ((task.ExpiresAt != nil && !task.ExpiresAt.After(time.Now().UTC())) || r.humanTimeoutFired(runID, node.NodeID, taskAttempt)) {
		now := time.Now().UTC()
		task.UpdatedAt = now
		task.ResolvedAt = &now

		switch task.TimeoutAction {
		case HumanTimeoutApprove:
			task.Status = HumanTaskApproved
		case HumanTimeoutCancel:
			task.Status = HumanTaskCanceled
		default:
			task.Status = HumanTaskTimedOut
		}

		if err := r.saveHumanTask(task); err != nil {
			return true, nodeResult{}, err
		}

		payload, _ := json.Marshal(task)
		_, _ = r.store.AppendEvent(RunEvent{
			RunID:      runID,
			Type:       EventHumanTaskResolved,
			WorkflowID: workflowID,
			NodeID:     node.NodeID,
			Attempt:    attempt,
			Payload:    payload,
		})
	}

	if task.Status == HumanTaskPending {
		return true, nodeResult{nodeID: node.NodeID, attempt: attempt, started: started, ended: started, input: cloneRaw(input), err: ErrHumanApprovalPending}, nil
	}

	out := cloneRaw(input)
	if len(task.EditedInput) > 0 {
		out = cloneRaw(task.EditedInput)
	}

	switch task.Status {
	case HumanTaskApproved:
		return true, nodeResult{
			nodeID:  node.NodeID,
			attempt: attempt,
			started: started,
			ended:   time.Now().UTC(),
			input:   cloneRaw(input),
			output:  out,
		}, nil
	case HumanTaskRejected, HumanTaskTimedOut:
		return true, nodeResult{
			nodeID:  node.NodeID,
			attempt: attempt,
			started: started,
			ended:   time.Now().UTC(),
			input:   cloneRaw(input),
			err:     fmt.Errorf("human step %s", task.Status),
		}, nil
	case HumanTaskCanceled:
		return true, nodeResult{
			nodeID:  node.NodeID,
			attempt: attempt,
			started: started,
			ended:   time.Now().UTC(),
			input:   cloneRaw(input),
			err:     ErrRunCanceled,
		}, nil
	default:
		return true, nodeResult{}, fmt.Errorf("unexpected human task status %q", task.Status)
	}
}
