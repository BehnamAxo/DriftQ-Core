package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestSideEffects_StageCommitCompensate(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)

	commitCount := 0
	compCount := 0
	reg.Register("email.send", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		runtime, ok := SideEffectRuntimeFrom(ctx)
		if !ok {
			t.Fatal("expected side-effect runtime context")
		}
		switch runtime.Mode {
		case SideEffectModeStage:
			return json.RawMessage(`{"preview":"prepared"}`), nil
		case SideEffectModeCommit:
			commitCount++
			return json.RawMessage(`{"sent":true}`), nil
		default:
			return nil, nil
		}
	})

	reg.Register("email.undo", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		runtime, ok := SideEffectRuntimeFrom(ctx)
		if !ok || runtime.Mode != SideEffectModeCompensate {
			t.Fatalf("expected compensate side-effect runtime, got %+v ok=%v", runtime, ok)
		}

		compCount++
		return json.RawMessage(`{"undone":true}`), nil
	})

	if err := runner.SaveToolGatewayBundle(ToolGatewayBundle{
		Tools: []ToolPolicy{{
			ID:       "email-send",
			Tool:     "email.send",
			Approved: true,
			SideEffect: &SideEffectPolicy{
				Enabled:          true,
				StageRequired:    true,
				DryRunSupported:  true,
				CompensationTool: "email.undo",
			},
		}},
	}); err != nil {
		t.Fatalf("SaveToolGatewayBundle: %v", err)
	}

	ctx := WithTenantID(context.Background(), "tenant-a")
	spec := []byte(`{"id":"wf_email","nodes":[{"id":"send","topic":"email.send"}]}`)

	if err := runner.RunSpecJSON(ctx, "run-email-stage", spec, reg, json.RawMessage(`{"to":"a@example.com"}`)); err != nil {
		t.Fatalf("RunSpecJSON stage: %v", err)
	}

	receipts, err := runner.ListSideEffectReceipts(ctx, "run-email-stage", "", 10)
	if err != nil {
		t.Fatalf("ListSideEffectReceipts: %v", err)
	}

	if len(receipts) != 1 || receipts[0].Status != SideEffectStatusStaged {
		t.Fatalf("expected 1 staged receipt, got %+v", receipts)
	}

	if commitCount != 0 {
		t.Fatalf("expected no commit during stage, got %d", commitCount)
	}

	receipt, err := runner.CommitSideEffect(ctx, receipts[0].ID)
	if err != nil {
		t.Fatalf("CommitSideEffect: %v", err)
	}

	if receipt.Status != SideEffectStatusCommitted || commitCount != 1 {
		t.Fatalf("expected committed receipt, got receipt=%+v commitCount=%d", receipt, commitCount)
	}

	receipt, err = runner.CompensateSideEffect(ctx, receipt.ID)
	if err != nil {
		t.Fatalf("CompensateSideEffect: %v", err)
	}

	if receipt.Status != SideEffectStatusCompensated || compCount != 1 {
		t.Fatalf("expected compensated receipt, got receipt=%+v compCount=%d", receipt, compCount)
	}
}

func TestSideEffects_ApprovalBeforeCommit(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)

	reg.Register("shell.exec", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		runtime, _ := SideEffectRuntimeFrom(ctx)
		switch runtime.Mode {
		case SideEffectModeStage:
			return json.RawMessage(`{"preview":"rm -rf /danger"}`), nil
		case SideEffectModeCommit:
			return json.RawMessage(`{"done":true}`), nil
		default:
			return nil, nil
		}
	})

	if err := runner.SaveToolGatewayBundle(ToolGatewayBundle{
		Tools: []ToolPolicy{{
			ID:       "shell-exec",
			Tool:     "shell.exec",
			Approved: true,
			SideEffect: &SideEffectPolicy{
				Enabled:              true,
				StageRequired:        true,
				Irreversible:         true,
				ApprovalBeforeCommit: true,
			},
		}},
	}); err != nil {
		t.Fatalf("SaveToolGatewayBundle: %v", err)
	}

	ctx := WithTenantID(context.Background(), "tenant-a")
	spec := []byte(`{"id":"wf_shell","nodes":[{"id":"run","topic":"shell.exec"}]}`)
	if err := runner.RunSpecJSON(ctx, "run-shell-stage", spec, reg, json.RawMessage(`{"cmd":"rm -rf /danger"}`)); err != nil {
		t.Fatalf("RunSpecJSON stage: %v", err)
	}

	receipts, err := runner.ListSideEffectReceipts(ctx, "run-shell-stage", "", 10)
	if err != nil {
		t.Fatalf("ListSideEffectReceipts: %v", err)
	}

	if len(receipts) != 1 {
		t.Fatalf("expected one receipt, got %+v", receipts)
	}

	_, err = runner.CommitSideEffect(ctx, receipts[0].ID)
	var pendingErr *HumanApprovalPendingError
	if !errors.As(err, &pendingErr) {
		t.Fatalf("expected HumanApprovalPendingError, got %v", err)
	}

	tasks, err := runner.ListHumanTasks("run-shell-stage", HumanTaskPending, 10)
	if err != nil {
		t.Fatalf("ListHumanTasks: %v", err)
	}

	if len(tasks) != 1 || tasks[0].SideEffectReceiptID != receipts[0].ID {
		t.Fatalf("expected pending side-effect approval task, got %+v", tasks)
	}

	if _, err := runner.ResolveHumanTask(ctx, tasks[0].ID, HumanDecisionApprove, nil, "approved", false); err != nil {
		t.Fatalf("ResolveHumanTask: %v", err)
	}

	receipt, err := runner.CommitSideEffect(ctx, receipts[0].ID)
	if err != nil {
		t.Fatalf("CommitSideEffect after approval: %v", err)
	}

	if receipt.Status != SideEffectStatusCommitted {
		t.Fatalf("expected committed receipt after approval, got %+v", receipt)
	}
}

func TestSideEffects_DryRunMode(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)

	commitCount := 0
	reg.Register("http.post", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		runtime, _ := SideEffectRuntimeFrom(ctx)
		if runtime.Mode == SideEffectModeDryRun {
			return json.RawMessage(`{"preview":"would post"}`), nil
		}
		commitCount++
		return json.RawMessage(`{"posted":true}`), nil
	})

	if err := runner.SaveToolGatewayBundle(ToolGatewayBundle{
		Tools: []ToolPolicy{{
			ID:       "http-post",
			Tool:     "http.post",
			Approved: true,
			SideEffect: &SideEffectPolicy{
				Enabled:         true,
				DryRunSupported: true,
			},
		}},
	}); err != nil {
		t.Fatalf("SaveToolGatewayBundle: %v", err)
	}

	ctx := WithTenantID(context.Background(), "tenant-a")
	ctx = WithSideEffectMode(ctx, SideEffectModeDryRun)
	spec := []byte(`{"id":"wf_post","nodes":[{"id":"post","topic":"http.post"}]}`)

	if err := runner.RunSpecJSON(ctx, "run-dry-side-effect", spec, reg, json.RawMessage(`{"url":"https://example.test"}`)); err != nil {
		t.Fatalf("RunSpecJSON dry-run: %v", err)
	}

	receipts, err := runner.ListSideEffectReceipts(WithTenantID(context.Background(), "tenant-a"), "run-dry-side-effect", "", 10)
	if err != nil {
		t.Fatalf("ListSideEffectReceipts: %v", err)
	}

	if len(receipts) != 1 || receipts[0].Status != SideEffectStatusDryRun || commitCount != 0 {
		t.Fatalf("expected dry-run receipt with no commit, got receipts=%+v commitCount=%d", receipts, commitCount)
	}
}
