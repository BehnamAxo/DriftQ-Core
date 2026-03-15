package engine

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestWorkflowReleaseVersioningAndDiff(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())

	v1 := WorkflowReleaseVersion{
		ID:             "wf-orders-v1",
		WorkflowID:     "wf_orders",
		Spec:           json.RawMessage(`{"id":"wf_orders","nodes":[{"id":"draft","topic":"notify.send"}]}`),
		PromptSnapshot: json.RawMessage(`{"prompt":"draft an order update"}`),
		ModelSnapshot:  json.RawMessage(`{"provider":"openai","model":"gpt-mini"}`),
	}

	if err := runner.SaveWorkflowReleaseVersion(v1); err != nil {
		t.Fatalf("SaveWorkflowReleaseVersion v1: %v", err)
	}

	if err := runner.SaveToolGatewayBundle(ToolGatewayBundle{
		Tools: []ToolPolicy{{
			ID:       "notify-send",
			Tool:     "notify.send",
			Approved: true,
			AdaptiveRouting: &AdaptiveRoutingPolicy{
				Routes: []AdaptiveRoute{{ID: "mini", Provider: "openai", Model: "gpt-mini"}},
			},
		}},
	}); err != nil {
		t.Fatalf("SaveToolGatewayBundle: %v", err)
	}

	v2 := WorkflowReleaseVersion{
		ID:             "wf-orders-v2",
		WorkflowID:     "wf_orders",
		Spec:           json.RawMessage(`{"id":"wf_orders","nodes":[{"id":"draft","topic":"notify.send"},{"id":"review","human":{"mode":"approval","prompt":"review order update"}}]}`),
		PromptSnapshot: json.RawMessage(`{"prompt":"draft a polished order update"}`),
		ModelSnapshot:  json.RawMessage(`{"provider":"anthropic","model":"sonnet"}`),
	}

	if err := runner.SaveWorkflowReleaseVersion(v2); err != nil {
		t.Fatalf("SaveWorkflowReleaseVersion v2: %v", err)
	}

	versions, err := runner.ListWorkflowReleaseVersions("wf_orders")
	if err != nil {
		t.Fatalf("ListWorkflowReleaseVersions: %v", err)
	}

	if len(versions) != 2 {
		t.Fatalf("versions=%d want=2", len(versions))
	}

	diff, err := runner.DiffWorkflowReleaseVersions("wf_orders", "wf-orders-v1", "wf-orders-v2")
	if err != nil {
		t.Fatalf("DiffWorkflowReleaseVersions: %v", err)
	}

	if !diff.Spec.Changed || !diff.Prompt.Changed || !diff.Model.Changed {
		t.Fatalf("expected spec/prompt/model changes, got %+v", diff)
	}
}

func TestWorkflowReleaseCanaryFinalizeAndRollback(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())

	for _, version := range []WorkflowReleaseVersion{
		{
			ID:         "wf-support-v1",
			WorkflowID: "wf_support",
			Spec:       json.RawMessage(`{"id":"wf_support","nodes":[{"id":"draft","topic":"notify.send"}]}`),
		},
		{
			ID:         "wf-support-v2",
			WorkflowID: "wf_support",
			Spec:       json.RawMessage(`{"id":"wf_support","nodes":[{"id":"draft","topic":"notify.send"},{"id":"handoff","topic":"notify.send"}]}`),
		},
	} {
		if err := runner.SaveWorkflowReleaseVersion(version); err != nil {
			t.Fatalf("SaveWorkflowReleaseVersion(%s): %v", version.ID, err)
		}
	}

	channel := WorkflowReleaseChannel{
		WorkflowID:                   "wf_support",
		Environment:                  WorkflowEnvironmentProd,
		ActiveVersionID:              "wf-support-v1",
		CanaryVersionID:              "wf-support-v2",
		CanaryPercent:                100,
		AutoRollbackOnEvalRegression: true,
	}

	if err := runner.SaveWorkflowReleaseChannel(channel); err != nil {
		t.Fatalf("SaveWorkflowReleaseChannel: %v", err)
	}

	resolution, err := runner.ResolveWorkflowRelease("wf_support", WorkflowEnvironmentProd, "run-canary")
	if err != nil {
		t.Fatalf("ResolveWorkflowRelease: %v", err)
	}

	if resolution.PrimaryVersionID != "wf-support-v2" || !resolution.Canary {
		t.Fatalf("expected full canary to select v2, got %+v", resolution)
	}

	failedReport := EvalReport{
		ID:          "eval-wf-support-fail",
		SuiteID:     "suite-support",
		DatasetID:   "dataset-support",
		Status:      "completed",
		Evaluator:   EvalEvaluatorRunSucceeded,
		Passed:      false,
		CaseCount:   1,
		PassRate:    0,
		CreatedAt:   ptrTime(time.Now().UTC()),
		CompletedAt: ptrTime(time.Now().UTC()),
	}

	if err := runner.SaveEvalReport(failedReport); err != nil {
		t.Fatalf("SaveEvalReport fail: %v", err)
	}

	channel, action, err := runner.FinalizeWorkflowCanaryWithEvalGate(context.Background(), "eval-wf-support-fail", "wf_support", WorkflowEnvironmentProd)
	if err != nil {
		t.Fatalf("FinalizeWorkflowCanaryWithEvalGate fail: %v", err)
	}

	if action != "rolled_back" {
		t.Fatalf("action=%q want rolled_back", action)
	}

	if channel.ActiveVersionID != "wf-support-v1" || channel.CanaryVersionID != "" {
		t.Fatalf("unexpected rolled back channel: %+v", channel)
	}

	channel.CanaryVersionID = "wf-support-v2"
	channel.CanaryPercent = 100

	if err := runner.SaveWorkflowReleaseChannel(channel); err != nil {
		t.Fatalf("SaveWorkflowReleaseChannel second canary: %v", err)
	}

	passedReport := EvalReport{
		ID:          "eval-wf-support-pass",
		SuiteID:     "suite-support",
		DatasetID:   "dataset-support",
		Status:      "completed",
		Evaluator:   EvalEvaluatorRunSucceeded,
		Passed:      true,
		CaseCount:   1,
		PassRate:    1,
		CreatedAt:   ptrTime(time.Now().UTC()),
		CompletedAt: ptrTime(time.Now().UTC()),
	}

	if err := runner.SaveEvalReport(passedReport); err != nil {
		t.Fatalf("SaveEvalReport pass: %v", err)
	}

	channel, action, err = runner.FinalizeWorkflowCanaryWithEvalGate(context.Background(), "eval-wf-support-pass", "wf_support", WorkflowEnvironmentProd)
	if err != nil {
		t.Fatalf("FinalizeWorkflowCanaryWithEvalGate pass: %v", err)
	}

	if action != "promoted" {
		t.Fatalf("action=%q want promoted", action)
	}

	if channel.ActiveVersionID != "wf-support-v2" || channel.PreviousVersionID != "wf-support-v1" {
		t.Fatalf("unexpected promoted channel: %+v", channel)
	}

	channel, err = runner.RollbackWorkflowRelease(context.Background(), "wf_support", WorkflowEnvironmentProd, "")
	if err != nil {
		t.Fatalf("RollbackWorkflowRelease: %v", err)
	}

	if channel.ActiveVersionID != "wf-support-v1" {
		t.Fatalf("expected rollback to v1, got %+v", channel)
	}
}

func TestRunWorkflowRelease_ShadowRunsUseDryRun(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)

	var mu sync.Mutex
	seenModes := make([]SideEffectMode, 0, 2)
	done := make(chan struct{}, 2)

	reg.Register("notify.send", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		runtime, ok := SideEffectRuntimeFrom(ctx)

		if !ok {
			t.Fatal("expected side effect runtime")
		}

		mu.Lock()
		seenModes = append(seenModes, runtime.Mode)
		mu.Unlock()

		done <- struct{}{}
		return json.RawMessage(`{"ok":true}`), nil
	})

	if err := runner.SaveToolGatewayBundle(ToolGatewayBundle{
		Tools: []ToolPolicy{{
			ID:       "notify-send",
			Tool:     "notify.send",
			Approved: true,
			SideEffect: &SideEffectPolicy{
				Enabled:         true,
				DryRunSupported: true,
			},
		}},
	}); err != nil {
		t.Fatalf("SaveToolGatewayBundle: %v", err)
	}

	for _, version := range []WorkflowReleaseVersion{
		{
			ID:         "wf-mail-v1",
			WorkflowID: "wf_mail",
			Spec:       json.RawMessage(`{"id":"wf_mail","nodes":[{"id":"send","topic":"notify.send"}]}`),
		},
		{
			ID:         "wf-mail-v2",
			WorkflowID: "wf_mail",
			Spec:       json.RawMessage(`{"id":"wf_mail","nodes":[{"id":"send","topic":"notify.send"}]}`),
		},
	} {
		if err := runner.SaveWorkflowReleaseVersion(version); err != nil {
			t.Fatalf("SaveWorkflowReleaseVersion(%s): %v", version.ID, err)
		}
	}

	if err := runner.SaveWorkflowReleaseChannel(WorkflowReleaseChannel{
		WorkflowID:       "wf_mail",
		Environment:      WorkflowEnvironmentProd,
		ActiveVersionID:  "wf-mail-v1",
		ShadowVersionIDs: []string{"wf-mail-v2"},
	}); err != nil {
		t.Fatalf("SaveWorkflowReleaseChannel: %v", err)
	}

	if _, err := runner.RunWorkflowRelease(context.Background(), "run-mail-release", "wf_mail", WorkflowEnvironmentProd, json.RawMessage(`{"message":"hello"}`)); err != nil {
		t.Fatalf("RunWorkflowRelease: %v", err)
	}

	timeout := time.After(2 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-timeout:
			t.Fatal("timed out waiting for primary + shadow side-effect execution")
		}
	}

	mu.Lock()
	defer mu.Unlock()

	if len(seenModes) != 2 {
		t.Fatalf("seenModes=%v want 2 entries", seenModes)
	}

	var sawCommit, sawDryRun bool
	for _, mode := range seenModes {
		if mode == SideEffectModeCommit {
			sawCommit = true
		}

		if mode == SideEffectModeDryRun {
			sawDryRun = true
		}
	}

	if !sawCommit || !sawDryRun {
		t.Fatalf("expected commit and dry_run modes, got %v", seenModes)
	}
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

func TestWorkflowReleaseHTTPRoutes(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	mux := http.NewServeMux()
	AttachDebugRoutes(mux, runner)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	body1 := `{
		"id":"wf-http-v1",
		"workflow_id":"wf_http",
		"spec":{"id":"wf_http","nodes":[{"id":"draft","topic":"notify.send"}]},
		"prompt_snapshot":{"prompt":"draft http update"}
	}`
	mustPostJSONStatus(t, srv.URL+"/debug/workflows/releases/versions", body1, http.StatusOK)

	body2 := `{
		"id":"wf-http-v2",
		"workflow_id":"wf_http",
		"spec":{"id":"wf_http","nodes":[{"id":"draft","topic":"notify.send"},{"id":"review","human":{"mode":"approval","prompt":"review"}}]},
		"prompt_snapshot":{"prompt":"draft and review http update"}
	}`
	mustPostJSONStatus(t, srv.URL+"/debug/workflows/releases/versions", body2, http.StatusOK)

	channelBody := `{
		"workflow_id":"wf_http",
		"environment":"staging",
		"active_version_id":"wf-http-v1",
		"canary_version_id":"wf-http-v2",
		"canary_percent":50,
		"auto_rollback_on_eval_regression":true
	}`
	mustPostJSONStatus(t, srv.URL+"/debug/workflows/releases/channel", channelBody, http.StatusOK)

	resolveResp, err := http.Get(srv.URL + "/debug/workflows/releases/resolve?workflow_id=wf_http&environment=staging&run_id=run-http")

	if err != nil {
		t.Fatalf("GET resolve: %v", err)
	}

	defer resolveResp.Body.Close()
	if resolveResp.StatusCode != http.StatusOK {
		t.Fatalf("resolve status=%d", resolveResp.StatusCode)
	}

	diffResp, err := http.Get(srv.URL + "/debug/workflows/releases/diff?workflow_id=wf_http&from_version_id=wf-http-v1&to_version_id=wf-http-v2")
	if err != nil {
		t.Fatalf("GET diff: %v", err)
	}

	defer diffResp.Body.Close()
	if diffResp.StatusCode != http.StatusOK {
		t.Fatalf("diff status=%d", diffResp.StatusCode)
	}
}
