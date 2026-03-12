package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestEvalRun_NodeOutputExact_UsesSourceRunAndSpecOverride(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)
	reg := NewHandlerRegistry()

	reg.Register("emit_v1", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"answer":"v1"}`), nil
	})
	reg.Register("emit_v2", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"answer":"v2"}`), nil
	})
	runner.SetHandlerRegistry(reg)

	sourceSpec := []byte(`{
	  "id":"wf_eval_source",
	  "nodes":[
	    {"id":"A","topic":"emit_v1"}
	  ]
	}`)
	if err := runner.RunSpecJSON(context.Background(), "src-run", sourceSpec, reg, json.RawMessage(`{}`)); err != nil {
		t.Fatalf("RunSpecJSON source: %v", err)
	}

	if err := runner.SaveEvalDataset(EvalDataset{
		ID:   "dataset-1",
		Name: "regression",
		Cases: []EvalCase{
			{
				ID:             "case-1",
				SourceRunID:    "src-run",
				TargetNodeID:   "A",
				ExpectedOutput: json.RawMessage(`{"answer":"v1"}`),
			},
		},
	}); err != nil {
		t.Fatalf("SaveEvalDataset: %v", err)
	}

	if err := runner.SaveEvalSuite(EvalSuite{
		ID:            "suite-1",
		DatasetID:     "dataset-1",
		Evaluator:     EvalEvaluatorNodeOutputExact,
		TargetNodeID:  "A",
		PassThreshold: 1,
	}); err != nil {
		t.Fatalf("SaveEvalSuite: %v", err)
	}

	report, err := runner.RunEvalSuite(context.Background(), EvalRunRequest{
		EvalRunID: "eval-pass",
		SuiteID:   "suite-1",
	})
	if err != nil {
		t.Fatalf("RunEvalSuite pass: %v", err)
	}
	if !report.Passed || report.PassRate != 1 {
		t.Fatalf("expected passing report, got %+v", report)
	}
	if len(report.Results) != 1 || !report.Results[0].Passed {
		t.Fatalf("expected single passing result, got %+v", report.Results)
	}

	overrideSpec := json.RawMessage(`{
	  "id":"wf_eval_override",
	  "nodes":[
	    {"id":"A","topic":"emit_v2"}
	  ]
	}`)
	report2, err := runner.RunEvalSuite(context.Background(), EvalRunRequest{
		EvalRunID:    "eval-fail",
		SuiteID:      "suite-1",
		SpecOverride: overrideSpec,
	})
	if err != nil {
		t.Fatalf("RunEvalSuite override: %v", err)
	}
	if report2.Passed {
		t.Fatalf("expected failing report, got %+v", report2)
	}
	if len(report2.Results) != 1 {
		t.Fatalf("expected single result, got %+v", report2.Results)
	}
	if got := string(report2.Results[0].ActualOutput); got != `{"answer":"v2"}` {
		t.Fatalf("actual output=%s want v2 payload", got)
	}
}

func TestEvalCaseFromFailedRunAndPromotionGate(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)
	reg := NewHandlerRegistry()

	reg.Register("always_fail", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New("boom")
	})
	reg.Register("always_pass", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	})
	runner.SetHandlerRegistry(reg)

	failedSpec := []byte(`{
	  "id":"wf_fail",
	  "nodes":[
	    {"id":"A","topic":"always_fail"}
	  ]
	}`)
	err := runner.RunSpecJSON(context.Background(), "failed-run", failedSpec, reg, json.RawMessage(`{"x":1}`))
	if !errors.Is(err, ErrNodeFailed) {
		t.Fatalf("expected ErrNodeFailed, got %v", err)
	}

	if err := runner.SaveEvalDataset(EvalDataset{ID: "dataset-fail", Cases: []EvalCase{}}); err != nil {
		t.Fatalf("SaveEvalDataset: %v", err)
	}

	dataset, capturedCase, err := runner.CreateEvalCaseFromRun("dataset-fail", "", "", "failed-run", "")
	if err != nil {
		t.Fatalf("CreateEvalCaseFromRun: %v", err)
	}
	if len(dataset.Cases) != 1 {
		t.Fatalf("expected 1 case, got %+v", dataset.Cases)
	}
	if capturedCase.SourceRunID != "failed-run" {
		t.Fatalf("captured source_run_id=%q", capturedCase.SourceRunID)
	}
	if got := capturedCase.Labels["source_run_status"]; got != string(RunStatusFailed) {
		t.Fatalf("source_run_status=%q want %q", got, RunStatusFailed)
	}
	if string(capturedCase.Input) != `{"x":1}` {
		t.Fatalf("captured input=%s want original input", string(capturedCase.Input))
	}

	succeededRun := Run{
		RunID:      "candidate-run",
		WorkflowID: "wf_candidate",
		Status:     RunStatusSucceeded,
	}
	if err := store.CreateRun(succeededRun); err != nil {
		t.Fatalf("CreateRun candidate: %v", err)
	}

	if err := runner.SaveEvalReport(EvalReport{
		ID:            "eval-gate-pass",
		SuiteID:       "suite-pass",
		DatasetID:     "dataset-fail",
		Status:        "completed",
		Evaluator:     EvalEvaluatorRunSucceeded,
		PassThreshold: 1,
		CaseCount:     1,
		PassedCount:   1,
		PassRate:      1,
		Passed:        true,
	}); err != nil {
		t.Fatalf("SaveEvalReport pass: %v", err)
	}

	active, err := runner.PromoteRunWithEvalGate("eval-gate-pass", "candidate-run", "candidate-v1")
	if err != nil {
		t.Fatalf("PromoteRunWithEvalGate pass: %v", err)
	}
	if active != "candidate-v1" {
		t.Fatalf("active version=%q want candidate-v1", active)
	}

	if err := runner.SaveEvalReport(EvalReport{
		ID:            "eval-gate-fail",
		SuiteID:       "suite-fail",
		DatasetID:     "dataset-fail",
		Status:        "completed",
		Evaluator:     EvalEvaluatorRunSucceeded,
		PassThreshold: 1,
		CaseCount:     1,
		PassedCount:   0,
		PassRate:      0,
		Passed:        false,
	}); err != nil {
		t.Fatalf("SaveEvalReport fail: %v", err)
	}

	if _, err := runner.PromoteRunWithEvalGate("eval-gate-fail", "candidate-run", "candidate-v2"); err == nil {
		t.Fatal("expected failed eval gate to block promotion")
	}
}
