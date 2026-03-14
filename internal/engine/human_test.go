package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestHumanStep_EditApproveAndResume(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)

	reg.Register("echo", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return cloneRaw(input), nil
	})

	spec := []byte(`{
	  "id":"wf_human_edit",
	  "nodes":[
	    {"id":"review","human":{"mode":"edit","prompt":"review and edit"}},
	    {"id":"echo","topic":"echo","deps":["review"]}
	  ]
	}`)

	if err := runner.RunSpecJSON(context.Background(), "run-human-edit", spec, reg, json.RawMessage(`{"message":"draft"}`)); err != nil {
		t.Fatalf("RunSpecJSON human edit: %v", err)
	}

	run, ok := store.GetRun("run-human-edit")
	if !ok || run.Status != RunStatusWaiting {
		t.Fatalf("expected waiting run, got ok=%v run=%+v", ok, run)
	}

	tasks, err := runner.ListHumanTasks("run-human-edit", HumanTaskPending, 10)
	if err != nil {
		t.Fatalf("ListHumanTasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].NodeID != "review" {
		t.Fatalf("expected one pending review task, got %+v", tasks)
	}

	_, err = runner.ResolveHumanTask(context.Background(), tasks[0].ID, HumanDecisionApprove, json.RawMessage(`{"message":"approved"}`), "looks good", true)
	if err != nil {
		t.Fatalf("ResolveHumanTask approve: %v", err)
	}

	run, ok = store.GetRun("run-human-edit")
	if !ok || run.Status != RunStatusSucceeded {
		t.Fatalf("expected succeeded run after approval, got ok=%v run=%+v", ok, run)
	}

	var reviewNode NodeExecution
	foundReview := false
	for _, node := range store.ListNodeExecutions("run-human-edit") {
		if node.NodeID == "review" && node.Status == NodeStatusSucceeded {
			reviewNode = node
			foundReview = true
			break
		}
	}
	if !foundReview {
		t.Fatalf("expected succeeded review node, got %+v", store.ListNodeExecutions("run-human-edit"))
	}
	if string(reviewNode.Output) != `{"message":"approved"}` {
		t.Fatalf("unexpected human output: %s", string(reviewNode.Output))
	}
}

func TestHumanStep_TimeoutRejectsRun(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)

	spec := []byte(`{
	  "id":"wf_human_timeout",
	  "nodes":[
	    {"id":"review","human":{"mode":"approval","prompt":"approve","timeout_ms":1,"on_timeout":"reject"}}
	  ]
	}`)

	if err := runner.RunSpecJSON(context.Background(), "run-human-timeout", spec, reg, json.RawMessage(`{}`)); err != nil {
		t.Fatalf("RunSpecJSON human timeout: %v", err)
	}

	fired, resumed, err := runner.FireDueTimersAndResume(context.Background(), time.Now().UTC().Add(10*time.Millisecond))
	if err != nil {
		t.Fatalf("FireDueTimersAndResume: %v", err)
	}
	if fired == 0 {
		t.Fatalf("expected timer fire, got fired=%d resumed=%d", fired, resumed)
	}

	run, ok := store.GetRun("run-human-timeout")
	if !ok || run.Status != RunStatusFailed {
		t.Fatalf("expected failed run after timeout, got ok=%v run=%+v", ok, run)
	}

	tasks, err := runner.ListHumanTasks("run-human-timeout", "", 10)
	if err != nil {
		t.Fatalf("ListHumanTasks timeout: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Status != HumanTaskTimedOut {
		t.Fatalf("expected timed out human task, got %+v", tasks)
	}
}

func TestRiskEscalation_CreatesHumanApprovalAndResumes(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)
	reg := NewHandlerRegistry()

	reg.Register("db.query", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"rows":[]}`), nil
	})
	reg.Register("http.upload", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"done":true}`), nil
	})
	runner.SetHandlerRegistry(reg)

	spec := []byte(`{
	  "id":"wf_risk_human",
	  "nodes":[
	    {"id":"A","topic":"db.query"},
	    {"id":"B","topic":"http.upload","deps":["A"]}
	  ]
	}`)

	err := runner.RunSpecJSON(context.Background(), "run-risk-human", spec, reg, json.RawMessage(`{}`))
	var pendingErr *HumanApprovalPendingError
	if !errors.As(err, &pendingErr) {
		t.Fatalf("expected HumanApprovalPendingError, got %v", err)
	}

	run, ok := store.GetRun("run-risk-human")
	if !ok || run.Status != RunStatusWaiting {
		t.Fatalf("expected waiting risk run, got ok=%v run=%+v", ok, run)
	}

	tasks, err := runner.ListHumanTasks("run-risk-human", HumanTaskPending, 10)
	if err != nil {
		t.Fatalf("ListHumanTasks risk: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Source != HumanTaskSourceRisk {
		t.Fatalf("expected one pending risk task, got %+v", tasks)
	}

	_, err = runner.ResolveHumanTask(context.Background(), tasks[0].ID, HumanDecisionApprove, nil, "approved risk", true)
	if err != nil {
		t.Fatalf("ResolveHumanTask risk approve: %v", err)
	}

	run, ok = store.GetRun("run-risk-human")
	if !ok || run.Status != RunStatusSucceeded {
		t.Fatalf("expected succeeded run after risk approval, got ok=%v run=%+v", ok, run)
	}
}
