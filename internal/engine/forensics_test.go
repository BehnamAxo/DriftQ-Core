package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type forensicFixture struct {
	runner      *Runner
	baseRunID   string
	failedRunID string
}

func newForensicFixture(t *testing.T) forensicFixture {
	t.Helper()

	runner := NewRunner(NewMemoryStore())
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)

	reg.Register("emit.v1", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"version":"v1"}`), nil
	})
	reg.Register("fail.step", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New("boom")
	})

	baseSpec := []byte(`{
	  "id":"wf_forensics",
	  "nodes":[
	    {"id":"A","topic":"emit.v1"}
	  ]
	}`)

	if err := runner.RunSpecJSON(context.Background(), "run-forensics-base", baseSpec, reg, json.RawMessage(`{"seed":"base"}`)); err != nil {
		t.Fatalf("RunSpecJSON base: %v", err)
	}

	failedSpec := []byte(`{
	  "id":"wf_forensics",
	  "nodes":[
	    {"id":"A","topic":"emit.v1"},
	    {"id":"B","topic":"fail.step","deps":["A"]}
	  ]
	}`)

	err := runner.RunSpecJSON(context.Background(), "run-forensics-failed", failedSpec, reg, json.RawMessage(`{"seed":"failed"}`))
	if err == nil {
		t.Fatal("expected failed forensic run")
	}

	return forensicFixture{
		runner:      runner,
		baseRunID:   "run-forensics-base",
		failedRunID: "run-forensics-failed",
	}
}

func containsForensicString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func TestForensics_BuildExecutionGraphAndRootCause(t *testing.T) {
	t.Parallel()

	fixture := newForensicFixture(t)
	graph, err := fixture.runner.BuildExecutionGraph(context.Background(), fixture.failedRunID)

	if err != nil {
		t.Fatalf("BuildExecutionGraph: %v", err)
	}

	if graph.Run.RunID != fixture.failedRunID {
		t.Fatalf("graph run_id=%q want %q", graph.Run.RunID, fixture.failedRunID)
	}

	if !graph.HasSpecGraph {
		t.Fatal("expected parsed spec graph")
	}

	if len(graph.Edges) != 1 || graph.Edges[0].From != "A" || graph.Edges[0].To != "B" {
		t.Fatalf("unexpected graph edges: %+v", graph.Edges)
	}

	nodesByID := map[string]ForensicExecutionNode{}
	for _, node := range graph.Nodes {
		nodesByID[node.NodeID] = node
	}

	if len(nodesByID) != 2 {
		t.Fatalf("expected 2 forensic nodes, got %+v", graph.Nodes)
	}

	if nodesByID["A"].LatestStatus != NodeStatusSucceeded {
		t.Fatalf("node A latest_status=%q want %q", nodesByID["A"].LatestStatus, NodeStatusSucceeded)
	}

	if nodesByID["B"].LatestStatus != NodeStatusFailed {
		t.Fatalf("node B latest_status=%q want %q", nodesByID["B"].LatestStatus, NodeStatusFailed)
	}

	if len(nodesByID["B"].Dependencies) != 1 || nodesByID["B"].Dependencies[0] != "A" {
		t.Fatalf("unexpected node B dependencies: %+v", nodesByID["B"].Dependencies)
	}

	if graph.SelfHealing == nil || graph.SelfHealing.RunID != fixture.failedRunID {
		t.Fatalf("expected self-healing artifact for failed run, got %+v", graph.SelfHealing)
	}

	rootCause, err := fixture.runner.BuildRootCauseView(context.Background(), fixture.failedRunID)
	if err != nil {
		t.Fatalf("BuildRootCauseView: %v", err)
	}

	if rootCause.PrimaryFailureNode != "B" {
		t.Fatalf("primary_failure_node=%q want %q", rootCause.PrimaryFailureNode, "B")
	}

	if len(rootCause.FailureNodes) == 0 || rootCause.FailureNodes[0].NodeID != "B" {
		t.Fatalf("unexpected failure nodes: %+v", rootCause.FailureNodes)
	}

	summary := strings.Join(rootCause.Summary, " | ")
	if !strings.Contains(summary, "primary failed node: B") {
		t.Fatalf("expected root-cause summary to mention failed node, got %q", summary)
	}
}

func TestForensics_DiffRunsAndWhatChanged(t *testing.T) {
	t.Parallel()

	fixture := newForensicFixture(t)

	diff, err := fixture.runner.DiffRuns(context.Background(), fixture.baseRunID, fixture.failedRunID)
	if err != nil {
		t.Fatalf("DiffRuns: %v", err)
	}

	if !diff.WorkflowSpec.Changed {
		t.Fatal("expected workflow spec diff")
	}

	if !diff.Status.Changed || diff.Status.From != string(RunStatusSucceeded) || diff.Status.To != string(RunStatusFailed) {
		t.Fatalf("unexpected status diff: %+v", diff.Status)
	}

	if !containsForensicString(diff.ChangedNodes, "B") {
		t.Fatalf("expected node B in changed nodes, got %+v", diff.ChangedNodes)
	}

	if !containsForensicString(diff.ChangedDomains, "workflow_spec") || !containsForensicString(diff.ChangedDomains, "node_executions") {
		t.Fatalf("unexpected changed domains: %+v", diff.ChangedDomains)
	}

	view, err := fixture.runner.BuildWhatChangedView(context.Background(), fixture.baseRunID, fixture.failedRunID)
	if err != nil {
		t.Fatalf("BuildWhatChangedView: %v", err)
	}

	if view.RootCause == nil {
		t.Fatal("expected root cause on what-changed view for failed run")
	}

	if view.RootCause.PrimaryFailureNode != "B" {
		t.Fatalf("what-changed root cause primary failure node=%q want %q", view.RootCause.PrimaryFailureNode, "B")
	}

	summary := strings.Join(view.Summary, " | ")
	if !strings.Contains(summary, "workflow spec changed between runs") {
		t.Fatalf("expected workflow diff summary, got %q", summary)
	}

	if !strings.Contains(summary, "changed nodes:") || !strings.Contains(summary, "B") {
		t.Fatalf("expected changed node summary, got %q", summary)
	}
}
