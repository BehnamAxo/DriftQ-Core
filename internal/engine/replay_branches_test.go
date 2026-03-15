package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestReplayBranch_CreateTimelineAndDiff(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)

	reg.Register("emit.v1", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"version":"v1"}`), nil
	})

	reg.Register("emit.v2", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"version":"v2"}`), nil
	})

	sourceSpec := []byte(`{
	  "id":"wf_branching",
	  "nodes":[
	    {"id":"A","topic":"emit.v1"},
	    {"id":"B","topic":"emit.v1","deps":["A"]}
	  ]
	}`)

	if err := runner.RunSpecJSON(context.Background(), "run-branch-source", sourceSpec, reg, json.RawMessage(`{"seed":"base"}`)); err != nil {
		t.Fatalf("RunSpecJSON source: %v", err)
	}

	branchSpec := json.RawMessage(`{
	  "id":"wf_branching",
	  "nodes":[
	    {"id":"A","topic":"emit.v1"},
	    {"id":"B","topic":"emit.v2","deps":["A"]}
	  ]
	}`)

	record, err := runner.CreateReplayBranch(context.Background(), ReplayBranchRequest{
		SourceRunID:  "run-branch-source",
		BranchName:   "alt-b",
		FromStep:     "B",
		Mode:         ReplayLive,
		SpecOverride: branchSpec,
	})

	if err != nil {
		t.Fatalf("CreateReplayBranch: %v", err)
	}

	if record.SourceRunID != "run-branch-source" {
		t.Fatalf("source_run_id=%q want %q", record.SourceRunID, "run-branch-source")
	}

	if record.RootRunID != "run-branch-source" {
		t.Fatalf("root_run_id=%q want %q", record.RootRunID, "run-branch-source")
	}

	if !record.SpecOverrideApplied {
		t.Fatal("expected spec override to be tracked")
	}

	if record.RunStatus != RunStatusSucceeded {
		t.Fatalf("branch run status=%q want %q", record.RunStatus, RunStatusSucceeded)
	}

	sourceNodes := runner.store.ListNodeExecutions("run-branch-source")
	if len(sourceNodes) != 2 {
		t.Fatalf("expected source node history untouched, got %+v", sourceNodes)
	}

	timeline, err := runner.BuildReplayTimeline(context.Background(), record.BranchRunID)
	if err != nil {
		t.Fatalf("BuildReplayTimeline: %v", err)
	}

	if timeline.RootRunID != "run-branch-source" {
		t.Fatalf("timeline root=%q want %q", timeline.RootRunID, "run-branch-source")
	}

	if len(timeline.Branches) != 1 || timeline.Branches[0].BranchRunID != record.BranchRunID {
		t.Fatalf("unexpected timeline branches: %+v", timeline.Branches)
	}

	view, err := runner.BuildWhatChangedView(context.Background(), "run-branch-source", record.BranchRunID)
	if err != nil {
		t.Fatalf("BuildWhatChangedView: %v", err)
	}

	summary := strings.Join(view.Summary, " | ")
	if !strings.Contains(summary, "workflow spec changed between runs") {
		t.Fatalf("expected workflow change in summary, got %q", summary)
	}

	if !strings.Contains(summary, "B") {
		t.Fatalf("expected node B change in summary, got %q", summary)
	}
}
