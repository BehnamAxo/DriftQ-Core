package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestAgentMemoryWriteListSearchAndSources(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	runner.SetArtifactStore(NewMemoryArtifactStore())

	ctx := WithTenantID(context.Background(), "tenant-a")

	note, err := runner.WriteAgentMemory(ctx, AgentMemoryWriteRequest{
		AgentID:    "planner",
		SourceKind: AgentMemorySourceNote,
		Content:    "customer is asking for invoice export and billing audit trail",
	})

	if err != nil {
		t.Fatalf("WriteAgentMemory note: %v", err)
	}

	if note.SourceKind != AgentMemorySourceNote {
		t.Fatalf("unexpected note source: %+v", note)
	}

	run := Run{
		RunID:      "run-memory-source",
		WorkflowID: "wf-memory",
		Status:     RunStatusSucceeded,
		TenantID:   "tenant-a",
		InitialInput: json.RawMessage(`{
			"goal":"investigate payment failures"
		}`),
		TerminalReason: "completed",
	}

	if err := runner.store.CreateRun(run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	if err := runner.store.UpsertNodeExecution(NodeExecution{
		RunID:      run.RunID,
		WorkflowID: run.WorkflowID,
		NodeID:     "analyze",
		Attempt:    1,
		Status:     NodeStatusSucceeded,
		Output:     json.RawMessage(`{"summary":"billing export broke after retry storm"}`),
	}); err != nil {
		t.Fatalf("UpsertNodeExecution: %v", err)
	}

	runMemory, err := runner.WriteAgentMemory(ctx, AgentMemoryWriteRequest{
		AgentID:    "planner",
		SourceKind: AgentMemorySourceRun,
		RunID:      run.RunID,
	})

	if err != nil {
		t.Fatalf("WriteAgentMemory run: %v", err)
	}

	if runMemory.RunID != run.RunID || runMemory.WorkflowID != run.WorkflowID {
		t.Fatalf("unexpected run memory: %+v", runMemory)
	}

	ref, meta, err := runner.PutArtifact(ctx, []byte("invoice export csv mapping for billing issues"), ArtifactMeta{
		TenantID:    "tenant-a",
		RunID:       run.RunID,
		WorkflowID:  run.WorkflowID,
		NodeID:      "artifact",
		ContentType: "text/plain",
	})

	if err != nil {
		t.Fatalf("PutArtifact: %v", err)
	}

	artifactMemory, err := runner.WriteAgentMemory(ctx, AgentMemoryWriteRequest{
		AgentID:    "planner",
		SourceKind: AgentMemorySourceArtifact,
		ArtifactID: ref.ArtifactID,
	})

	if err != nil {
		t.Fatalf("WriteAgentMemory artifact: %v", err)
	}

	if artifactMemory.ArtifactID != meta.ArtifactID {
		t.Fatalf("unexpected artifact memory: %+v", artifactMemory)
	}

	stateSnap, err := runner.WriteAgentState(ctx, AgentStateWriteRequest{
		AgentID: "planner",
		State:   json.RawMessage(`{"focus":"customer billing exports"}`),
	})

	if err != nil {
		t.Fatalf("WriteAgentState: %v", err)
	}

	stateMemory, err := runner.WriteAgentMemory(ctx, AgentMemoryWriteRequest{
		AgentID:      "planner",
		SourceKind:   AgentMemorySourceState,
		StateVersion: stateSnap.Version,
	})

	if err != nil {
		t.Fatalf("WriteAgentMemory state: %v", err)
	}

	if stateMemory.StateVersion != stateSnap.Version {
		t.Fatalf("unexpected state memory: %+v", stateMemory)
	}

	entries, err := runner.ListAgentMemory(ctx, "planner", 10)
	if err != nil {
		t.Fatalf("ListAgentMemory: %v", err)
	}

	if len(entries) != 4 {
		t.Fatalf("expected 4 memory entries, got %d", len(entries))
	}

	results, err := runner.SearchAgentMemory(ctx, AgentMemorySearchRequest{
		AgentID: "planner",
		Query:   "billing export issue",
		Limit:   3,
	})

	if err != nil {
		t.Fatalf("SearchAgentMemory: %v", err)
	}

	if len(results) == 0 {
		t.Fatal("expected semantic search results")
	}

	if top := results[0].Entry.Content; !bytes.Contains([]byte(top), []byte("billing")) {
		t.Fatalf("expected top result to be billing-related, got %q", top)
	}

	scopedCtx := WithAgentID(ctx, "planner")
	_, err = runner.ListAgentMemory(scopedCtx, "writer", 10)
	if !errors.Is(err, ErrAgentMemoryAccessDenied) {
		t.Fatalf("expected ErrAgentMemoryAccessDenied, got %v", err)
	}

	_, err = runner.SearchAgentMemory(context.Background(), AgentMemorySearchRequest{
		AgentID: "planner",
		Query:   "billing",
	})

	if !errors.Is(err, ErrTenantAccessDenied) {
		t.Fatalf("expected ErrTenantAccessDenied, got %v", err)
	}
}

func TestAgentMemoryReplaySafetyAndHandlerContext(t *testing.T) {
	runner := NewRunner(NewMemoryStore())
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)

	ctx := WithTenantID(context.Background(), "tenant-a")

	if _, err := runner.WriteAgentMemory(ctx, AgentMemoryWriteRequest{
		AgentID:    "planner",
		SourceKind: AgentMemorySourceNote,
		Content:    "draft the billing export response",
	}); err != nil {
		t.Fatalf("seed memory: %v", err)
	}

	var mu sync.Mutex
	replayEntered := make(chan struct{}, 1)
	releaseReplay := make(chan struct{})
	replaySeen := false

	reg.Register("search_memory", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		accessor := AgentMemoryFrom(ctx)
		if accessor == nil {
			t.Fatal("expected agent memory accessor in handler context")
		}

		if _, ok := ExecutionRefFrom(ctx); !ok {
			t.Fatal("expected execution ref in handler context")
		}

		if _, ok := ReplayScopeFrom(ctx); ok {
			mu.Lock()
			replaySeen = true
			mu.Unlock()
			replayEntered <- struct{}{}
			<-releaseReplay
		}

		results, err := accessor.SearchAgentMemory(ctx, AgentMemorySearchRequest{
			AgentID: "planner",
			Query:   "billing export",
			Limit:   5,
		})

		if err != nil {
			return nil, err
		}
		return json.Marshal(map[string]any{"count": len(results)})
	})

	graph := WorkflowGraph{
		ID: "wf_agent_memory_replay",
		Nodes: []NodeDef{{
			NodeID: "search",
			Topic:  "search_memory",
			Run: func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
				fn, _ := reg.Lookup("search_memory")
				return fn(ctx, input)
			},
		}},
	}

	if err := runner.RunDAG(ctx, "agent-memory-run", graph, json.RawMessage(`{}`)); err != nil {
		t.Fatalf("RunDAG initial: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Replay(ctx, "agent-memory-run", ReplayLive)
	}()

	<-replayEntered
	time.Sleep(10 * time.Millisecond)

	if _, err := runner.WriteAgentMemory(ctx, AgentMemoryWriteRequest{
		AgentID:    "planner",
		SourceKind: AgentMemorySourceNote,
		Content:    "new billing memory created after replay started",
	}); err != nil {
		t.Fatalf("write during replay from normal ctx: %v", err)
	}

	close(releaseReplay)

	if err := <-errCh; err != nil {
		t.Fatalf("Replay: %v", err)
	}

	nodes := runner.store.ListNodeExecutions("agent-memory-run")
	if len(nodes) != 2 {
		t.Fatalf("expected 2 node executions, got %d", len(nodes))
	}

	var output struct {
		Count int `json:"count"`
	}

	if err := json.Unmarshal(nodes[len(nodes)-1].Output, &output); err != nil {
		t.Fatalf("decode replay output: %v", err)
	}

	if output.Count != 1 {
		t.Fatalf("expected replay-safe search pinned to 1 result, got %d", output.Count)
	}

	replayCtx := WithReplayScope(ctx, "agent-memory-run", ReplayLive)
	_, err := runner.WriteAgentMemory(replayCtx, AgentMemoryWriteRequest{
		AgentID:    "planner",
		SourceKind: AgentMemorySourceNote,
		Content:    "blocked replay write",
	})

	if !errors.Is(err, ErrAgentMemoryReplayWriteDenied) {
		t.Fatalf("expected ErrAgentMemoryReplayWriteDenied, got %v", err)
	}

	mu.Lock()
	seenReplay := replaySeen
	mu.Unlock()

	if !seenReplay {
		t.Fatal("expected replay scope to be visible inside handler")
	}
}
