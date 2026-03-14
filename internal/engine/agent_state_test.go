package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestAgentStateWriteReadLineageAndScope(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	ctx := WithTenantID(context.Background(), "tenant-a")

	snap1, err := runner.WriteAgentState(ctx, AgentStateWriteRequest{
		AgentID: "planner",
		State:   json.RawMessage(`{"stage":"draft"}`),
	})
	if err != nil {
		t.Fatalf("WriteAgentState v1: %v", err)
	}
	if snap1.Version != 1 || snap1.ParentVersion != 0 {
		t.Fatalf("unexpected v1 snapshot: %+v", snap1)
	}

	snap2, err := runner.WriteAgentState(ctx, AgentStateWriteRequest{
		AgentID:         "planner",
		State:           json.RawMessage(`{"stage":"review"}`),
		ExpectedVersion: 1,
	})
	if err != nil {
		t.Fatalf("WriteAgentState v2: %v", err)
	}
	if snap2.Version != 2 || snap2.ParentVersion != 1 {
		t.Fatalf("unexpected v2 snapshot: %+v", snap2)
	}

	latest, err := runner.ReadAgentState(ctx, "planner", AgentStateReadOptions{})
	if err != nil {
		t.Fatalf("ReadAgentState latest: %v", err)
	}
	if latest.Version != 2 || string(latest.State) != `{"stage":"review"}` {
		t.Fatalf("unexpected latest snapshot: %+v", latest)
	}

	first, err := runner.ReadAgentState(ctx, "planner", AgentStateReadOptions{Version: 1})
	if err != nil {
		t.Fatalf("ReadAgentState version 1: %v", err)
	}
	if first.Version != 1 || string(first.State) != `{"stage":"draft"}` {
		t.Fatalf("unexpected v1 snapshot: %+v", first)
	}

	lineage, err := runner.ListAgentStateLineage(ctx, "planner", 10)
	if err != nil {
		t.Fatalf("ListAgentStateLineage: %v", err)
	}
	if len(lineage) != 2 || lineage[0].Version != 2 || lineage[1].Version != 1 {
		t.Fatalf("unexpected lineage: %+v", lineage)
	}

	_, err = runner.ReadAgentState(context.Background(), "planner", AgentStateReadOptions{})
	if !errors.Is(err, ErrTenantAccessDenied) {
		t.Fatalf("expected ErrTenantAccessDenied without tenant, got %v", err)
	}

	scopedCtx := WithAgentID(ctx, "planner")
	_, err = runner.ReadAgentState(scopedCtx, "writer", AgentStateReadOptions{})
	if !errors.Is(err, ErrAgentStateAccessDenied) {
		t.Fatalf("expected ErrAgentStateAccessDenied for mismatched agent scope, got %v", err)
	}
}

func TestAgentStateReplaySafetyAndHandlerContext(t *testing.T) {
	runner := NewRunner(NewMemoryStore())
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)

	ctx := WithTenantID(context.Background(), "tenant-a")
	if _, err := runner.WriteAgentState(ctx, AgentStateWriteRequest{
		AgentID: "planner",
		State:   json.RawMessage(`{"version":"v1"}`),
	}); err != nil {
		t.Fatalf("seed state v1: %v", err)
	}

	var mu sync.Mutex
	replayEntered := make(chan struct{}, 1)
	releaseReplay := make(chan struct{})
	replaySeen := false

	reg.Register("read_agent_state", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		accessor := AgentStateFrom(ctx)
		if accessor == nil {
			t.Fatal("expected agent state accessor in handler context")
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

		snapshot, err := accessor.ReadAgentState(ctx, "planner", AgentStateReadOptions{})
		if err != nil {
			return nil, err
		}
		return json.Marshal(map[string]any{"version": snapshot.Version})
	})

	graph := WorkflowGraph{
		ID: "wf_agent_state_replay",
		Nodes: []NodeDef{{
			NodeID: "read",
			Topic:  "read_agent_state",
			Run: func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
				fn, _ := reg.Lookup("read_agent_state")
				return fn(ctx, input)
			},
		}},
	}

	if err := runner.RunDAG(ctx, "agent-state-run", graph, json.RawMessage(`{}`)); err != nil {
		t.Fatalf("RunDAG initial: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Replay(ctx, "agent-state-run", ReplayLive)
	}()

	<-replayEntered
	time.Sleep(10 * time.Millisecond)

	if _, err := runner.WriteAgentState(ctx, AgentStateWriteRequest{
		AgentID:         "planner",
		State:           json.RawMessage(`{"version":"v2"}`),
		ExpectedVersion: 1,
	}); err != nil {
		t.Fatalf("write v2 during replay: %v", err)
	}

	close(releaseReplay)

	if err := <-errCh; err != nil {
		t.Fatalf("Replay: %v", err)
	}

	nodes := runner.store.ListNodeExecutions("agent-state-run")
	if len(nodes) != 2 {
		t.Fatalf("expected 2 node executions, got %d", len(nodes))
	}

	var output struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(nodes[len(nodes)-1].Output, &output); err != nil {
		t.Fatalf("decode replay output: %v", err)
	}
	if output.Version != 1 {
		t.Fatalf("expected replay-safe read pinned to version 1, got %d", output.Version)
	}

	mu.Lock()
	seenReplay := replaySeen
	mu.Unlock()
	if !seenReplay {
		t.Fatal("expected replay scope to be visible inside handler")
	}

	replayCtx := WithReplayScope(ctx, "agent-state-run", ReplayLive)
	_, err := runner.WriteAgentState(replayCtx, AgentStateWriteRequest{
		AgentID: "planner",
		State:   json.RawMessage(`{"version":"v3"}`),
	})
	if !errors.Is(err, ErrAgentStateReplayWriteDenied) {
		t.Fatalf("expected ErrAgentStateReplayWriteDenied, got %v", err)
	}
}
