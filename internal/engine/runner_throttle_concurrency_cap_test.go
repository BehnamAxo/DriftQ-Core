package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestRunDAG_TopicConcurrencyCapBlocksAndEmitsThrottled(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)
	r.maxParallel = 2

	topic := "embed"

	// Ensure maps are initialized (depending on your NewRunner implementation)
	if r.topicCaps == nil {
		r.topicCaps = map[string]int{}
	}

	if r.inflightCaps == nil {
		r.inflightCaps = map[string]int{}
	}

	// Cap = 1 means only one node with this topic can be inflight at a time
	r.topicCaps[topic] = 1

	aStarted := make(chan struct{})
	allowA := make(chan struct{})
	bStarted := make(chan struct{})

	g := WorkflowGraph{
		ID: "wf_cap",
		Nodes: []NodeDef{
			{
				NodeID: "A",
				Topic:  topic,
				Run: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					close(aStarted)
					<-allowA // hold the cap
					return json.RawMessage(`"a"`), nil
				},
			},
			{
				NodeID: "B",
				Topic:  topic,
				Run: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					close(bStarted)
					return json.RawMessage(`"b"`), nil
				},
			},
		},
	}

	runID := "run_cap"

	done := make(chan error, 1)
	go func() {
		done <- r.runDAGWithCache(context.Background(), runID, g, json.RawMessage(`{}`), nil, nil)
	}()

	// A should start quickly
	select {
	case <-aStarted:
	case <-time.After(1 * time.Second):
		t.Fatal("A did not start")
	}

	// B must NOT start while A holds the cap
	select {
	case <-bStarted:
		t.Fatal("B started while A still held the concurrency cap")
	case <-time.After(200 * time.Millisecond):
		// goodio
	}

	// Let A finish so B can run.
	close(allowA)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run hung")
	}

	// We expect at least one Throttled event (B got blocked by cap)
	events := store.ListEvents(runID)
	found := false
	for _, ev := range events {
		if ev.Type == EventThrottled && ev.NodeID == "B" {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("expected at least one %v event for node B; got %d events", EventThrottled, len(events))
	}
}
