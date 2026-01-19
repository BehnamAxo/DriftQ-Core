package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestRunDAG_TenantTopicConcurrencyCapOverridesTopicCap(t *testing.T) {
	store := NewMemoryStore()
	r := NewRunner(store)
	r.maxParallel = 4

	runID := "run_tenant_cap"
	wfID := "wf_tenant_cap"

	tenantID := "t1"
	topic := "llm"

	// Global topic cap would allow 2 in parallel...
	r.SetTopicConcurrencyCap(topic, 2)
	// ...but tenant override should force 1
	r.SetTenantTopicConcurrencyCap(tenantID, topic, 1)

	// Two independent nodes that would run in parallel if cap=2
	g := WorkflowGraph{
		ID: wfID,
		Nodes: []NodeDef{
			{
				NodeID: "A",
				Topic:  topic,
				Run: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					time.Sleep(100 * time.Millisecond)
					return json.RawMessage(`"okA"`), nil
				},
			},
			{
				NodeID: "B",
				Topic:  topic,
				Run: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					time.Sleep(100 * time.Millisecond)
					return json.RawMessage(`"okB"`), nil
				},
			},
		},
		Edges: nil,
	}

	// Run with tenant in ctx so cap selection uses tenant override
	ctx := WithTenantID(context.Background(), tenantID)

	start := time.Now()
	err := r.RunDAG(ctx, runID, g, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("RunDAG: %v", err)
	}
	elapsed := time.Since(start)

	// If they ran truly in parallel, we'd be near ~100ms
	// With tenant cap=1, should be closer to ~200ms
	if elapsed < 150*time.Millisecond {
		t.Fatalf("expected serialization due to tenant cap; elapsed=%v", elapsed)
	}

	evs := store.ListEvents(runID)
	found := false
	for _, e := range evs {
		if e.Type != EventThrottled {
			continue
		}

		if string(e.Payload) == "" {
			continue
		}
		if containsAll(string(e.Payload), []string{`"reason":"concurrency_cap"`, `"scope":"tenant_topic"`, `"topic":"` + topic + `"`}) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected EventThrottled with tenant_topic scope; events=%d", len(evs))
	}
}

func containsAll(s string, subs []string) bool {
	for _, sub := range subs {
		if !contains(s, sub) {
			return false
		}
	}

	return true
}
