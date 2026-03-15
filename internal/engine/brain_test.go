package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestBrainRouting_PrefersHistoricalSuccessOverCheapDefault(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)

	var got ToolRuntimeContext
	reg.Register("llm.brain", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		got, _ = ToolRuntimeFrom(ctx)
		return json.RawMessage(`{"ok":true}`), nil
	})

	if err := runner.SaveToolGatewayBundle(ToolGatewayBundle{
		Tools: []ToolPolicy{{
			ID:       "llm-brain",
			Tool:     "llm.brain",
			Approved: true,
			AdaptiveRouting: &AdaptiveRoutingPolicy{
				CheapFirst: true,
				Routes: []AdaptiveRoute{
					{ID: "cheap", Provider: "openai", Model: "gpt-mini", EstimatedDollars: 0.01, Priority: 1},
					{ID: "strong", Provider: "anthropic", Model: "sonnet", EstimatedDollars: 0.08, Priority: 10},
				},
			},
		}},
	}); err != nil {
		t.Fatalf("SaveToolGatewayBundle: %v", err)
	}

	if err := runner.SaveBrainPolicy(BrainPolicy{
		Enabled:            true,
		LookbackRecords:    50,
		MinSamples:         2,
		DefaultSuccessRate: 0.75,
		DefaultLatencyMS:   1000,
		Weights: BrainWeights{
			SuccessRate:      0.55,
			WorkflowAffinity: 0.20,
			Latency:          0.05,
			Cost:             0.10,
			Priority:         0.05,
			Escalation:       0.05,
		},
	}); err != nil {
		t.Fatalf("SaveBrainPolicy: %v", err)
	}

	ctx := WithTenantID(context.Background(), "tenant-a")
	for i := 0; i < 4; i++ {
		runner.saveToolCallRecord(ctx, ToolCallRecord{
			At:         time.Now().UTC(),
			TenantID:   "tenant-a",
			Tool:       "llm.brain",
			WorkflowID: "wf_brain",
			RouteID:    "cheap",
			Provider:   "openai",
			Model:      "gpt-mini",
			Allowed:    i == 0,
			DurationMS: 80,
			Error: func() string {
				if i == 0 {
					return ""
				}
				return "provider failure"
			}(),
		})

		runner.saveToolCallRecord(ctx, ToolCallRecord{
			At:         time.Now().UTC(),
			TenantID:   "tenant-a",
			Tool:       "llm.brain",
			WorkflowID: "wf_brain",
			RouteID:    "strong",
			Provider:   "anthropic",
			Model:      "sonnet",
			Allowed:    true,
			DurationMS: 180,
		})
	}

	spec := []byte(`{"id":"wf_brain","nodes":[{"id":"gen","topic":"llm.brain"}]}`)
	if err := runner.RunSpecJSON(ctx, "run-brain-choice", spec, reg, json.RawMessage(`{"prompt":"pick best"}`)); err != nil {
		t.Fatalf("RunSpecJSON: %v", err)
	}

	if got.RouteID != "strong" || got.Provider != "anthropic" {
		t.Fatalf("expected brain to choose historically successful strong route, got %+v", got)
	}

	decision, err := runner.ExplainBrainRoute(ctx, BrainRouteRequest{
		RunID:      "run-brain-choice",
		WorkflowID: "wf_brain",
		NodeID:     "gen",
		Attempt:    1,
		Tool:       "llm.brain",
		Input:      json.RawMessage(`{"prompt":"pick best"}`),
	})

	if err != nil {
		t.Fatalf("ExplainBrainRoute: %v", err)
	}

	if decision.Selected.ID != "strong" {
		t.Fatalf("expected explanation to select strong, got %+v", decision)
	}

	if len(decision.Candidates) < 2 {
		t.Fatalf("expected scored candidates, got %+v", decision)
	}
}
