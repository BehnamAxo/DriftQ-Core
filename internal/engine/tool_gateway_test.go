package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestToolGateway_ApprovedToolSchemaRedactionAndAudit(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)

	var sawRuntime ToolRuntimeContext
	reg.Register("safe_tool", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		var ok bool
		sawRuntime, ok = ToolRuntimeFrom(ctx)

		if !ok {
			t.Fatal("expected tool runtime context")
		}

		return json.RawMessage(`{"status":"ok","token":"super-secret"}`), nil
	})

	if err := runner.SaveToolGatewayBundle(ToolGatewayBundle{
		Tools: []ToolPolicy{{
			ID:           "safe-tool",
			Tool:         "safe_tool",
			Approved:     true,
			ServerID:     "mcp-safe",
			RedactFields: []string{"api_key"},
		}},
		Servers: []ToolServerDefinition{{
			ID:             "mcp-safe",
			Kind:           "mcp",
			Endpoint:       "https://tools.example.test/safe",
			Approved:       true,
			AllowedTools:   []string{"safe_tool"},
			TenantScopes:   []string{"tenant-a"},
			SandboxAllowed: true,
		}},
	}); err != nil {
		t.Fatalf("SaveToolGatewayBundle: %v", err)
	}

	ctx := WithTenantID(context.Background(), "tenant-a")
	spec := []byte(`{
	  "id":"wf_tool_gateway",
	  "nodes":[
	    {
	      "id":"call_safe",
	      "topic":"safe_tool",
	      "input_schema":{
	        "type":"object",
	        "required":["name","api_key"],
	        "properties":{
	          "name":{"type":"string"},
	          "api_key":{"type":"string"}
	        },
	        "additionalProperties":false
	      },
	      "output_schema":{
	        "type":"object",
	        "required":["status"],
	        "properties":{
	          "status":{"type":"string"},
	          "token":{"type":"string"}
	        }
	      }
	    }
	  ]
	}`)

	if err := runner.RunSpecJSON(ctx, "run-tool-safe", spec, reg, json.RawMessage(`{"name":"alice","api_key":"secret-123"}`)); err != nil {
		t.Fatalf("RunSpecJSON: %v", err)
	}

	if sawRuntime.Tool != "safe_tool" || sawRuntime.ServerID != "mcp-safe" || sawRuntime.ServerKind != "mcp" {
		t.Fatalf("unexpected tool runtime: %+v", sawRuntime)
	}

	records, err := runner.ListToolCallRecords(ctx, "run-tool-safe", "safe_tool", 10)
	if err != nil {
		t.Fatalf("ListToolCallRecords: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 tool call record, got %d", len(records))
	}

	if !records[0].Allowed {
		t.Fatalf("expected allowed tool call record, got %+v", records[0])
	}

	var redactedInput map[string]any
	if err := json.Unmarshal(records[0].Input, &redactedInput); err != nil {
		t.Fatalf("decode redacted input: %v", err)
	}

	if redactedInput["api_key"] != "[REDACTED]" {
		t.Fatalf("expected api_key redacted, got %+v", redactedInput)
	}

	var redactedOutput map[string]any
	if err := json.Unmarshal(records[0].Output, &redactedOutput); err != nil {
		t.Fatalf("decode redacted output: %v", err)
	}

	if redactedOutput["token"] != "[REDACTED]" {
		t.Fatalf("expected token redacted, got %+v", redactedOutput)
	}

	audits, err := runner.ListAuditRecords("tenant-a", "run-tool-safe", 20)
	if err != nil {
		t.Fatalf("ListAuditRecords: %v", err)
	}

	found := false
	for _, rec := range audits {
		if rec.Action == "tool.call" && rec.ResourceID == "safe_tool" {
			found = true

			if rec.Outcome != AuditOutcomeAllowed {
				t.Fatalf("expected allowed tool audit, got %+v", rec)
			}
		}
	}

	if !found {
		t.Fatalf("expected tool.call audit record, got %+v", audits)
	}
}

func TestToolGateway_DeniedToolAndSchemaValidation(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)
	calledBlocked := false

	reg.Register("blocked_tool", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		calledBlocked = true
		return json.RawMessage(`{"status":"ok"}`), nil
	})

	calledSchema := false
	reg.Register("schema_tool", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		calledSchema = true
		return json.RawMessage(`{"status":"ok"}`), nil
	})

	if err := runner.SaveToolGatewayBundle(ToolGatewayBundle{
		Tools: []ToolPolicy{
			{ID: "blocked", Tool: "blocked_tool", Approved: false},
			{ID: "schema", Tool: "schema_tool", Approved: true},
		},
	}); err != nil {
		t.Fatalf("SaveToolGatewayBundle: %v", err)
	}

	ctx := WithTenantID(context.Background(), "tenant-a")
	blockedSpec := []byte(`{"id":"wf_blocked","nodes":[{"id":"call","topic":"blocked_tool"}]}`)
	err := runner.RunSpecJSON(ctx, "run-blocked-tool", blockedSpec, reg, json.RawMessage(`{}`))

	if !errors.Is(err, ErrNodeFailed) {
		t.Fatalf("expected ErrNodeFailed for blocked tool, got %v", err)
	}

	if calledBlocked {
		t.Fatal("blocked tool handler should not be called")
	}

	schemaSpec := []byte(`{
	  "id":"wf_schema",
	  "nodes":[
	    {
	      "id":"call",
	      "topic":"schema_tool",
	      "input_schema":{
	        "type":"object",
	        "required":["name"],
	        "properties":{"name":{"type":"string"}},
	        "additionalProperties":false
	      }
	    }
	  ]
	}`)

	err = runner.RunSpecJSON(ctx, "run-schema-tool", schemaSpec, reg, json.RawMessage(`{}`))
	if !errors.Is(err, ErrNodeFailed) {
		t.Fatalf("expected ErrNodeFailed for schema violation, got %v", err)
	}

	if calledSchema {
		t.Fatal("schema-invalid tool handler should not be called")
	}

	records, err := runner.ListToolCallRecords(ctx, "", "", 10)
	if err != nil {
		t.Fatalf("ListToolCallRecords: %v", err)
	}

	if len(records) < 2 {
		t.Fatalf("expected denied tool call records, got %+v", records)
	}

	if !strings.Contains(records[0].Reason, ErrToolSchemaInvalid.Error()) && !strings.Contains(records[1].Reason, ErrToolSchemaInvalid.Error()) {
		t.Fatalf("expected schema denial recorded, got %+v", records)
	}

	if !strings.Contains(records[0].Reason, ErrToolNotApproved.Error()) && !strings.Contains(records[1].Reason, ErrToolNotApproved.Error()) {
		t.Fatalf("expected approval denial recorded, got %+v", records)
	}
}

func TestToolGateway_SandboxContextVisibleToHandler(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)

	var sawRuntime ToolRuntimeContext
	reg.Register("db.query", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		var ok bool
		sawRuntime, ok = ToolRuntimeFrom(ctx)

		if !ok {
			t.Fatal("expected tool runtime context")
		}

		return json.RawMessage(`{"rows":1}`), nil
	})

	if err := runner.SaveToolGatewayBundle(ToolGatewayBundle{
		Tools: []ToolPolicy{{
			ID:              "db-query",
			Tool:            "db.query",
			Approved:        true,
			ServerID:        "mcp-db",
			SandboxRequired: true,
		}},
		Servers: []ToolServerDefinition{{
			ID:             "mcp-db",
			Kind:           "mcp",
			Approved:       true,
			AllowedTools:   []string{"db.query"},
			TenantScopes:   []string{"tenant-a"},
			SandboxAllowed: true,
		}},
	}); err != nil {
		t.Fatalf("SaveToolGatewayBundle: %v", err)
	}

	ctx := WithTenantID(context.Background(), "tenant-a")
	spec := []byte(`{"id":"wf_sandbox","nodes":[{"id":"call","topic":"db.query"}]}`)

	if err := runner.RunSpecJSON(ctx, "run-sandbox-tool", spec, reg, json.RawMessage(`{}`)); err != nil {
		t.Fatalf("RunSpecJSON sandbox: %v", err)
	}

	if !sawRuntime.Sandboxed || sawRuntime.ServerID != "mcp-db" {
		t.Fatalf("expected sandboxed MCP runtime, got %+v", sawRuntime)
	}
}

type staticRateLimiter struct {
	denyProvider string
}

func (s staticRateLimiter) Decide(ctx context.Context, req RateLimitRequest) (RateLimitDecision, error) {
	if strings.TrimSpace(req.Provider) == strings.TrimSpace(s.denyProvider) {
		return RateLimitDecision{Allowed: false, RetryAfter: 10 * time.Millisecond, Reason: "provider_denied"}, nil
	}
	return RateLimitDecision{Allowed: true}, nil
}

func TestAdaptiveRouting_CheapFirstAndEscalation(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)

	var calls []ToolRuntimeContext
	reg.Register("llm.generate", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		runtime, ok := ToolRuntimeFrom(ctx)
		if !ok {
			t.Fatal("expected tool runtime context")
		}
		calls = append(calls, runtime)
		return json.RawMessage(`{"ok":true}`), nil
	})

	if err := runner.SaveToolGatewayBundle(ToolGatewayBundle{
		Tools: []ToolPolicy{{
			ID:       "llm-generate",
			Tool:     "llm.generate",
			Approved: true,
			AdaptiveRouting: &AdaptiveRoutingPolicy{
				CheapFirst:            true,
				EscalateOnUncertainty: true,
				UncertaintyThreshold:  0.7,
				EscalateOnFailure:     true,
				FailureAttemptThreshold: 2,
				EscalateOnRisk:        true,
				RiskScoreThreshold:    40,
				Routes: []AdaptiveRoute{
					{ID: "cheap", Provider: "openai", Model: "gpt-mini", EstimatedDollars: 0.01, EstimatedTokens: 100, Priority: 1},
					{ID: "strong", Provider: "anthropic", Model: "sonnet", EstimatedDollars: 0.08, EstimatedTokens: 400, Priority: 10},
				},
			},
		}},
	}); err != nil {
		t.Fatalf("SaveToolGatewayBundle: %v", err)
	}

	ctx := WithTenantID(context.Background(), "tenant-a")
	spec := []byte(`{"id":"wf_adaptive","nodes":[{"id":"gen","topic":"llm.generate"}]}`)

	if err := runner.RunSpecJSON(ctx, "run-adaptive-cheap", spec, reg, json.RawMessage(`{"prompt":"hi"}`)); err != nil {
		t.Fatalf("RunSpecJSON cheap: %v", err)
	}
	if got := calls[len(calls)-1]; got.RouteID != "cheap" || got.Provider != "openai" || got.Model != "gpt-mini" {
		t.Fatalf("expected cheap route, got %+v", got)
	}

	if err := runner.RunSpecJSON(ctx, "run-adaptive-uncertain", spec, reg, json.RawMessage(`{"prompt":"hard","routing":{"uncertainty":0.95}}`)); err != nil {
		t.Fatalf("RunSpecJSON uncertain: %v", err)
	}
	if got := calls[len(calls)-1]; got.RouteID != "strong" || got.Provider != "anthropic" {
		t.Fatalf("expected escalated route on uncertainty, got %+v", got)
	}

	_ = runner.store.CreateRun(Run{
		RunID:      "run-adaptive-risk",
		WorkflowID: "wf_adaptive",
		Status:     RunStatusQueued,
		TenantID:   "tenant-a",
	})
	reportCtx := WithRiskDecision(ctx, RuntimeRiskDecision{Action: RiskActionSandbox, Score: 55})
	_, err := runner.invokeTool(reportCtx, toolInvocation{
		RunID:      "run-adaptive-risk",
		WorkflowID: "wf_adaptive",
		NodeID:     "gen",
		Attempt:    1,
		Tool:       "llm.generate",
		Handler:    reg.byTopic["llm.generate"],
	}, json.RawMessage(`{"prompt":"risky"}`))
	if err != nil {
		t.Fatalf("invokeTool risk: %v", err)
	}
	if got := calls[len(calls)-1]; got.RouteID != "strong" {
		t.Fatalf("expected escalated route on risk, got %+v", got)
	}

	records, err := runner.ListToolCallRecords(ctx, "", "llm.generate", 10)
	if err != nil {
		t.Fatalf("ListToolCallRecords: %v", err)
	}
	if len(records) < 3 {
		t.Fatalf("expected adaptive route records, got %+v", records)
	}
}

func TestAdaptiveRouting_BudgetAndRateLimitFallback(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)

	var got ToolRuntimeContext
	reg.Register("llm.answer", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		got, _ = ToolRuntimeFrom(ctx)
		return json.RawMessage(`{"ok":true}`), nil
	})

	if err := runner.SaveToolGatewayBundle(ToolGatewayBundle{
		Tools: []ToolPolicy{{
			ID:       "llm-answer",
			Tool:     "llm.answer",
			Approved: true,
			AdaptiveRouting: &AdaptiveRoutingPolicy{
				CheapFirst: true,
				Routes: []AdaptiveRoute{
					{ID: "cheap", Provider: "openai", Model: "gpt-mini", EstimatedDollars: 0.01, EstimatedTokens: 100},
					{ID: "mid", Provider: "google", Model: "flash", EstimatedDollars: 0.03, EstimatedTokens: 120},
					{ID: "expensive", Provider: "anthropic", Model: "opus", EstimatedDollars: 2.0, EstimatedTokens: 1000},
				},
			},
		}},
	}); err != nil {
		t.Fatalf("SaveToolGatewayBundle: %v", err)
	}

	ctx := WithTenantID(context.Background(), "tenant-a")
	ctx = WithRateLimiter(ctx, staticRateLimiter{denyProvider: "openai"})
	_ = runner.store.CreateRun(Run{
		RunID:      "run-budget-fallback",
		WorkflowID: "wf_budget_fallback",
		Status:     RunStatusQueued,
		TenantID:   "tenant-a",
		RunBudget:  BudgetPolicy{MaxDollars: 0.05, MaxTokens: 300},
	})

	_, err := runner.invokeTool(ctx, toolInvocation{
		RunID:      "run-budget-fallback",
		WorkflowID: "wf_budget_fallback",
		NodeID:     "answer",
		Attempt:    1,
		Tool:       "llm.answer",
		Handler:    reg.byTopic["llm.answer"],
	}, json.RawMessage(`{"prompt":"hello"}`))
	if err != nil {
		t.Fatalf("invokeTool fallback: %v", err)
	}

	if got.RouteID != "mid" || got.Provider != "google" {
		t.Fatalf("expected rate-limit fallback to mid route, got %+v", got)
	}
}
