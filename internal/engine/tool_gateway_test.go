package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
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
