package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestEvaluateAuthorization_RulesAndBuiltins(t *testing.T) {
	bundle := AuthorizationPolicyBundle{
		Rules: []AuthorizationRule{
			{
				ID:      "allow-builder-run",
				Effect:  PolicyEffectAllow,
				Actions: []PolicyAction{PolicyActionRunStart},
				Roles:   []string{"builder"},
			},
			{
				ID:      "allow-builder-tools",
				Effect:  PolicyEffectAllow,
				Actions: []PolicyAction{PolicyActionToolExecute},
				Roles:   []string{"builder"},
				Tools:   []string{"tool.safe.*"},
			},
			{
				ID:      "deny-secret-tool",
				Effect:  PolicyEffectDeny,
				Actions: []PolicyAction{PolicyActionToolExecute},
				Tools:   []string{"tool.safe.secret"},
			},
		},
	}

	if err := bundle.NormalizeAndValidate(); err != nil {
		t.Fatalf("NormalizeAndValidate: %v", err)
	}

	principal := Principal{
		ID:           "agent-1",
		TenantScopes: []string{"tenant-a"},
		Roles:        []string{"builder"},
		Capabilities: []string{"coding"},
	}

	runCheck := evaluateAuthorization(bundle, AuthorizationRequest{
		Action:     PolicyActionRunStart,
		TenantID:   "tenant-a",
		WorkflowID: "wf-secure",
		Principal:  principal,
	})

	if !runCheck.Allowed {
		t.Fatalf("run check denied: %+v", runCheck)
	}

	toolCheck := evaluateAuthorization(bundle, AuthorizationRequest{
		Action:    PolicyActionToolExecute,
		TenantID:  "tenant-a",
		Tool:      "tool.safe.echo",
		Principal: principal,
	})

	if !toolCheck.Allowed {
		t.Fatalf("tool check denied: %+v", toolCheck)
	}

	denyCheck := evaluateAuthorization(bundle, AuthorizationRequest{
		Action:    PolicyActionToolExecute,
		TenantID:  "tenant-a",
		Tool:      "tool.safe.secret",
		Principal: principal,
	})

	if denyCheck.Allowed {
		t.Fatalf("expected deny override, got %+v", denyCheck)
	}

	if len(denyCheck.MatchedDenyRules) != 1 || denyCheck.MatchedDenyRules[0] != "deny-secret-tool" {
		t.Fatalf("unexpected deny rules: %+v", denyCheck)
	}

	tenantCheck := evaluateAuthorization(bundle, AuthorizationRequest{
		Action:    PolicyActionRunStart,
		TenantID:  "tenant-b",
		Principal: principal,
	})

	if tenantCheck.Allowed {
		t.Fatalf("expected tenant mismatch deny, got %+v", tenantCheck)
	}

	capCheck := evaluateAuthorization(bundle, AuthorizationRequest{
		Action:             PolicyActionToolExecute,
		TenantID:           "tenant-a",
		Tool:               "tool.safe.echo",
		RequiredCapability: "review",
		Principal:          principal,
	})

	if capCheck.Allowed {
		t.Fatalf("expected missing capability deny, got %+v", capCheck)
	}
}

func TestRunnerAuthorization_BlocksUnauthorizedRun(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)
	reg := NewHandlerRegistry()

	reg.Register("safe_tool", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	})

	if err := runner.SaveAuthorizationPolicy(AuthorizationPolicyBundle{
		Rules: []AuthorizationRule{
			{
				ID:      "allow-builder-run",
				Effect:  PolicyEffectAllow,
				Actions: []PolicyAction{PolicyActionRunStart},
				Roles:   []string{"builder"},
			},
			{
				ID:      "allow-builder-tools",
				Effect:  PolicyEffectAllow,
				Actions: []PolicyAction{PolicyActionToolExecute},
				Roles:   []string{"builder"},
				Tools:   []string{"safe_tool"},
			},
		},
	}); err != nil {
		t.Fatalf("SaveAuthorizationPolicy: %v", err)
	}

	spec := []byte(`{
	  "id":"wf_authz",
	  "nodes":[
	    {"id":"A","topic":"safe_tool"}
	  ]
	}`)

	ctx := WithPrincipal(context.Background(), Principal{
		ID:    "agent-viewer",
		Roles: []string{"viewer"},
	})

	err := runner.RunSpecJSON(ctx, "run-denied", spec, reg, json.RawMessage(`{}`))
	if !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("expected ErrAuthorizationDenied, got %v", err)
	}

	if _, ok := store.GetRun("run-denied"); ok {
		t.Fatal("run should not be created when preflight authorization fails")
	}
}

func TestRunnerAuthorization_RequiredCapability(t *testing.T) {
	store := NewMemoryStore()
	runner := NewRunner(store)
	reg := NewHandlerRegistry()

	reg.Register("safe_tool", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	})

	if err := runner.SaveAuthorizationPolicy(AuthorizationPolicyBundle{
		Rules: []AuthorizationRule{
			{
				ID:      "allow-builder-run",
				Effect:  PolicyEffectAllow,
				Actions: []PolicyAction{PolicyActionRunStart},
				Roles:   []string{"builder"},
			},
			{
				ID:      "allow-builder-tools",
				Effect:  PolicyEffectAllow,
				Actions: []PolicyAction{PolicyActionToolExecute},
				Roles:   []string{"builder"},
				Tools:   []string{"safe_tool"},
			},
		},
	}); err != nil {
		t.Fatalf("SaveAuthorizationPolicy: %v", err)
	}

	spec := []byte(`{
	  "id":"wf_capability",
	  "nodes":[
	    {"id":"A","topic":"safe_tool","capability":"coding"}
	  ]
	}`)

	ctxNoCapability := WithPrincipal(context.Background(), Principal{
		ID:    "agent-builder",
		Roles: []string{"builder"},
	})

	err := runner.RunSpecJSON(ctxNoCapability, "run-missing-capability", spec, reg, json.RawMessage(`{}`))
	if !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("expected ErrAuthorizationDenied for missing capability, got %v", err)
	}

	ctxWithCapability := WithPrincipal(context.Background(), Principal{
		ID:           "agent-builder",
		Roles:        []string{"builder"},
		Capabilities: []string{"coding"},
	})

	if err := runner.RunSpecJSON(ctxWithCapability, "run-with-capability", spec, reg, json.RawMessage(`{}`)); err != nil {
		t.Fatalf("RunSpecJSON with capability: %v", err)
	}
}
