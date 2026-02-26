package multiagent

import (
	"context"
	"errors"
	"testing"

	"github.com/driftq-org/DriftQ-Core/internal/broker"
)

func TestCapabilityRegistry_RegisterResolveRoundRobin(t *testing.T) {
	reg := NewCapabilityRegistry()
	if err := reg.Register("coding", "coder-a", "coder-b", "coder-a"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	snap := reg.Snapshot()
	gotAgents := snap["coding"]
	if len(gotAgents) != 2 || gotAgents[0] != "coder-a" || gotAgents[1] != "coder-b" {
		t.Fatalf("unexpected snapshot: %#v", snap)
	}

	got1, err := reg.ResolveAgent("coding")
	if err != nil {
		t.Fatalf("ResolveAgent #1: %v", err)
	}
	got2, err := reg.ResolveAgent("coding")
	if err != nil {
		t.Fatalf("ResolveAgent #2: %v", err)
	}
	got3, err := reg.ResolveAgent("coding")
	if err != nil {
		t.Fatalf("ResolveAgent #3: %v", err)
	}

	if got1 != "coder-a" || got2 != "coder-b" || got3 != "coder-a" {
		t.Fatalf("unexpected rr sequence: %q, %q, %q", got1, got2, got3)
	}
}

func TestCapabilityRegistry_Errors(t *testing.T) {
	var nilReg *CapabilityRegistry
	if _, err := nilReg.ResolveAgent("coding"); !errors.Is(err, ErrNilRegistry) {
		t.Fatalf("expected ErrNilRegistry, got %v", err)
	}

	reg := NewCapabilityRegistry()
	if _, err := reg.ResolveAgent("missing"); !errors.Is(err, ErrCapabilityNotFound) {
		t.Fatalf("expected ErrCapabilityNotFound, got %v", err)
	}

	if err := reg.Replace("coding", nil); err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if _, err := reg.ResolveAgent("coding"); !errors.Is(err, ErrCapabilityHasNoAgents) {
		t.Fatalf("expected ErrCapabilityHasNoAgents, got %v", err)
	}
}

func TestMultiAgentRouter_Direct(t *testing.T) {
	r := NewRouter(RouterConfig{})
	msg := broker.Message{Value: []byte(`{"sender":"planner","receiver":"coder","intent":"implement","payload":{"repo":"driftq"}}`)}

	dec, err := r.Route(context.Background(), "work.in", msg)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if dec.TargetTopic != "agent.coder.inbox" {
		t.Fatalf("unexpected target topic: %s", dec.TargetTopic)
	}
	if dec.Label != "multiagent.direct" {
		t.Fatalf("unexpected label: %s", dec.Label)
	}
	if dec.Meta["receiver"] != "coder" {
		t.Fatalf("expected receiver metadata, got %#v", dec.Meta)
	}
}

func TestMultiAgentRouter_Broadcast(t *testing.T) {
	r := NewRouter(RouterConfig{})
	msg := broker.Message{Value: []byte(`{"sender":"planner","team":"core","intent":"announce","payload":{"msg":"deploy"}}`)}

	dec, err := r.Route(context.Background(), "work.in", msg)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if dec.TargetTopic != "team.core.broadcast" {
		t.Fatalf("unexpected target topic: %s", dec.TargetTopic)
	}
	if dec.Label != "multiagent.broadcast" {
		t.Fatalf("unexpected label: %s", dec.Label)
	}
	if dec.Meta["team"] != "core" {
		t.Fatalf("expected team metadata, got %#v", dec.Meta)
	}
}

func TestMultiAgentRouter_CapabilityRoundRobin(t *testing.T) {
	reg := NewCapabilityRegistry()
	if err := reg.Register("coding", "coder-a", "coder-b"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	r := NewRouterWithRegistry(reg)
	msg := broker.Message{Value: []byte(`{"sender":"planner","capability":"coding","intent":"implement","payload":{"ticket":"v3.1"}}`)}

	dec1, err := r.Route(context.Background(), "work.in", msg)
	if err != nil {
		t.Fatalf("Route #1: %v", err)
	}
	dec2, err := r.Route(context.Background(), "work.in", msg)
	if err != nil {
		t.Fatalf("Route #2: %v", err)
	}

	if dec1.TargetTopic != "agent.coder-a.inbox" || dec2.TargetTopic != "agent.coder-b.inbox" {
		t.Fatalf("unexpected capability route targets: %q, %q", dec1.TargetTopic, dec2.TargetTopic)
	}
	if dec1.Meta["capability"] != "coding" || dec1.Meta["selected_agent"] != "coder-a" {
		t.Fatalf("unexpected metadata #1: %#v", dec1.Meta)
	}
	if dec2.Meta["selected_agent"] != "coder-b" {
		t.Fatalf("unexpected metadata #2: %#v", dec2.Meta)
	}
}

func TestMultiAgentRouter_RoleFallsBackToCapabilityRegistry(t *testing.T) {
	reg := NewCapabilityRegistry()
	if err := reg.Register("reviewer", "reviewer-1"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	r := NewRouterWithRegistry(reg)
	msg := broker.Message{Value: []byte(`{"sender":"planner","role":"reviewer","intent":"review","payload":{"pr":12}}`)}

	dec, err := r.Route(context.Background(), "work.in", msg)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if dec.TargetTopic != "agent.reviewer-1.inbox" {
		t.Fatalf("unexpected target topic: %s", dec.TargetTopic)
	}
	if dec.Meta["capability"] != "reviewer" {
		t.Fatalf("expected role fallback in metadata, got %#v", dec.Meta)
	}
}

func TestMultiAgentRouter_NonAgentPayloadIgnoredWhenNotStrict(t *testing.T) {
	r := NewRouter(RouterConfig{})
	msg := broker.Message{Value: []byte(`not-json`)}

	dec, err := r.Route(context.Background(), "work.in", msg)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if dec.TargetTopic != "" || dec.Label != "" || len(dec.Meta) != 0 {
		t.Fatalf("expected noop decision, got %#v", dec)
	}
}

func TestMultiAgentRouter_StrictModeReturnsError(t *testing.T) {
	r := NewRouter(RouterConfig{Strict: true})
	msg := broker.Message{Value: []byte(`{"sender":"planner","intent":"x","payload":{"ok":true}}`)} // missing route target

	_, err := r.Route(context.Background(), "work.in", msg)
	if !errors.Is(err, ErrNoRouteTarget) {
		t.Fatalf("expected ErrNoRouteTarget, got %v", err)
	}
}

func TestMultiAgentRouter_SourceTopicFilter(t *testing.T) {
	r := NewRouter(RouterConfig{SourceTopics: SourceTopicSet("multiagent.entry")})
	msg := broker.Message{Value: []byte(`{"sender":"planner","receiver":"coder","intent":"x","payload":{"ok":true}}`)}

	dec, err := r.Route(context.Background(), "other.topic", msg)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if dec.TargetTopic != "" || dec.Label != "" {
		t.Fatalf("expected noop due to source filter, got %#v", dec)
	}

	dec, err = r.Route(context.Background(), "multiagent.entry", msg)
	if err != nil {
		t.Fatalf("Route allowed topic: %v", err)
	}
	if dec.TargetTopic != "agent.coder.inbox" {
		t.Fatalf("unexpected target for allowed topic: %#v", dec)
	}
}

func TestMultiAgentRouter_CapabilityNeedsRegistry(t *testing.T) {
	r := NewRouter(RouterConfig{})
	msg := broker.Message{Value: []byte(`{"sender":"planner","capability":"coding","intent":"x","payload":{"ok":true}}`)}

	_, err := r.Route(context.Background(), "work.in", msg)
	if !errors.Is(err, ErrNilRegistry) {
		t.Fatalf("expected ErrNilRegistry, got %v", err)
	}
}
