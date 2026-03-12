package multiagent

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/driftq-org/DriftQ-Core/internal/broker"
)

const RouterName = "multiagent-v3.1"

type RouterConfig struct {
	// Registry is required for capability/role routing. Direct and broadcast routing do not need it.
	Registry *CapabilityRegistry

	// If true, parse/validation failures return an error. If false, non-agent or
	// malformed payloads are ignored and the original producer topic is preserved
	//
	// Default behavior in NewRouter is false to avoid breaking non-agent traffic
	// when this router is attached globally
	Strict bool

	// Optional list of source topics this router should handle. If empty, it will
	// inspect every produced message.
	SourceTopics map[string]struct{}
}

type Router struct {
	cfg RouterConfig
}

func NewRouter(cfg RouterConfig) *Router {
	if cfg.SourceTopics == nil {
		cfg.SourceTopics = make(map[string]struct{})
	}
	return &Router{cfg: cfg}
}

func NewRouterWithRegistry(reg *CapabilityRegistry) *Router {
	return NewRouter(RouterConfig{Registry: reg})
}

func SourceTopicSet(topics ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(topics))

	for _, t := range topics {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		out[t] = struct{}{}
	}

	return out
}

func (r *Router) Route(_ context.Context, topic string, msg broker.Message) (broker.RoutingDecision, error) {
	if r == nil {
		return broker.RoutingDecision{}, nil
	}

	if len(r.cfg.SourceTopics) > 0 {
		if _, ok := r.cfg.SourceTopics[topic]; !ok {
			return broker.RoutingDecision{}, nil
		}
	}

	am, err := ParseAgentMessage(msg.Value)
	if err != nil {
		if r.cfg.Strict {
			return broker.RoutingDecision{}, err
		}
		return broker.RoutingDecision{}, nil
	}

	if err := am.Validate(); err != nil {
		if r.cfg.Strict {
			return broker.RoutingDecision{}, err
		}
		return broker.RoutingDecision{}, nil
	}

	if sourceAgentID, ok := AgentIDFromOutboxTopic(topic); ok {
		sender := strings.TrimSpace(am.Sender)
		if sender != sourceAgentID {
			return broker.RoutingDecision{}, fmt.Errorf("%w: source_topic=%s sender=%s", ErrSourceTopicSenderMismatch, topic, sender)
		}
	}

	kind, err := am.ResolvedRouteKind()
	if err != nil {
		return broker.RoutingDecision{}, err
	}

	decision := broker.RoutingDecision{
		Label: "multiagent." + string(kind),
		Meta: map[string]string{
			"router":       RouterName,
			"route_kind":   string(kind),
			"source_topic": topic,
			"sender":       strings.TrimSpace(am.Sender),
			"intent":       strings.TrimSpace(am.Intent),
		},
	}

	if tid := strings.TrimSpace(am.TenantID); tid != "" {
		decision.Meta["tenant_id"] = tid
	}

	if cid := strings.TrimSpace(am.CorrelationID); cid != "" {
		decision.Meta["correlation_id"] = cid
	}

	if mid := strings.TrimSpace(am.MessageID); mid != "" {
		decision.Meta["message_id"] = mid
	}

	if sourceAgentID, ok := AgentIDFromOutboxTopic(topic); ok {
		decision.Meta["source_agent"] = sourceAgentID
	}

	switch kind {
	case RouteKindDirect:
		receiver := strings.TrimSpace(am.Receiver)
		target, err := AgentInboxTopic(receiver)
		if err != nil {
			return broker.RoutingDecision{}, err
		}
		decision.TargetTopic = target
		decision.Meta["receiver"] = receiver
		return decision, nil

	case RouteKindBroadcast:
		team := strings.TrimSpace(am.Team)
		target, err := TeamBroadcastTopic(team)

		if err != nil {
			return broker.RoutingDecision{}, err
		}

		decision.TargetTopic = target
		decision.Meta["team"] = team
		return decision, nil

	case RouteKindCapability:
		capability := am.CapabilityOrRole()
		decision.Meta["capability"] = capability

		if r.cfg.Registry == nil {
			return broker.RoutingDecision{}, ErrNilRegistry
		}

		agentID, err := r.cfg.Registry.ResolveAgent(capability)
		if err != nil {
			return broker.RoutingDecision{}, err
		}

		target, err := AgentInboxTopic(agentID)
		if err != nil {
			return broker.RoutingDecision{}, err
		}

		decision.TargetTopic = target
		decision.Meta["selected_agent"] = agentID
		return decision, nil

	default:
		return broker.RoutingDecision{}, fmt.Errorf("%w: %s", ErrInvalidRouteKind, kind)
	}
}

// IsCapabilityResolveError helps callers/tests detect expected registry misses
func IsCapabilityResolveError(err error) bool {
	return errors.Is(err, ErrCapabilityNotFound) ||
		errors.Is(err, ErrCapabilityHasNoAgents) ||
		errors.Is(err, ErrEmptyCapability) ||
		errors.Is(err, ErrNilRegistry)
}
