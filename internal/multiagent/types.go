package multiagent

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type RouteKind string

const (
	RouteKindDirect     RouteKind = "direct"
	RouteKindCapability RouteKind = "capability"
	RouteKindBroadcast  RouteKind = "broadcast"
)

// This is the foundational message contract carried inside broker.Message.Value
// It stays JSON-first so callers can produce through existing /v1/produce APIs without new endpoints
type AgentMessage struct {
	MessageID     string     `json:"message_id,omitempty"`
	CorrelationID string     `json:"correlation_id,omitempty"`
	ReplyTo       string     `json:"reply_to,omitempty"`
	CreatedAt     *time.Time `json:"created_at,omitempty"`
	TenantID      string     `json:"tenant_id,omitempty"`

	Sender     string `json:"sender"`
	Receiver   string `json:"receiver,omitempty"`
	Team       string `json:"team,omitempty"`
	Role       string `json:"role,omitempty"`
	Capability string `json:"capability,omitempty"`
	Intent     string `json:"intent"`
	Route      string `json:"route,omitempty"`

	Coordination *CoordinationMetadata `json:"coordination,omitempty"`
	Payload json.RawMessage `json:"payload"`
}

func ParseAgentMessage(b []byte) (AgentMessage, error) {
	var m AgentMessage
	if len(strings.TrimSpace(string(b))) == 0 {
		return m, ErrEmptyMessage
	}

	if err := json.Unmarshal(b, &m); err != nil {
		return m, fmt.Errorf("parse agent message json: %w", err)
	}

	return m, nil
}

func AgentInboxTopic(agentID string) (string, error) {
	if err := validateTopicIDComponent("agent id", agentID); err != nil {
		return "", err
	}

	return "agent." + agentID + ".inbox", nil
}

func AgentOutboxTopic(agentID string) (string, error) {
	if err := validateTopicIDComponent("agent id", agentID); err != nil {
		return "", err
	}

	return "agent." + agentID + ".outbox", nil
}

func TeamBroadcastTopic(teamID string) (string, error) {
	if err := validateTopicIDComponent("team id", teamID); err != nil {
		return "", err
	}

	return "team." + teamID + ".broadcast", nil
}

func AgentIDFromInboxTopic(topic string) (string, bool) {
	return parseTopicComponent(topic, "agent.", ".inbox", "agent id")
}

func AgentIDFromOutboxTopic(topic string) (string, bool) {
	return parseTopicComponent(topic, "agent.", ".outbox", "agent id")
}

func TeamIDFromBroadcastTopic(topic string) (string, bool) {
	return parseTopicComponent(topic, "team.", ".broadcast", "team id")
}

func parseTopicComponent(topic, prefix, suffix, kind string) (string, bool) {
	if !strings.HasPrefix(topic, prefix) || !strings.HasSuffix(topic, suffix) {
		return "", false
	}

	id := strings.TrimSuffix(strings.TrimPrefix(topic, prefix), suffix)
	if id == "" {
		return "", false
	}

	if err := validateTopicIDComponent(kind, id); err != nil {
		return "", false
	}

	return id, true
}

func (m AgentMessage) CapabilityOrRole() string {
	if strings.TrimSpace(m.Capability) != "" {
		return strings.TrimSpace(m.Capability)
	}
	return strings.TrimSpace(m.Role)
}

func (m AgentMessage) ResolvedRouteKind() (RouteKind, error) {
	hasReceiver := strings.TrimSpace(m.Receiver) != ""
	hasTeam := strings.TrimSpace(m.Team) != ""
	hasCapability := strings.TrimSpace(m.Capability) != "" || strings.TrimSpace(m.Role) != ""

	count := 0
	if hasReceiver {
		count++
	}

	if hasTeam {
		count++
	}

	if hasCapability {
		count++
	}

	if count == 0 {
		return "", ErrNoRouteTarget
	}

	if count > 1 {
		return "", ErrAmbiguousRoute
	}

	var inferred RouteKind
	switch {
	case hasReceiver:
		inferred = RouteKindDirect
	case hasTeam:
		inferred = RouteKindBroadcast
	default:
		inferred = RouteKindCapability
	}

	if raw := strings.TrimSpace(m.Route); raw != "" {
		declared := RouteKind(strings.ToLower(raw))
		switch declared {
		case RouteKindDirect, RouteKindCapability, RouteKindBroadcast:
			if declared != inferred {
				return "", fmt.Errorf("%w: declared=%s inferred=%s", ErrRouteMismatch, declared, inferred)
			}

			return declared, nil
		default:
			return "", fmt.Errorf("%w: %q", ErrInvalidRouteKind, m.Route)
		}
	}

	return inferred, nil
}

// TargetTopic returns the convention topic for direct/broadcast messages. Capability-routed messages require a registry lookup, so this returns ErrTargetTopicRequiresRouting.
func (m AgentMessage) TargetTopic() (string, error) {
	kind, err := m.ResolvedRouteKind()
	if err != nil {
		return "", err
	}

	switch kind {
	case RouteKindDirect:
		return AgentInboxTopic(strings.TrimSpace(m.Receiver))
	case RouteKindBroadcast:
		return TeamBroadcastTopic(strings.TrimSpace(m.Team))
	case RouteKindCapability:
		return "", ErrTargetTopicRequiresRouting
	default:
		return "", fmt.Errorf("%w: %s", ErrInvalidRouteKind, kind)
	}
}
