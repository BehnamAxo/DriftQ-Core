package multiagent

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	ErrEmptyMessage               = errors.New("agent message is empty")
	ErrNoRouteTarget              = errors.New("agent message must include exactly one route target (receiver, capability/role, or team)")
	ErrAmbiguousRoute             = errors.New("agent message has ambiguous route target")
	ErrInvalidRouteKind           = errors.New("invalid route kind")
	ErrRouteMismatch              = errors.New("declared route does not match message fields")
	ErrTargetTopicRequiresRouting = errors.New("target topic requires capability routing lookup")
	ErrSourceTopicSenderMismatch  = errors.New("sender does not match source outbox topic")
	ErrMissingSender              = errors.New("sender is required")
	ErrMissingIntent              = errors.New("intent is required")
	ErrMissingPayload             = errors.New("payload is required")
	ErrPayloadMustBeJSONObject    = errors.New("payload must be a JSON object")
	ErrInvalidPayloadJSON         = errors.New("payload is not valid JSON")
	ErrInvalidTopicIDComponent    = errors.New("invalid topic id component")
)

var topicIDComponentRe = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

func validateTopicIDComponent(kind, id string) error {
	v := strings.TrimSpace(id)
	if v == "" {
		return fmt.Errorf("%w: %s cannot be empty", ErrInvalidTopicIDComponent, kind)
	}

	if v != id {
		return fmt.Errorf("%w: %s %q cannot have surrounding whitespace", ErrInvalidTopicIDComponent, kind, id)
	}

	if !topicIDComponentRe.MatchString(v) {
		return fmt.Errorf("%w: %s %q must match %s", ErrInvalidTopicIDComponent, kind, id, topicIDComponentRe.String())
	}

	return nil
}

func (m AgentMessage) Validate() error {
	if strings.TrimSpace(m.Sender) == "" {
		return ErrMissingSender
	}

	if strings.TrimSpace(m.Intent) == "" {
		return ErrMissingIntent
	}

	if err := validateTopicIDComponent("sender", m.Sender); err != nil {
		return err
	}

	if strings.TrimSpace(m.Receiver) != "" {
		if err := validateTopicIDComponent("receiver", m.Receiver); err != nil {
			return err
		}
	}

	if strings.TrimSpace(m.Team) != "" {
		if err := validateTopicIDComponent("team", m.Team); err != nil {
			return err
		}
	}

	kind, err := m.ResolvedRouteKind()
	if err != nil {
		return err
	}

	if kind == RouteKindCapability && strings.TrimSpace(m.CapabilityOrRole()) == "" {
		return ErrNoRouteTarget
	}

	if len(m.Payload) == 0 {
		return ErrMissingPayload
	}

	if !json.Valid(m.Payload) {
		return ErrInvalidPayloadJSON
	}

	var decoded any
	if err := json.Unmarshal(m.Payload, &decoded); err != nil {
		return fmt.Errorf("decode payload: %w", err)
	}

	if _, ok := decoded.(map[string]any); !ok {
		return ErrPayloadMustBeJSONObject
	}

	if m.Coordination != nil {
		if err := m.Coordination.NormalizeAndValidate(); err != nil {
			return err
		}
	}

	return nil
}
