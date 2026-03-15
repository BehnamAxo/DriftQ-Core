package multiagent

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type CoordinationPattern string
type FailureAction string

const (
	PatternPlannerWorker   CoordinationPattern = "planner_worker"
	PatternPlannerReviewer CoordinationPattern = "planner_reviewer"
	PatternHandoff         CoordinationPattern = "handoff"
	PatternBroadcast       CoordinationPattern = "broadcast_request"
	PatternRequestReply    CoordinationPattern = "request_reply"
	PatternEscalation      CoordinationPattern = "escalation"
)

const (
	FailureActionEscalate FailureAction = "escalate"
	FailureActionHandoff  FailureAction = "handoff"
	FailureActionFail     FailureAction = "fail"
)

type CoordinationTarget struct {
	Receiver   string `json:"receiver,omitempty"`
	Team       string `json:"team,omitempty"`
	Capability string `json:"capability,omitempty"`
	Role       string `json:"role,omitempty"`
}

type FailureStrategy struct {
	Action            FailureAction        `json:"action,omitempty"`
	EscalationChain   []CoordinationTarget `json:"escalation_chain,omitempty"`
	FallbackHandoffTo *CoordinationTarget  `json:"fallback_handoff_to,omitempty"`
}

type CoordinationMetadata struct {
	Pattern         CoordinationPattern `json:"pattern,omitempty"`
	ParentMessageID string              `json:"parent_message_id,omitempty"`
	ParentSender    string              `json:"parent_sender,omitempty"`
	HandoffFrom     string              `json:"handoff_from,omitempty"`
	HandoffReason   string              `json:"handoff_reason,omitempty"`
	RequiresReply   bool                `json:"requires_reply,omitempty"`
	ReviewRequired  bool                `json:"review_required,omitempty"`
	EscalationIndex int                 `json:"escalation_index,omitempty"`
	FailureStrategy *FailureStrategy    `json:"failure_strategy,omitempty"`
}

type FailureSignal struct {
	Code        string `json:"code,omitempty"`
	Message     string `json:"message,omitempty"`
	FailedAgent string `json:"failed_agent,omitempty"`
	Retryable   bool   `json:"retryable,omitempty"`
}

func (c *CoordinationMetadata) NormalizeAndValidate() error {
	if c == nil {
		return nil
	}

	c.Pattern = CoordinationPattern(strings.TrimSpace(string(c.Pattern)))
	c.ParentMessageID = strings.TrimSpace(c.ParentMessageID)
	c.ParentSender = strings.TrimSpace(c.ParentSender)
	c.HandoffFrom = strings.TrimSpace(c.HandoffFrom)
	c.HandoffReason = strings.TrimSpace(c.HandoffReason)

	switch c.Pattern {
	case "", PatternPlannerWorker, PatternPlannerReviewer, PatternHandoff, PatternBroadcast, PatternRequestReply, PatternEscalation:
	default:
		return fmt.Errorf("invalid coordination pattern %q", c.Pattern)
	}

	if c.ParentSender != "" {
		if err := validateTopicIDComponent("parent sender", c.ParentSender); err != nil {
			return err
		}
	}

	if c.HandoffFrom != "" {
		if err := validateTopicIDComponent("handoff_from", c.HandoffFrom); err != nil {
			return err
		}
	}

	if c.EscalationIndex < 0 {
		c.EscalationIndex = 0
	}

	if c.FailureStrategy != nil {
		if err := c.FailureStrategy.NormalizeAndValidate(); err != nil {
			return err
		}
	}
	return nil
}

func (f *FailureStrategy) NormalizeAndValidate() error {
	if f == nil {
		return nil
	}

	f.Action = FailureAction(strings.TrimSpace(string(f.Action)))
	switch f.Action {
	case "", FailureActionEscalate, FailureActionHandoff, FailureActionFail:
	default:
		return fmt.Errorf("invalid failure action %q", f.Action)
	}

	for i := range f.EscalationChain {
		if err := f.EscalationChain[i].NormalizeAndValidate(); err != nil {
			return err
		}
	}

	if f.FallbackHandoffTo != nil {
		if err := f.FallbackHandoffTo.NormalizeAndValidate(); err != nil {
			return err
		}
	}

	return nil
}

func (t *CoordinationTarget) NormalizeAndValidate() error {
	if t == nil {
		return fmt.Errorf("coordination target is nil")
	}

	t.Receiver = strings.TrimSpace(t.Receiver)
	t.Team = strings.TrimSpace(t.Team)
	t.Capability = strings.TrimSpace(t.Capability)
	t.Role = strings.TrimSpace(t.Role)

	count := 0
	if t.Receiver != "" {
		count++
		if err := validateTopicIDComponent("receiver", t.Receiver); err != nil {
			return err
		}
	}

	if t.Team != "" {
		count++
		if err := validateTopicIDComponent("team", t.Team); err != nil {
			return err
		}
	}

	if t.Capability != "" {
		count++
		if err := validateTopicIDComponent("capability", t.Capability); err != nil {
			return err
		}
	}

	if t.Role != "" {
		count++
		if err := validateTopicIDComponent("role", t.Role); err != nil {
			return err
		}
	}

	if count == 0 || count > 1 {
		return ErrNoRouteTarget
	}

	return nil
}

func applyTarget(m *AgentMessage, target CoordinationTarget) error {
	if err := target.NormalizeAndValidate(); err != nil {
		return err
	}

	m.Receiver = ""
	m.Team = ""
	m.Capability = ""
	m.Role = ""

	switch {
	case target.Receiver != "":
		m.Receiver = target.Receiver
	case target.Team != "":
		m.Team = target.Team
	case target.Capability != "":
		m.Capability = target.Capability
	case target.Role != "":
		m.Role = target.Role
	}

	return nil
}

func marshalPayloadObject(payload any) (json.RawMessage, error) {
	switch typed := payload.(type) {
	case nil:
		return nil, ErrMissingPayload
	case json.RawMessage:
		if !json.Valid(typed) {
			return nil, ErrInvalidPayloadJSON
		}
		var decoded any
		if err := json.Unmarshal(typed, &decoded); err != nil {
			return nil, err
		}
		if _, ok := decoded.(map[string]any); !ok {
			return nil, ErrPayloadMustBeJSONObject
		}
		return append(json.RawMessage(nil), typed...), nil
	default:
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		return marshalPayloadObject(json.RawMessage(b))
	}
}

func NewPlannerWorkerMessage(sender string, worker CoordinationTarget, intent string, payload any) (AgentMessage, error) {
	raw, err := marshalPayloadObject(payload)
	if err != nil {
		return AgentMessage{}, err
	}

	msg := AgentMessage{
		Sender:    strings.TrimSpace(sender),
		Intent:    strings.TrimSpace(intent),
		Payload:   raw,
		Route:     "",
		CreatedAt: ptrTime(time.Now().UTC()),
		Coordination: &CoordinationMetadata{
			Pattern: PatternPlannerWorker,
		},
	}

	if err := applyTarget(&msg, worker); err != nil {
		return AgentMessage{}, err
	}

	return msg, msg.Validate()
}

func NewReviewerRequest(sender string, reviewer CoordinationTarget, intent string, payload any) (AgentMessage, error) {
	msg, err := NewPlannerWorkerMessage(sender, reviewer, intent, payload)

	if err != nil {
		return AgentMessage{}, err
	}

	msg.Coordination.Pattern = PatternPlannerReviewer
	msg.Coordination.ReviewRequired = true
	return msg, nil
}

func NewBroadcastRequest(sender, team, intent string, payload any, requireReplies bool) (AgentMessage, error) {
	raw, err := marshalPayloadObject(payload)
	if err != nil {
		return AgentMessage{}, err
	}

	msg := AgentMessage{
		Sender:    strings.TrimSpace(sender),
		Team:      strings.TrimSpace(team),
		Intent:    strings.TrimSpace(intent),
		Payload:   raw,
		CreatedAt: ptrTime(time.Now().UTC()),
		Coordination: &CoordinationMetadata{
			Pattern:       PatternBroadcast,
			RequiresReply: requireReplies,
		},
	}

	if requireReplies {
		if inbox, err := AgentInboxTopic(msg.Sender); err == nil {
			msg.ReplyTo = inbox
		}
	}

	return msg, msg.Validate()
}

func NewRequestReplyMessage(sender string, target CoordinationTarget, intent string, payload any) (AgentMessage, error) {
	msg, err := NewPlannerWorkerMessage(sender, target, intent, payload)
	if err != nil {
		return AgentMessage{}, err
	}

	msg.Coordination.Pattern = PatternRequestReply
	msg.Coordination.RequiresReply = true

	if inbox, err := AgentInboxTopic(msg.Sender); err == nil {
		msg.ReplyTo = inbox
	}

	return msg, nil
}

func NewReply(request AgentMessage, sender string, payload any) (AgentMessage, error) {
	raw, err := marshalPayloadObject(payload)
	if err != nil {
		return AgentMessage{}, err
	}

	msg := AgentMessage{
		Sender:        strings.TrimSpace(sender),
		Receiver:      strings.TrimSpace(request.Sender),
		Intent:        strings.TrimSpace(request.Intent) + ".reply",
		Payload:       raw,
		CreatedAt:     ptrTime(time.Now().UTC()),
		CorrelationID: firstNonEmpty(request.CorrelationID, request.MessageID),
		Coordination: &CoordinationMetadata{
			Pattern:         PatternRequestReply,
			ParentMessageID: strings.TrimSpace(firstNonEmpty(request.MessageID, request.CorrelationID)),
			ParentSender:    strings.TrimSpace(request.Sender),
		},
	}

	return msg, msg.Validate()
}

func NewHandoff(from AgentMessage, sender string, target CoordinationTarget, reason string, payload any) (AgentMessage, error) {
	raw, err := marshalPayloadObject(payload)

	if err != nil {
		return AgentMessage{}, err
	}

	msg := AgentMessage{
		Sender:        strings.TrimSpace(sender),
		Intent:        strings.TrimSpace(from.Intent),
		Payload:       raw,
		CreatedAt:     ptrTime(time.Now().UTC()),
		TenantID:      strings.TrimSpace(from.TenantID),
		CorrelationID: firstNonEmpty(strings.TrimSpace(from.CorrelationID), strings.TrimSpace(from.MessageID)),
		Coordination: &CoordinationMetadata{
			Pattern:         PatternHandoff,
			ParentMessageID: firstNonEmpty(strings.TrimSpace(from.MessageID), strings.TrimSpace(from.CorrelationID)),
			ParentSender:    strings.TrimSpace(from.Sender),
			HandoffFrom:     strings.TrimSpace(sender),
			HandoffReason:   strings.TrimSpace(reason),
		},
	}

	if err := applyTarget(&msg, target); err != nil {
		return AgentMessage{}, err
	}

	return msg, msg.Validate()
}

func EscalateOnFailure(from AgentMessage, sender string, failure FailureSignal) (AgentMessage, error) {
	if from.Coordination == nil || from.Coordination.FailureStrategy == nil {
		return AgentMessage{}, fmt.Errorf("failure strategy is required")
	}

	strategy := from.Coordination.FailureStrategy
	index := from.Coordination.EscalationIndex

	if index < 0 {
		index = 0
	}

	var target CoordinationTarget
	switch strategy.Action {
	case FailureActionEscalate:
		if index >= len(strategy.EscalationChain) {
			return AgentMessage{}, fmt.Errorf("escalation chain exhausted")
		}
		target = strategy.EscalationChain[index]
	case FailureActionHandoff:
		if strategy.FallbackHandoffTo == nil {
			return AgentMessage{}, fmt.Errorf("fallback handoff target is required")
		}
		target = *strategy.FallbackHandoffTo
	default:
		return AgentMessage{}, fmt.Errorf("failure action %q does not produce a follow-up message", strategy.Action)
	}

	payload, err := marshalPayloadObject(map[string]any{
		"original_payload": json.RawMessage(from.Payload),
		"failure":          failure,
	})

	if err != nil {
		return AgentMessage{}, err
	}

	msg := AgentMessage{
		Sender:        strings.TrimSpace(sender),
		Intent:        strings.TrimSpace(from.Intent),
		Payload:       payload,
		CreatedAt:     ptrTime(time.Now().UTC()),
		TenantID:      strings.TrimSpace(from.TenantID),
		CorrelationID: firstNonEmpty(strings.TrimSpace(from.CorrelationID), strings.TrimSpace(from.MessageID)),
		Coordination: &CoordinationMetadata{
			Pattern:         PatternEscalation,
			ParentMessageID: firstNonEmpty(strings.TrimSpace(from.MessageID), strings.TrimSpace(from.CorrelationID)),
			ParentSender:    strings.TrimSpace(from.Sender),
			EscalationIndex: index + 1,
			FailureStrategy: strategy,
		},
	}

	if err := applyTarget(&msg, target); err != nil {
		return AgentMessage{}, err
	}

	return msg, msg.Validate()
}

func ptrTime(t time.Time) *time.Time { return &t }

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v != "" {
			return v
		}
	}

	return ""
}
