package multiagent

import (
	"errors"
	"testing"
)

func TestTopicNamingConventions(t *testing.T) {
	t.Run("agent inbox", func(t *testing.T) {
		got, err := AgentInboxTopic("planner")
		if err != nil {
			t.Fatalf("AgentInboxTopic: %v", err)
		}

		if got != "agent.planner.inbox" {
			t.Fatalf("unexpected topic: %s", got)
		}
	})

	t.Run("agent outbox", func(t *testing.T) {
		got, err := AgentOutboxTopic("coder_1")
		if err != nil {
			t.Fatalf("AgentOutboxTopic: %v", err)
		}

		if got != "agent.coder_1.outbox" {
			t.Fatalf("unexpected topic: %s", got)
		}
	})

	t.Run("team broadcast", func(t *testing.T) {
		got, err := TeamBroadcastTopic("core-team")
		if err != nil {
			t.Fatalf("TeamBroadcastTopic: %v", err)
		}

		if got != "team.core-team.broadcast" {
			t.Fatalf("unexpected topic: %s", got)
		}
	})

	for _, tc := range []struct {
		name string
		fn   func(string) (string, error)
		id   string
	}{
		{name: "empty", fn: AgentInboxTopic, id: ""},
		{name: "space", fn: AgentInboxTopic, id: "planner bot"},
		{name: "surrounding_whitespace", fn: AgentInboxTopic, id: " planner "},
		{name: "dot", fn: TeamBroadcastTopic, id: "core.team"},
		{name: "slash", fn: AgentOutboxTopic, id: "planner/bot"},
	} {
		t.Run("invalid_"+tc.name, func(t *testing.T) {
			_, err := tc.fn(tc.id)
			if err == nil {
				t.Fatalf("expected error for %q", tc.id)
			}

			if !errors.Is(err, ErrInvalidTopicIDComponent) {
				t.Fatalf("expected ErrInvalidTopicIDComponent, got %v", err)
			}
		})
	}
}

func TestAgentMessageValidate_Direct(t *testing.T) {
	m := AgentMessage{
		Sender:   "planner",
		Receiver: "coder",
		Intent:   "generate_code",
		Payload:  []byte(`{"task":"implement PR #1"}`),
	}

	if err := m.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	kind, err := m.ResolvedRouteKind()
	if err != nil {
		t.Fatalf("ResolvedRouteKind: %v", err)
	}

	if kind != RouteKindDirect {
		t.Fatalf("expected direct, got %s", kind)
	}

	topic, err := m.TargetTopic()
	if err != nil {
		t.Fatalf("TargetTopic: %v", err)
	}

	if topic != "agent.coder.inbox" {
		t.Fatalf("unexpected target topic: %s", topic)
	}
}

func TestAgentMessageValidate_Capability(t *testing.T) {
	m := AgentMessage{
		Sender:     "planner",
		Capability: "coding",
		Intent:     "implement",
		Payload:    []byte(`{"repo":"driftq-core"}`),
	}

	if err := m.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	kind, err := m.ResolvedRouteKind()
	if err != nil {
		t.Fatalf("ResolvedRouteKind: %v", err)
	}

	if kind != RouteKindCapability {
		t.Fatalf("expected capability, got %s", kind)
	}

	if _, err := m.TargetTopic(); !errors.Is(err, ErrTargetTopicRequiresRouting) {
		t.Fatalf("expected ErrTargetTopicRequiresRouting, got %v", err)
	}
}

func TestAgentMessageValidate_Broadcast(t *testing.T) {
	m := AgentMessage{
		Sender:  "planner",
		Team:    "core",
		Intent:  "announce",
		Route:   "broadcast",
		Payload: []byte(`{"msg":"deployment starting"}`),
	}

	if err := m.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	kind, err := m.ResolvedRouteKind()
	if err != nil {
		t.Fatalf("ResolvedRouteKind: %v", err)
	}

	if kind != RouteKindBroadcast {
		t.Fatalf("expected broadcast, got %s", kind)
	}

	topic, err := m.TargetTopic()
	if err != nil {
		t.Fatalf("TargetTopic: %v", err)
	}

	if topic != "team.core.broadcast" {
		t.Fatalf("unexpected target topic: %s", topic)
	}
}

func TestAgentMessageValidate_Errors(t *testing.T) {
	cases := []struct {
		name string
		msg  AgentMessage
		want error
	}{
		{
			name: "missing sender",
			msg:  AgentMessage{Intent: "x", Receiver: "coder", Payload: []byte(`{"ok":true}`)},
			want: ErrMissingSender,
		},
		{
			name: "missing intent",
			msg:  AgentMessage{Sender: "planner", Receiver: "coder", Payload: []byte(`{"ok":true}`)},
			want: ErrMissingIntent,
		},
		{
			name: "missing route target",
			msg:  AgentMessage{Sender: "planner", Intent: "x", Payload: []byte(`{"ok":true}`)},
			want: ErrNoRouteTarget,
		},
		{
			name: "ambiguous route target",
			msg:  AgentMessage{Sender: "planner", Receiver: "coder", Capability: "coding", Intent: "x", Payload: []byte(`{"ok":true}`)},
			want: ErrAmbiguousRoute,
		},
		{
			name: "route mismatch",
			msg:  AgentMessage{Sender: "planner", Receiver: "coder", Route: "broadcast", Intent: "x", Payload: []byte(`{"ok":true}`)},
			want: ErrRouteMismatch,
		},
		{
			name: "invalid route kind",
			msg:  AgentMessage{Sender: "planner", Receiver: "coder", Route: "weird", Intent: "x", Payload: []byte(`{"ok":true}`)},
			want: ErrInvalidRouteKind,
		},
		{
			name: "missing payload",
			msg:  AgentMessage{Sender: "planner", Receiver: "coder", Intent: "x"},
			want: ErrMissingPayload,
		},
		{
			name: "invalid payload json",
			msg:  AgentMessage{Sender: "planner", Receiver: "coder", Intent: "x", Payload: []byte(`{"ok":`)},
			want: ErrInvalidPayloadJSON,
		},
		{
			name: "payload not object",
			msg:  AgentMessage{Sender: "planner", Receiver: "coder", Intent: "x", Payload: []byte(`[]`)},
			want: ErrPayloadMustBeJSONObject,
		},
		{
			name: "invalid receiver topic component",
			msg:  AgentMessage{Sender: "planner", Receiver: "coder bot", Intent: "x", Payload: []byte(`{"ok":true}`)},
			want: ErrInvalidTopicIDComponent,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.msg.Validate()
			if err == nil {
				t.Fatalf("expected error")
			}

			if !errors.Is(err, tc.want) {
				t.Fatalf("expected %v, got %v", tc.want, err)
			}
		})
	}
}

func TestParseAgentMessage(t *testing.T) {
	_, err := ParseAgentMessage([]byte("   "))
	if !errors.Is(err, ErrEmptyMessage) {
		t.Fatalf("expected ErrEmptyMessage, got %v", err)
	}

	m, err := ParseAgentMessage([]byte(`{"sender":"planner","receiver":"coder","intent":"x","payload":{"ok":true}}`))
	if err != nil {
		t.Fatalf("ParseAgentMessage: %v", err)
	}

	if m.Sender != "planner" || m.Receiver != "coder" || m.Intent != "x" {
		t.Fatalf("unexpected parsed message: %+v", m)
	}

	if err := m.Validate(); err != nil {
		t.Fatalf("Validate parsed message: %v", err)
	}
}
