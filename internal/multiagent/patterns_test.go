package multiagent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/driftq-org/DriftQ-Core/internal/broker"
)

func TestCoordinationPatterns_PlannerWorkerReviewerAndReply(t *testing.T) {
	msg, err := NewPlannerWorkerMessage("planner", CoordinationTarget{Capability: "coding"}, "implement", map[string]any{"task": "v3.11"})
	if err != nil {
		t.Fatalf("NewPlannerWorkerMessage: %v", err)
	}
	if msg.Coordination == nil || msg.Coordination.Pattern != PatternPlannerWorker {
		t.Fatalf("unexpected planner/worker metadata: %+v", msg)
	}

	review, err := NewReviewerRequest("planner", CoordinationTarget{Role: "reviewer"}, "review", map[string]any{"pr": 12})
	if err != nil {
		t.Fatalf("NewReviewerRequest: %v", err)
	}
	if !review.Coordination.ReviewRequired || review.Role != "reviewer" {
		t.Fatalf("unexpected reviewer request: %+v", review)
	}

	replyReq, err := NewRequestReplyMessage("planner", CoordinationTarget{Receiver: "coder-a"}, "implement", map[string]any{"task": "reply"})
	if err != nil {
		t.Fatalf("NewRequestReplyMessage: %v", err)
	}
	if !replyReq.Coordination.RequiresReply || replyReq.ReplyTo != "agent.planner.inbox" {
		t.Fatalf("unexpected request/reply message: %+v", replyReq)
	}

	replyReq.MessageID = "msg-123"
	reply, err := NewReply(replyReq, "coder-a", map[string]any{"done": true})
	if err != nil {
		t.Fatalf("NewReply: %v", err)
	}
	if reply.Receiver != "planner" || reply.CorrelationID != "msg-123" {
		t.Fatalf("unexpected reply: %+v", reply)
	}
}

func TestCoordinationPatterns_HandoffBroadcastAndEscalation(t *testing.T) {
	orig, err := NewPlannerWorkerMessage("planner", CoordinationTarget{Receiver: "worker-a"}, "implement", map[string]any{"task": "x"})
	if err != nil {
		t.Fatalf("NewPlannerWorkerMessage: %v", err)
	}
	orig.MessageID = "msg-orig"

	handoff, err := NewHandoff(orig, "worker-a", CoordinationTarget{Receiver: "worker-b"}, "needs specialist", map[string]any{"task": "x", "note": "handoff"})
	if err != nil {
		t.Fatalf("NewHandoff: %v", err)
	}
	if handoff.Coordination == nil || handoff.Coordination.HandoffFrom != "worker-a" {
		t.Fatalf("unexpected handoff: %+v", handoff)
	}

	bcast, err := NewBroadcastRequest("planner", "core", "announce", map[string]any{"msg": "deploy"}, true)
	if err != nil {
		t.Fatalf("NewBroadcastRequest: %v", err)
	}
	if bcast.Team != "core" || !bcast.Coordination.RequiresReply {
		t.Fatalf("unexpected broadcast request: %+v", bcast)
	}

	orig.Coordination = &CoordinationMetadata{
		Pattern: PatternPlannerWorker,
		FailureStrategy: &FailureStrategy{
			Action: FailureActionEscalate,
			EscalationChain: []CoordinationTarget{
				{Role: "reviewer"},
				{Receiver: "lead"},
			},
		},
	}
	escalated, err := EscalateOnFailure(orig, "worker-a", FailureSignal{Code: "timeout", Message: "worker timed out"})
	if err != nil {
		t.Fatalf("EscalateOnFailure: %v", err)
	}
	if escalated.Role != "reviewer" || escalated.Coordination.EscalationIndex != 1 {
		t.Fatalf("unexpected escalated message: %+v", escalated)
	}
}

func TestCoordinationRouter_EmitsPatternMetadata(t *testing.T) {
	reg := NewCapabilityRegistry()
	if err := reg.Register("coding", "coder-a"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	r := NewRouterWithRegistry(reg)

	msg, err := NewRequestReplyMessage("planner", CoordinationTarget{Capability: "coding"}, "implement", map[string]any{"task": "metadata"})
	if err != nil {
		t.Fatalf("NewRequestReplyMessage: %v", err)
	}
	msg.MessageID = "msg-1"
	raw, err := jsonMarshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	dec, err := r.Route(context.Background(), "work.in", broker.Message{Value: raw})
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if dec.Meta["pattern"] != string(PatternRequestReply) || dec.Meta["requires_reply"] != "true" {
		t.Fatalf("expected coordination metadata, got %#v", dec.Meta)
	}
}

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }
