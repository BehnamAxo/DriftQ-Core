package engine

import (
	"encoding/json"
	"testing"
	"time"
)

func TestRunValidate(t *testing.T) {
	r := Run{RunID: "r1", WorkflowID: "wf1", Status: RunStatusQueued}
	if err := r.Validate(); err != nil {
		t.Fatalf("expected valid run, got err: %v", err)
	}

	r.Status = "weird"
	if err := r.Validate(); err == nil {
		t.Fatalf("expected invalid status error")
	}
}

func TestNodeExecutionValidate(t *testing.T) {
	n := NodeExecution{
		RunID:      "r1",
		WorkflowID: "wf1",
		NodeID:     "nodeA",
		Attempt:    1,
		Status:     NodeStatusRunning,
	}

	if err := n.Validate(); err != nil {
		t.Fatalf("expected valid node exec, got err: %v", err)
	}

	n.Attempt = 0
	if err := n.Validate(); err == nil {
		t.Fatalf("expected attempt validation error")
	}
}

func TestRunEventValidateAndJSON(t *testing.T) {
	now := time.Now().UTC()
	e := RunEvent{
		RunID: "r1",
		Seq:   1,
		Type:  EventRunCreated,
		At:    now,
	}

	if err := e.Validate(); err != nil {
		t.Fatalf("expected valid event, got err: %v", err)
	}

	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	s := string(b)
	if !contains(s, `"run_id":"r1"`) {
		t.Fatalf("expected run_id in json, got: %s", s)
	}

	if !contains(s, `"seq":1`) {
		t.Fatalf("expected seq in json, got: %s", s)
	}

	if !contains(s, `"type":"run_created"`) {
		t.Fatalf("expected type in json, got: %s", s)
	}
}

func contains(haystack, needle string) bool {
	return len(needle) == 0 || (len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})())
}
