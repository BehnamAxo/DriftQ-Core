package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type RunStatus string
type NodeStatus string
type RunEventType string

type Run struct {
	RunID      string    `json:"run_id"`
	WorkflowID string    `json:"workflow_id"`
	Status     RunStatus `json:"status"`

	StartedAt *time.Time `json:"started_at,omitempty"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
}

type RunEvent struct {
	RunID string       `json:"run_id"`
	Seq   int64        `json:"seq"`
	Type  RunEventType `json:"type"`
	At    time.Time    `json:"at"`

	// Optional for now
	WorkflowID string `json:"workflow_id,omitempty"`
	NodeID     string `json:"node_id,omitempty"`
	Attempt    int    `json:"attempt,omitempty"`

	// Optional for now
	Payload json.RawMessage `json:"payload,omitempty"`
}

type NodeExecution struct {
	RunID      string     `json:"run_id"`
	WorkflowID string     `json:"workflow_id"`
	NodeID     string     `json:"node_id"`
	Attempt    int        `json:"attempt"`
	Status     NodeStatus `json:"status"`

	StartedAt *time.Time `json:"started_at,omitempty"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`

	Input  json.RawMessage `json:"input,omitempty"`
	Output json.RawMessage `json:"output,omitempty"`

	Error string `json:"error,omitempty"`
}

const (
	RunStatusQueued    RunStatus = "queued"
	RunStatusRunning   RunStatus = "running"
	RunStatusSucceeded RunStatus = "succeeded"
	RunStatusFailed    RunStatus = "failed"
	RunStatusCanceled  RunStatus = "canceled"
)

const (
	NodeStatusQueued    NodeStatus = "queued"
	NodeStatusRunning   NodeStatus = "running"
	NodeStatusSucceeded NodeStatus = "succeeded"
	NodeStatusFailed    NodeStatus = "failed"
	NodeStatusCanceled  NodeStatus = "canceled"
)

const (
	EventRunCreated       RunEventType = "run_created"
	EventRunStarted       RunEventType = "run_started"
	EventRunFinished      RunEventType = "run_finished"
	EventNodeStarted      RunEventType = "node_started"
	EventNodeFinished     RunEventType = "node_finished"
	EventNodeFailed       RunEventType = "node_failed"
	EventNodeRetryPlanned RunEventType = "node_retry_planned"
)

func (s RunStatus) Valid() bool {
	switch s {
	case RunStatusQueued, RunStatusRunning, RunStatusSucceeded, RunStatusFailed, RunStatusCanceled:
		return true

	default:
		return false
	}
}

func (s NodeStatus) Valid() bool {
	switch s {
	case NodeStatusQueued, NodeStatusRunning, NodeStatusSucceeded, NodeStatusFailed, NodeStatusCanceled:
		return true
	default:
		return false
	}
}

func (r Run) Validate() error {
	if r.RunID == "" {
		return errors.New("run_id is required")
	}

	if r.WorkflowID == "" {
		return errors.New("workflow_id is required")
	}

	if !r.Status.Valid() {
		return fmt.Errorf("invalid run status: %q", r.Status)
	}

	// For now considering StartedAt and EndedAt both optional
	return nil
}

func (n NodeExecution) Validate() error {
	if n.RunID == "" {
		return errors.New("run_id is required")
	}

	if n.WorkflowID == "" {
		return errors.New("workflow_id is required")
	}

	if n.NodeID == "" {
		return errors.New("node_id is required")
	}

	if n.Attempt < 1 {
		return errors.New("attempt must be >= 1")
	}

	if !n.Status.Valid() {
		return fmt.Errorf("invalid node status: %q", n.Status)
	}

	return nil
}

func (e RunEvent) Validate() error {
	if e.RunID == "" {
		return errors.New("run_id is required")
	}

	if e.Seq < 1 {
		return errors.New("seq must be >= 1")
	}

	if e.Type == "" {
		return errors.New("type is required")
	}

	if e.At.IsZero() {
		return errors.New("at is required")
	}

	return nil
}
