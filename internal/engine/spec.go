package engine

import "encoding/json"

// This is minimal v2.2 (will wire it in v2.3 or v2.4)
type RetryPolicy struct {
	MaxAttempts int `json:"max_attempts" yaml:"max_attempts"`
}

type NodeSpec struct {
	ID     string          `json:"id" yaml:"id"`
	Topic  string          `json:"topic,omitempty" yaml:"topic,omitempty"` // step -> topic mapping
	Deps   []string        `json:"deps,omitempty" yaml:"deps,omitempty"`
	Input  json.RawMessage `json:"input_schema,omitempty" yaml:"input_schema,omitempty"`
	Output json.RawMessage `json:"output_schema,omitempty" yaml:"output_schema,omitempty"`
	Retry  RetryPolicy     `json:"retry,omitempty" yaml:"retry,omitempty"`
}

type WorkflowSpec struct {
	ID    string     `json:"id" yaml:"id"`
	Nodes []NodeSpec `json:"nodes" yaml:"nodes"`
}
