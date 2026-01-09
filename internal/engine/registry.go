package engine

import (
	"fmt"
)

type HandlerRegistry struct {
	byTopic map[string]NodeFunc
}

func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{byTopic: map[string]NodeFunc{}}
}

func (r *HandlerRegistry) Register(topic string, fn NodeFunc) {
	if topic == "" || fn == nil {
		return
	}

	r.byTopic[topic] = fn
}

func (r *HandlerRegistry) Lookup(topic string) (NodeFunc, bool) {
	fn, ok := r.byTopic[topic]
	return fn, ok
}

func CompileSpecToExecutable(spec WorkflowSpec, g WorkflowGraph, reg *HandlerRegistry) (WorkflowGraph, error) {
	if reg == nil {
		return WorkflowGraph{}, fmt.Errorf("handler registry is required")
	}

	topicByNode := map[string]string{}
	for _, n := range spec.Nodes {
		topicByNode[n.ID] = n.Topic
	}

	out := WorkflowGraph{ID: g.ID, Edges: append([]NodeEdge(nil), g.Edges...)}
	for _, nd := range g.Nodes {
		topic := topicByNode[nd.NodeID]
		if topic == "" {
			return WorkflowGraph{}, fmt.Errorf("node %s missing topic", nd.NodeID)
		}

		fn, ok := reg.Lookup(topic)
		if !ok {
			return WorkflowGraph{}, fmt.Errorf("no handler registered for topic %q (node %s)", topic, nd.NodeID)
		}
		out.Nodes = append(out.Nodes, NodeDef{
			NodeID: nd.NodeID,
			Run:    fn,
		})
	}

	if err := out.Validate(); err != nil {
		return WorkflowGraph{}, err
	}

	return out, nil
}
