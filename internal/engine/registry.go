package engine

import (
	"fmt"
	"strings"
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
	capabilityByNode := map[string]string{}
	humanByNode := map[string]*HumanStepSpec{}
	for _, n := range spec.Nodes {
		topicByNode[n.ID] = n.Topic
		capabilityByNode[n.ID] = strings.TrimSpace(n.Capability)
		humanByNode[n.ID] = cloneHumanStepSpec(n.Human)
	}

	out := WorkflowGraph{ID: g.ID, Edges: append([]NodeEdge(nil), g.Edges...)}
	for _, nd := range g.Nodes {
		topic := topicByNode[nd.NodeID]
		human := humanByNode[nd.NodeID]
		if topic == "" && human == nil {
			return WorkflowGraph{}, fmt.Errorf("node %s missing topic", nd.NodeID)
		}
		if human != nil {
			if err := human.NormalizeAndValidate(); err != nil {
				return WorkflowGraph{}, fmt.Errorf("node %s invalid human config: %w", nd.NodeID, err)
			}
		}

		var fn NodeFunc
		if topic != "" {
			var ok bool
			fn, ok = reg.Lookup(topic)
			if !ok {
				return WorkflowGraph{}, fmt.Errorf("no handler registered for topic %q (node %s)", topic, nd.NodeID)
			}
		}

		out.Nodes = append(out.Nodes, NodeDef{
			NodeID:             nd.NodeID,
			Topic:              topic,
			RequiredCapability: capabilityByNode[nd.NodeID],
			Human:              human,
			Run:                fn,
		})
	}

	if err := out.Validate(); err != nil {
		return WorkflowGraph{}, err
	}

	return out, nil
}
