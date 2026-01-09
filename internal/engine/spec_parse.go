package engine

import (
	"encoding/json"
	"errors"
)

var ErrSpecInvalid = errors.New("invalid workflow spec")

func ParseWorkflowSpecJSON(b []byte) (WorkflowGraph, WorkflowSpec, error) {
	var spec WorkflowSpec
	if err := json.Unmarshal(b, &spec); err != nil {
		return WorkflowGraph{}, WorkflowSpec{}, err
	}

	if spec.ID == "" {
		return WorkflowGraph{}, WorkflowSpec{}, errors.New("spec id is required")
	}

	if len(spec.Nodes) == 0 {
		return WorkflowGraph{}, WorkflowSpec{}, errors.New("spec must have nodes")
	}

	// Build graph
	g := WorkflowGraph{ID: spec.ID}

	seen := map[string]bool{}
	for _, ns := range spec.Nodes {
		if ns.ID == "" {
			return WorkflowGraph{}, WorkflowSpec{}, errors.New("node id is required")
		}
		if seen[ns.ID] {
			return WorkflowGraph{}, WorkflowSpec{}, errors.New("duplicate node id: " + ns.ID)
		}
		seen[ns.ID] = true

		// Note to myself that NodeDef.Run is wired later via topic/handler registry
		g.Nodes = append(g.Nodes, NodeDef{NodeID: ns.ID})
		for _, dep := range ns.Deps {
			g.Edges = append(g.Edges, NodeEdge{From: dep, To: ns.ID})
		}
	}

	if err := g.Validate(); err != nil {
		return WorkflowGraph{}, WorkflowSpec{}, err
	}

	return g, spec, nil
}
