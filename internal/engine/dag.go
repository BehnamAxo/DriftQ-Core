package engine

import (
	"errors"
)

type NodeEdge struct {
	From string
	To   string
}

// Represents a DAG
type WorkflowGraph struct {
	ID        string     `json:"id" yaml:"id"`
	Nodes     []NodeDef  `json:"nodes" yaml:"nodes"`
	Edges     []NodeEdge `json:"edges" yaml:"edges"`
	TimeoutMS int        `json:"timeout_ms,omitempty" yaml:"timeout_ms,omitempty"`
}

func (g *WorkflowGraph) Validate() error {
	nodeSet := map[string]bool{}
	for _, n := range g.Nodes {
		if n.NodeID == "" {
			return errors.New("node has empty id")
		}

		if nodeSet[n.NodeID] {
			return errors.New("duplicate node id: " + n.NodeID)
		}

		nodeSet[n.NodeID] = true
	}

	for _, e := range g.Edges {
		if !nodeSet[e.From] {
			return errors.New("edge references unknown node: " + e.From)
		}

		if !nodeSet[e.To] {
			return errors.New("edge references unknown node: " + e.To)
		}
	}

	if hasCycle(g) {
		return errors.New("workflow graph contains a cycle")
	}
	return nil
}

func (g *WorkflowGraph) TopologicalOrder() ([]NodeDef, error) {
	inDegree := map[string]int{}
	children := map[string][]string{}

	for _, n := range g.Nodes {
		inDegree[n.NodeID] = 0
	}

	for _, e := range g.Edges {
		children[e.From] = append(children[e.From], e.To)
		inDegree[e.To]++
	}

	var queue []string
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	var ordered []NodeDef
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]

		for _, n := range g.Nodes {
			if n.NodeID == id {
				ordered = append(ordered, n)
				break
			}
		}

		for _, child := range children[id] {
			inDegree[child]--
			if inDegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}

	if len(ordered) != len(g.Nodes) {
		return nil, errors.New("graph has cycles or disconnected nodes")
	}

	return ordered, nil
}

// Detect cycle using DFS. Like this a lot
func hasCycle(g *WorkflowGraph) bool {
	visited := map[string]bool{}
	stack := map[string]bool{}

	var dfs func(string) bool
	children := map[string][]string{}
	for _, e := range g.Edges {
		children[e.From] = append(children[e.From], e.To)
	}

	dfs = func(node string) bool {
		if stack[node] {
			return true
		}

		if visited[node] {
			return false
		}

		visited[node] = true
		stack[node] = true
		for _, child := range children[node] {
			if dfs(child) {
				return true
			}
		}

		stack[node] = false
		return false
	}

	for _, n := range g.Nodes {
		if dfs(n.NodeID) {
			return true
		}
	}
	return false
}
