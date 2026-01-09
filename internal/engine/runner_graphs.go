package engine

func (r *Runner) rememberGraph(id string, g WorkflowGraph) {
	if id == "" {
		return
	}

	g.ID = id
	r.mu.Lock()
	r.graphs[id] = g
	r.mu.Unlock()
}

func (r *Runner) getGraph(id string) (WorkflowGraph, bool) {
	r.mu.RLock()
	g, ok := r.graphs[id]
	r.mu.RUnlock()
	return g, ok
}
