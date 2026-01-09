package engine

import (
	"context"
	"encoding/json"
)

func (r *Runner) RunSpecJSON(ctx context.Context, runID string, specJSON []byte, reg *HandlerRegistry, initialInput json.RawMessage) error {
	g, spec, err := ParseWorkflowSpecJSON(specJSON)
	if err != nil {
		return err
	}

	exec, err := CompileSpecToExecutable(spec, g, reg)
	if err != nil {
		return err
	}

	return r.runDAG(ctx, runID, exec, initialInput, json.RawMessage(specJSON))
}
