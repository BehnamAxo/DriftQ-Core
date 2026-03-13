package engine

import (
	"context"
	"encoding/json"
)

func (r *Runner) RunSpecJSON(ctx context.Context, runID string, specJSON []byte, reg *HandlerRegistry, initialInput json.RawMessage) error {
	tenantID := effectiveTenantFromContext(ctx)
	if reg == nil {
		reg = r.HandlerRegistryForTenant(tenantID)
	} else if r.HandlerRegistryForTenant(tenantID) == nil {
		if tenantID != "" {
			r.SetTenantHandlerRegistry(tenantID, reg)
		} else {
			r.SetHandlerRegistry(reg)
		}
	}

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
