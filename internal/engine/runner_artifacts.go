package engine

import (
	"context"
	"encoding/json"
	"strings"
)

func (r *Runner) buildNodeFinishedPayload(ctx context.Context, runID, workflowID, nodeID string, attempt int, out json.RawMessage) (json.RawMessage, error) {
	_ = strings.TrimSpace(runID)
	_ = strings.TrimSpace(workflowID)
	_ = strings.TrimSpace(nodeID)
	_ = attempt // kept for future when ArtifactMeta grows

	limit := r.GetArtifactInlineLimit()

	if len(out) == 0 {
		return json.Marshal(NodeFinishedPayload{Output: out})
	}

	// Inline if under limit (limit == 0 means "never inline")
	if limit > 0 && len(out) <= limit {
		return json.Marshal(NodeFinishedPayload{Output: cloneRaw(out)})
	}

	ref, meta, err := r.PutArtifact(ctx, out, ArtifactMeta{
		ContentType: "application/json",

		RunID:      runID,
		WorkflowID: workflowID,
		NodeID:     nodeID,
		Attempt:    attempt,

		Labels: map[string]string{"kind": "step_output"},
	})

	if err != nil {
		return nil, err
	}

	return json.Marshal(NodeFinishedPayload{
		ArtifactRef:  &ref,
		ArtifactMeta: &meta,
	})
}

func (r *Runner) getArtifactStore() (ArtifactStore, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.artifacts == nil {
		return nil, ErrArtifactStoreUnset
	}

	return r.artifacts, nil
}

func (r *Runner) GetArtifactInlineLimit() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.artifactInlineLimit < 0 {
		return 0
	}

	return r.artifactInlineLimit
}

func (r *Runner) PutArtifact(ctx context.Context, data []byte, meta ArtifactMeta) (ArtifactRef, ArtifactMeta, error) {
	s, err := r.getArtifactStore()
	if err != nil {
		return ArtifactRef{}, ArtifactMeta{}, err
	}

	return s.Put(ctx, data, meta)
}

// Convenience wrapper for debug/demo endpoints stuff (and CLI-friendly artifacts)
func (r *Runner) PutArtifactWithContentType(ctx context.Context, data []byte, contentType string) (ArtifactRef, ArtifactMeta, error) {
	ct := strings.TrimSpace(contentType)
	if ct == "" {
		ct = "application/octet-stream"
	}

	return r.PutArtifact(ctx, data, ArtifactMeta{
		ContentType: ct,
	})
}

func (r *Runner) GetArtifact(ctx context.Context, artifactID string) ([]byte, ArtifactMeta, error) {
	s, err := r.getArtifactStore()
	if err != nil {
		return nil, ArtifactMeta{}, err
	}

	return s.Get(ctx, strings.TrimSpace(artifactID))
}

func (r *Runner) DeleteArtifact(ctx context.Context, artifactID string) error {
	s, err := r.getArtifactStore()
	if err != nil {
		return err
	}

	return s.Delete(ctx, strings.TrimSpace(artifactID))
}
