package engine

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
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
		if r.obs != nil {
			r.obs.observeArtifact("inline", ArtifactMeta{
				ContentType: "application/json",
				RunID:       runID,
				WorkflowID:  workflowID,
				NodeID:      nodeID,
				Attempt:     attempt,
			}, int64(len(out)), 0)
		}
		return json.Marshal(NodeFinishedPayload{Output: cloneRaw(out)})
	}

	ref, meta, err := r.PutArtifact(ctx, out, ArtifactMeta{
		ContentType: "application/json",

		TenantID:   effectiveTenantFromContext(ctx),
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

	ctx, span := r.startSpan(ctx, "driftq.artifact.put",
		attribute.String("driftq.workflow_id", strings.TrimSpace(meta.WorkflowID)),
		attribute.String("driftq.node_id", strings.TrimSpace(meta.NodeID)),
		attribute.String("driftq.content_type", strings.TrimSpace(meta.ContentType)),
	)
	start := time.Now()
	ref, storedMeta, err := s.Put(ctx, data, meta)

	if r.obs != nil {
		r.obs.observeArtifact("put", storedMeta, int64(len(data)), time.Since(start))
	}

	r.finishSpan(span, err,
		attribute.String("driftq.artifact_id", strings.TrimSpace(storedMeta.ArtifactID)),
		attribute.Int64("driftq.artifact_size", int64(len(data))),
	)
	return ref, storedMeta, err
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

	ctx, span := r.startSpan(ctx, "driftq.artifact.get",
		attribute.String("driftq.artifact_id", strings.TrimSpace(artifactID)),
	)

	start := time.Now()
	data, meta, err := s.Get(ctx, strings.TrimSpace(artifactID))
	if err != nil {
		r.finishSpan(span, err)
		return nil, ArtifactMeta{}, err
	}

	if err := r.ensureArtifactTenantAccess(ctx, meta, "artifact.get"); err != nil {
		r.finishSpan(span, err)
		return nil, ArtifactMeta{}, err
	}

	if r.obs != nil {
		r.obs.observeArtifact("get", meta, int64(len(data)), time.Since(start))
	}

	r.finishSpan(span, nil,
		attribute.String("driftq.workflow_id", strings.TrimSpace(meta.WorkflowID)),
		attribute.String("driftq.node_id", strings.TrimSpace(meta.NodeID)),
		attribute.String("driftq.content_type", strings.TrimSpace(meta.ContentType)),
		attribute.Int64("driftq.artifact_size", int64(len(data))),
	)

	return data, meta, nil
}

func (r *Runner) DeleteArtifact(ctx context.Context, artifactID string) error {
	s, err := r.getArtifactStore()
	if err != nil {
		return err
	}

	artifactID = strings.TrimSpace(artifactID)
	ctx, span := r.startSpan(ctx, "driftq.artifact.delete",
		attribute.String("driftq.artifact_id", strings.TrimSpace(artifactID)),
	)

	start := time.Now()
	_, meta, err := s.Get(ctx, artifactID)
	if err != nil {
		r.finishSpan(span, err)
		return err
	}

	if err := r.ensureArtifactTenantAccess(ctx, meta, "artifact.delete"); err != nil {
		r.finishSpan(span, err)
		return err
	}

	err = s.Delete(ctx, artifactID)
	if r.obs != nil {
		r.obs.observeArtifact("delete", meta, meta.Size, time.Since(start))
	}

	r.finishSpan(span, err,
		attribute.String("driftq.workflow_id", strings.TrimSpace(meta.WorkflowID)),
		attribute.String("driftq.node_id", strings.TrimSpace(meta.NodeID)),
		attribute.String("driftq.content_type", strings.TrimSpace(meta.ContentType)),
		attribute.Int64("driftq.artifact_size", meta.Size),
	)
	return err
}
