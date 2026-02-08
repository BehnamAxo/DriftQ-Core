package engine

import (
	"encoding/json"
	"fmt"
	"time"
)

// NodeFinishedProofFields are injected into EventNodeFinished payloads so the demo
// can *prove* time-travel/replay-cache behavior and show timing breakdowns.
type NodeFinishedProofFields struct {
	UsedCachedOutput bool
	CachedAttempt    int

	QueuedAt  *time.Time
	StartedAt *time.Time
	EndedAt   *time.Time

	QueueMS  int64
	WorkerMS int64
}

// InjectNodeFinishedProofFields merges proof-grade fields into an existing NodeFinishedPayload JSON blob.
// It preserves Output/ArtifactRef/ArtifactMeta (and any future fields on NodeFinishedPayload).
func InjectNodeFinishedProofFields(payload json.RawMessage, f NodeFinishedProofFields) (json.RawMessage, error) {
	var p NodeFinishedPayload
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &p); err != nil {
			return nil, fmt.Errorf("unmarshal node finished payload: %w", err)
		}
	}

	// Inject/overwrite proof fields
	p.UsedCachedOutput = f.UsedCachedOutput
	if f.UsedCachedOutput {
		p.CachedAttempt = f.CachedAttempt
	}

	p.QueuedAt = f.QueuedAt
	p.StartedAt = f.StartedAt
	p.EndedAt = f.EndedAt
	p.QueueMS = f.QueueMS
	p.WorkerMS = f.WorkerMS

	b, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal node finished payload: %w", err)
	}
	return b, nil
}
