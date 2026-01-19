package engine

import (
	"context"
	"encoding/json"
	"time"
)

type eventingRateLimiter struct {
	inner    RateLimiter
	emit     func(p ThrottledPayload)
	tenantID string
	topic    string
	provider string
	kind     RateLimitKind
}

func (e *eventingRateLimiter) Decide(ctx context.Context, req RateLimitRequest) (RateLimitDecision, error) {
	dec, err := e.inner.Decide(ctx, req)
	if err != nil {
		// We can optionally emit an event on limiter error, but I'd keep it quiet for now
		return dec, err
	}

	if !dec.Allowed {
		p := ThrottledPayload{
			Reason:       "provider_rate_limit",
			Scope:        ThrottleScopeTenantTopic, // reuse scope, payload also includes provider/kind in Reason+Meta below
			TenantID:     req.TenantID,
			Topic:        e.topic,
			Limit:        0,
			Inflight:     0,
			RetryAfterMS: dec.RetryAfter.Milliseconds(),
		}

		// Sneak extra info into Reason string; or if you prefer, extend ThrottledPayload struct later
		// Keeping it simple for v1:
		if req.Provider != "" || req.Kind != "" || dec.Reason != "" {
			p.Reason = "provider_rate_limit:" + string(req.Kind) + ":" + req.Provider + ":" + dec.Reason
		}

		e.emit(p)
	}

	return dec, nil
}

// helper used by runner goroutine injection
func (r *Runner) wrapRateLimiter(runID, wfID, nodeID string, attempt int, tenantID, topic string) RateLimiter {
	inner := r.getRateLimiter()
	if inner == nil {
		return nil
	}

	return &eventingRateLimiter{
		inner:    inner,
		tenantID: tenantID,
		topic:    topic,
		emit: func(p ThrottledPayload) {
			// best-effort (never block node execution)
			b, _ := json.Marshal(p)
			_, _ = r.store.AppendEvent(RunEvent{
				RunID:      runID,
				Type:       EventThrottled,
				WorkflowID: wfID,
				NodeID:     nodeID,
				Attempt:    attempt,
				Payload:    b,
			})
		},
	}
}

func ThrottleDelay(retryAfter time.Duration, reason string) error {
	if retryAfter <= 0 {
		retryAfter = 250 * time.Millisecond
	}

	if reason == "" {
		reason = "throttled"
	}
	return Delay(retryAfter, reason)
}
