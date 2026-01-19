package engine

import (
	"time"
)

type ThrottleScope string

const (
	ThrottleScopeTopic       ThrottleScope = "topic"
	ThrottleScopeTenantTopic ThrottleScope = "tenant_topic"
)

type ThrottledPayload struct {
	Reason       string        `json:"reason"`
	Scope        ThrottleScope `json:"scope"`
	TenantID     string        `json:"tenant_id,omitempty"`
	Topic        string        `json:"topic,omitempty"`
	Limit        int           `json:"limit"`
	Inflight     int           `json:"inflight"`
	RetryAfterMS int64         `json:"retry_after_ms,omitempty"`
}

func (r *Runner) capKey(scope ThrottleScope, tenantID, topic string) string {
	switch scope {
	case ThrottleScopeTenantTopic:
		return "tenant:" + tenantID + "|topic:" + topic
	default:
		return "topic:" + topic
	}
}

func (r *Runner) concurrencyCapFor(tenantID, topic string) (int, ThrottleScope, string) {
	if topic == "" {
		return 0, "", ""
	}

	r.throttleMu.Lock()
	defer r.throttleMu.Unlock()

	// tenant-specific overrides topic cap if present
	if tenantID != "" {
		if m := r.tenantTopicCaps[tenantID]; m != nil {
			if cap, ok := m[topic]; ok && cap > 0 {
				return cap, ThrottleScopeTenantTopic, r.capKey(ThrottleScopeTenantTopic, tenantID, topic)
			}
		}
	}

	if cap, ok := r.topicCaps[topic]; ok && cap > 0 {
		return cap, ThrottleScopeTopic, r.capKey(ThrottleScopeTopic, "", topic)
	}

	return 0, "", ""
}

func (r *Runner) tryAcquireCap(key string, cap int) (ok bool, inflight int) {
	if key == "" || cap <= 0 {
		return true, 0
	}

	r.throttleMu.Lock()
	defer r.throttleMu.Unlock()

	cur := r.inflightCaps[key]
	if cur >= cap {
		return false, cur
	}

	r.inflightCaps[key] = cur + 1
	return true, cur + 1
}

func (r *Runner) releaseCap(key string) {
	if key == "" {
		return
	}

	r.throttleMu.Lock()
	defer r.throttleMu.Unlock()

	cur := r.inflightCaps[key]
	if cur <= 1 {
		delete(r.inflightCaps, key)
		return
	}

	r.inflightCaps[key] = cur - 1
}

func shouldEmitThrottle(last map[string]time.Time, key string, now time.Time, every time.Duration) bool {
	if key == "" {
		return false
	}

	if t, ok := last[key]; ok && now.Sub(t) < every {
		return false
	}

	last[key] = now
	return true
}
