package engine

import "strings"

// v2.7: caps are GLOBAL across the runner (across runs)
// That’s the whole point: prevent "500 parallel embeddings" accidents

func (r *Runner) SetTopicConcurrencyCap(topic string, cap int) {
	if topic == "" || cap < 0 {
		return
	}

	r.throttleMu.Lock()
	defer r.throttleMu.Unlock()
	if cap == 0 {
		delete(r.topicCaps, topic)
		return
	}

	r.topicCaps[topic] = cap
}

func (r *Runner) SetTenantTopicConcurrencyCap(tenantID, topic string, cap int) {
	if tenantID == "" || topic == "" || cap < 0 {
		return
	}

	r.throttleMu.Lock()
	defer r.throttleMu.Unlock()
	m := r.tenantTopicCaps[tenantID]

	if m == nil {
		m = make(map[string]int)
		r.tenantTopicCaps[tenantID] = m
	}

	if cap == 0 {
		delete(m, topic)
		if len(m) == 0 {
			delete(r.tenantTopicCaps, tenantID)
		}

		return
	}

	m[topic] = cap
}

// This is a convenience config for wiring caps at Runner construction time
type ThrottleConfig struct {
	// Global per-topic caps (across all tenants)
	TopicCaps map[string]int

	// Tenant/topic caps keyed as "tenant:topic".
	// Special tenant "*" means: treat it as global topic cap.
	// Example: "*:expensive" => global cap for topic "expensive".
	TenantTopicCaps map[string]int
}

func WithThrottleConfig(cfg ThrottleConfig) RunnerOption {
	return func(r *Runner) {
		// Global caps
		for topic, cap := range cfg.TopicCaps {
			r.SetTopicConcurrencyCap(topic, cap)
		}

		// Tenant/topic caps
		for k, cap := range cfg.TenantTopicCaps {
			tenant, topic := parseTenantTopicKey(k)
			if topic == "" {
				continue
			}

			// "*" => treat as global cap for that topic
			if tenant == "*" || tenant == "" {
				r.SetTopicConcurrencyCap(topic, cap)
				continue
			}

			r.SetTenantTopicConcurrencyCap(tenant, topic, cap)
		}
	}
}

func parseTenantTopicKey(s string) (tenant string, topic string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}

	// Prefer "tenant:topic"
	if parts := strings.SplitN(s, ":", 2); len(parts) == 2 {
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	}

	// Fallback: treat entire string as topic
	return "*", s
}
