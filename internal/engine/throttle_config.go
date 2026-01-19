package engine

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
