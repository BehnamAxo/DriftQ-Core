package engine

func (r *Runner) SetDefaultRunBudget(p BudgetPolicy) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.defaultRunBudget = p
}

func (r *Runner) SetTenantBudget(tenantID string, p BudgetPolicy) {
	if tenantID == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tenantBudgets == nil {
		r.tenantBudgets = make(map[string]BudgetPolicy)
	}

	r.tenantBudgets[tenantID] = p
}

func (r *Runner) SetRateLimiter(rl RateLimiter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rateLimiter = rl
}

func (r *Runner) getTenantBudget(tenantID string) (BudgetPolicy, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.tenantBudgets == nil || tenantID == "" {
		return BudgetPolicy{}, false
	}

	p, ok := r.tenantBudgets[tenantID]
	return p, ok
}

func (r *Runner) getDefaultRunBudget() BudgetPolicy {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.defaultRunBudget
}

func (r *Runner) getRateLimiter() RateLimiter {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.rateLimiter
}
