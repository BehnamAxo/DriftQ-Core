package engine

import (
	"context"
	"errors"
	"time"
)

var ErrBudgetExceeded = errors.New("budget exceeded")

// BudgetPolicy is a best-effort safety guardrail for a run
//
// v2.7 scope:
//   - wall-clock timeout and max attempts are enforced inside the runner
//   - token/dollar accounting is best-effort (reported/recorded if handlers provide usage)
//
// NOTE: This is intentionally simple and in-memory for now!!
type BudgetPolicy struct {
	MaxTokens  int64   `json:"max_tokens,omitempty" yaml:"max_tokens,omitempty"`
	MaxDollars float64 `json:"max_dollars,omitempty" yaml:"max_dollars,omitempty"`

	// WallClockTimeoutMS is an overall run timeout (milliseconds)
	WallClockTimeoutMS int64 `json:"wall_clock_timeout_ms,omitempty" yaml:"wall_clock_timeout_ms,omitempty"`

	// MaxAttempts is a ceiling on total step attempts across the run
	MaxAttempts int `json:"max_attempts,omitempty" yaml:"max_attempts,omitempty"`
}

func (p BudgetPolicy) WallClockTimeout() time.Duration {
	if p.WallClockTimeoutMS <= 0 {
		return 0
	}

	return time.Duration(p.WallClockTimeoutMS) * time.Millisecond
}

// BudgetUsage is a snapshot of what we think the run has spent
//
// Token/dollar values are best-effort and only incremented when handlers emit usage
type BudgetUsage struct {
	Tokens    int64   `json:"tokens,omitempty"`
	Dollars   float64 `json:"dollars,omitempty"`
	Attempts  int     `json:"attempts,omitempty"`
	WallClock int64   `json:"wall_clock_ms,omitempty"`
}

type BudgetScope string
type BudgetReason string

const (
	BudgetScopeRun    BudgetScope = "run"
	BudgetScopeTenant BudgetScope = "tenant"

	BudgetReasonTokens      BudgetReason = "tokens"
	BudgetReasonDollars     BudgetReason = "dollars"
	BudgetReasonWallClock   BudgetReason = "wall_clock_timeout"
	BudgetReasonMaxAttempts BudgetReason = "max_attempts"
)

// BudgetExceededPayload is stored in RunEvent payloads and/or Run.TerminalMeta
type BudgetExceededPayload struct {
	Scope  BudgetScope  `json:"scope"`
	Reason BudgetReason `json:"reason"`

	Limit any `json:"limit,omitempty"`
	Used  any `json:"used,omitempty"`
}

type RateLimitKind string

const (
	RateLimitLLM  RateLimitKind = "llm"
	RateLimitTool RateLimitKind = "tool"
)

type RateLimitRequest struct {
	TenantID string
	Provider string
	Kind     RateLimitKind

	// Best-effort hints (optional)
	EstimatedTokens  int64
	EstimatedDollars float64
}

type RateLimitDecision struct {
	Allowed    bool
	RetryAfter time.Duration
	Reason     string
}

// RateLimiter can be consulted by handlers (LLM calls, tool calls, embeddings, etc)
// before making an expensive or rate-limited request
//
// The runner will also use this later for per-topic concurrency caps
type RateLimiter interface {
	Decide(ctx context.Context, req RateLimitRequest) (RateLimitDecision, error)
}

// WithTenantID / TenantIDFrom are helpers so the runtime and handlers can share tenant context

type ctxKeyTenantID struct{}

func WithTenantID(ctx context.Context, tenantID string) context.Context {
	if tenantID == "" {
		return ctx
	}

	return context.WithValue(ctx, ctxKeyTenantID{}, tenantID)
}

func TenantIDFrom(ctx context.Context) string {
	v := ctx.Value(ctxKeyTenantID{})
	if s, ok := v.(string); ok {
		return s
	}

	return ""
}

func effectiveBudget(def BudgetPolicy, tenant BudgetPolicy, perRun BudgetPolicy) BudgetPolicy {
	return BudgetPolicy{
		MaxTokens:          minPosI64(def.MaxTokens, tenant.MaxTokens, perRun.MaxTokens),
		MaxDollars:         minPosF64(def.MaxDollars, tenant.MaxDollars, perRun.MaxDollars),
		WallClockTimeoutMS: minPosI64(def.WallClockTimeoutMS, tenant.WallClockTimeoutMS, perRun.WallClockTimeoutMS),
		MaxAttempts:        minPosInt(def.MaxAttempts, tenant.MaxAttempts, perRun.MaxAttempts),
	}
}

func minPosI64(vals ...int64) int64 {
	var out int64
	for _, v := range vals {
		if v <= 0 {
			continue
		}

		if out == 0 || v < out {
			out = v
		}
	}

	return out
}

func minPosF64(vals ...float64) float64 {
	var out float64
	for _, v := range vals {
		if v <= 0 {
			continue
		}
		if out == 0 || v < out {
			out = v
		}
	}

	return out
}

func minPosInt(vals ...int) int {
	var out int
	for _, v := range vals {
		if v <= 0 {
			continue
		}

		if out == 0 || v < out {
			out = v
		}
	}

	return out
}

func WithDefaultBudget(p BudgetPolicy) RunnerOption {
	return func(r *Runner) {
		r.defaultRunBudget = p
	}
}
