package engine

import "context"

type UsageDelta struct {
	Tokens  int64
	Dollars float64
}

type usageSinkKey struct{}
type UsageSink func(UsageDelta)

func WithUsageSink(ctx context.Context, sink UsageSink) context.Context {
	if sink == nil {
		return ctx
	}

	return context.WithValue(ctx, usageSinkKey{}, sink)
}

func ReportUsage(ctx context.Context, tokens int64, dollars float64) {
	v := ctx.Value(usageSinkKey{})
	sink, ok := v.(UsageSink)

	if !ok || sink == nil {
		return
	}

	sink(UsageDelta{Tokens: tokens, Dollars: dollars})
}

type rateLimiterKey struct{}

func WithRateLimiter(ctx context.Context, rl RateLimiter) context.Context {
	if rl == nil {
		return ctx
	}

	return context.WithValue(ctx, rateLimiterKey{}, rl)
}

func RateLimiterFrom(ctx context.Context) RateLimiter {
	v := ctx.Value(rateLimiterKey{})
	rl, _ := v.(RateLimiter)

	return rl
}
