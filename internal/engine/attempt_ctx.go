package engine

import "context"

type ctxKeyAttempt struct{}

func WithAttempt(ctx context.Context, attempt int) context.Context {
	return context.WithValue(ctx, ctxKeyAttempt{}, attempt)
}

func AttemptFrom(ctx context.Context) int {
	v := ctx.Value(ctxKeyAttempt{})
	if n, ok := v.(int); ok {
		return n
	}
	return 0
}
