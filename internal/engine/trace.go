package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

type traceKey struct{}

func NewTraceID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceKey{}, traceID)
}

func TraceIDFrom(ctx context.Context) string {
	if v := ctx.Value(traceKey{}); v != nil {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}

	return ""
}
