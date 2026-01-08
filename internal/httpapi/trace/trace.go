package trace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
)

type key struct{}

const Header = "X-Trace-Id"

func NewID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func WithID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, key{}, id)
}

func From(ctx context.Context) string {
	if v := ctx.Value(key{}); v != nil {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	return ""
}

// Returns header trace id if present
func FromRequest(r *http.Request) string {
	return r.Header.Get(Header)
}
