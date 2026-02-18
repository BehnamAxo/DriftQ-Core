package engine

import "context"

type artifactStoreCtxKey struct{}

func WithArtifactStoreContext(ctx context.Context, s ArtifactStore) context.Context {
	if s == nil {
		return ctx
	}

	return context.WithValue(ctx, artifactStoreCtxKey{}, s)
}

func ArtifactStoreFrom(ctx context.Context) ArtifactStore {
	v := ctx.Value(artifactStoreCtxKey{})
	if v == nil {
		return nil
	}

	s, _ := v.(ArtifactStore)
	return s
}
