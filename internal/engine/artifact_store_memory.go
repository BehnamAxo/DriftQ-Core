package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

// MemoryArtifactStore is a tiny in-memory ArtifactStore for local debugging
// Not durable. Not for prod. Perfect for /debug/* endpoints, that's it!
type MemoryArtifactStore struct {
	mu    sync.RWMutex
	blobs map[string][]byte
	metas map[string]ArtifactMeta
}

func NewMemoryArtifactStore() *MemoryArtifactStore {
	return &MemoryArtifactStore{
		blobs: make(map[string][]byte),
		metas: make(map[string]ArtifactMeta),
	}
}

func (s *MemoryArtifactStore) Put(ctx context.Context, data []byte, meta ArtifactMeta) (ArtifactRef, ArtifactMeta, error) {
	_ = ctx

	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = time.Now().UTC()
	}
	meta.Size = int64(len(data))

	// keep ID format consistent w/ LocalArtifactStore: 64 hex chars
	id := newHex64()

	sum := sha256.Sum256(data)
	meta.ArtifactID = id
	meta.Sha256 = hex.EncodeToString(sum[:])

	s.mu.Lock()
	s.blobs[id] = cloneBytes(data)
	s.metas[id] = meta
	s.mu.Unlock()

	return ArtifactRef{ArtifactID: id}, meta, nil
}

func (s *MemoryArtifactStore) Get(ctx context.Context, artifactID string) ([]byte, ArtifactMeta, error) {
	_ = ctx

	s.mu.RLock()
	b, okB := s.blobs[artifactID]
	m, okM := s.metas[artifactID]
	s.mu.RUnlock()

	if !okB || !okM {
		return nil, ArtifactMeta{}, ErrArtifactNotFound
	}

	return cloneBytes(b), m, nil
}

func (s *MemoryArtifactStore) Delete(ctx context.Context, artifactID string) error {
	_ = ctx

	s.mu.Lock()
	delete(s.blobs, artifactID)
	delete(s.metas, artifactID)
	s.mu.Unlock()
	return nil
}

func newHex64() string {
	var buf [32]byte // 32 bytes -> 64 hex chars
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}

	out := make([]byte, len(b))
	copy(out, b)
	return out
}
