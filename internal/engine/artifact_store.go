package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"
)

var (
	ErrArtifactNotFound   = errors.New("artifact not found")
	ErrInvalidArtifactID  = errors.New("invalid artifact id")
	ErrArtifactStoreUnset = errors.New("artifact store not configured")
)

type ArtifactRef struct {
	ArtifactID string `json:"artifact_id"`
}

type ArtifactMeta struct {
	ArtifactID  string    `json:"artifact_id"`
	Sha256      string    `json:"sha256"`
	ContentType string    `json:"content_type,omitempty"`
	Size        int64     `json:"size"`
	CreatedAt   time.Time `json:"created_at"`

	// Keeping these around for debugging
	TenantID   string `json:"tenant_id,omitempty"`
	RunID      string `json:"run_id,omitempty"`
	WorkflowID string `json:"workflow_id,omitempty"`
	NodeID     string `json:"node_id,omitempty"`
	Attempt    int    `json:"attempt,omitempty"`

	// Optional extra info
	Labels       map[string]string `json:"labels,omitempty"`
	Description  string            `json:"description,omitempty"`
	OriginalName string            `json:"original_name,omitempty"`
}

type ArtifactStore interface {
	Put(ctx context.Context, data []byte, meta ArtifactMeta) (ArtifactRef, ArtifactMeta, error)
	Get(ctx context.Context, artifactID string) ([]byte, ArtifactMeta, error)
	Delete(ctx context.Context, artifactID string) error
}

type LocalArtifactStore struct {
	root string
}

// Layout:
//
//	<root>/blobs/aa/<artifact_id>
//	<root>/meta/aa/<artifact_id>.json
func NewLocalArtifactStore(root string) (*LocalArtifactStore, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, fmt.Errorf("artifact store root required")
	}

	if err := os.MkdirAll(filepath.Join(root, "blobs"), 0o755); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Join(root, "meta"), 0o755); err != nil {
		return nil, err
	}

	return &LocalArtifactStore{root: root}, nil
}

var hex64 = regexp.MustCompile(`^[a-f0-9]{64}$`)

func (s *LocalArtifactStore) validateID(id string) error {
	id = strings.TrimSpace(id)
	if !hex64.MatchString(id) {
		return ErrInvalidArtifactID
	}
	return nil
}

func (s *LocalArtifactStore) blobPath(id string) (string, error) {
	if err := s.validateID(id); err != nil {
		return "", err
	}

	prefix := id[:2]
	return filepath.Join(s.root, "blobs", prefix, id), nil
}

func (s *LocalArtifactStore) metaPath(id string) (string, error) {
	if err := s.validateID(id); err != nil {
		return "", err
	}

	prefix := id[:2]
	return filepath.Join(s.root, "meta", prefix, id+".json"), nil
}

func (s *LocalArtifactStore) Put(ctx context.Context, data []byte, meta ArtifactMeta) (ArtifactRef, ArtifactMeta, error) {
	_ = ctx // kept (for now) for future cancellation/IO pacing hooks

	sum := sha256.Sum256(data)
	id := hex.EncodeToString(sum[:])

	meta.ArtifactID = id
	meta.Sha256 = id
	meta.Size = int64(len(data))

	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = time.Now().UTC()
	}

	blobPath, err := s.blobPath(id)
	if err != nil {
		return ArtifactRef{}, ArtifactMeta{}, err
	}

	metaPath, err := s.metaPath(id)
	if err != nil {
		return ArtifactRef{}, ArtifactMeta{}, err
	}

	// Ensure dirs exist
	if err := os.MkdirAll(filepath.Dir(blobPath), 0o755); err != nil {
		return ArtifactRef{}, ArtifactMeta{}, err
	}

	if err := os.MkdirAll(filepath.Dir(metaPath), 0o755); err != nil {
		return ArtifactRef{}, ArtifactMeta{}, err
	}

	if _, statErr := os.Stat(blobPath); errors.Is(statErr, os.ErrNotExist) {
		if err := writeFileAtomic(blobPath, data, 0o644, false /* no replace */); err != nil {
			return ArtifactRef{}, ArtifactMeta{}, err
		}
	} else if statErr != nil {
		return ArtifactRef{}, ArtifactMeta{}, statErr
	}

	// Write/replace metadata (safe to overwrite; last-writer wins for meta fields)
	metaBytes, _ := json.MarshalIndent(meta, "", "  ")
	if err := writeFileAtomic(metaPath, metaBytes, 0o644, true /* replace */); err != nil {
		return ArtifactRef{}, ArtifactMeta{}, err
	}

	return ArtifactRef{ArtifactID: id}, meta, nil
}

func (s *LocalArtifactStore) Get(ctx context.Context, artifactID string) ([]byte, ArtifactMeta, error) {
	_ = ctx

	blobPath, err := s.blobPath(artifactID)
	if err != nil {
		return nil, ArtifactMeta{}, err
	}

	metaPath, err := s.metaPath(artifactID)
	if err != nil {
		return nil, ArtifactMeta{}, err
	}

	mb, err := os.ReadFile(metaPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ArtifactMeta{}, ErrArtifactNotFound
	}

	if err != nil {
		return nil, ArtifactMeta{}, err
	}

	var meta ArtifactMeta
	if err := json.Unmarshal(mb, &meta); err != nil {
		return nil, ArtifactMeta{}, fmt.Errorf("bad meta json for %s: %w", artifactID, err)
	}

	b, err := os.ReadFile(blobPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ArtifactMeta{}, ErrArtifactNotFound
	}

	if err != nil {
		return nil, ArtifactMeta{}, err
	}

	return b, meta, nil
}

func (s *LocalArtifactStore) Delete(ctx context.Context, artifactID string) error {
	_ = ctx

	blobPath, err := s.blobPath(artifactID)
	if err != nil {
		return err
	}

	metaPath, err := s.metaPath(artifactID)
	if err != nil {
		return err
	}

	// Best-effort delete
	_ = os.Remove(metaPath)
	_ = os.Remove(blobPath)
	return nil
}

func (s *LocalArtifactStore) ListByRun(ctx context.Context, runID string, limit int) ([]ArtifactMeta, error) {
	// Best-effort listing by scanning meta files. This is primarily for debugging / CLI usage
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, errors.New("run_id is required")
	}

	metaRoot := filepath.Join(s.root, "meta")
	if _, err := os.Stat(metaRoot); errors.Is(err, os.ErrNotExist) {
		return []ArtifactMeta{}, nil
	} else if err != nil {
		return nil, err
	}

	out := make([]ArtifactMeta, 0, 32)
	err := filepath.WalkDir(metaRoot, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if d.IsDir() {
			return nil
		}

		if !strings.HasSuffix(d.Name(), ".json") {
			return nil
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		mb, err := os.ReadFile(path)
		if err != nil {
			// best-effort: skip unreadable files
			return nil
		}

		var meta ArtifactMeta
		if err := json.Unmarshal(mb, &meta); err != nil {
			// best-effort: skip corrupted meta
			return nil
		}

		if meta.RunID == runID {
			out = append(out, meta)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// newest first
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})

	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}

	return out, nil
}

type PruneStats struct {
	Considered int
	Deleted    int
}

// Deletes artifacts whose meta.CreatedAt is strictly before cutoff
func (s *LocalArtifactStore) PruneOlderThan(ctx context.Context, cutoff time.Time) (PruneStats, error) {
	var stats PruneStats

	metaRoot := filepath.Join(s.root, "meta")
	if _, err := os.Stat(metaRoot); errors.Is(err, os.ErrNotExist) {
		// Nothing to prune
		return stats, nil
	} else if err != nil {
		return stats, err
	}

	err := filepath.WalkDir(metaRoot, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if d.IsDir() {
			return nil
		}

		if !strings.HasSuffix(d.Name(), ".json") {
			return nil
		}

		// Respect caller cancellation
		if err := ctx.Err(); err != nil {
			return err
		}

		stats.Considered++

		mb, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		var meta ArtifactMeta
		if err := json.Unmarshal(mb, &meta); err != nil {
			return fmt.Errorf("bad meta json at %s: %w", path, err)
		}

		// If CreatedAt is missing, skip (don’t delete unknown-age artifacts).
		if meta.CreatedAt.IsZero() {
			return nil
		}

		if meta.CreatedAt.Before(cutoff) {
			artifactID := strings.TrimSuffix(d.Name(), ".json")
			_ = s.Delete(ctx, artifactID) // best-effort
			stats.Deleted++
		}

		return nil
	})

	if err != nil {
		return stats, err
	}

	return stats, nil
}

// This guy writes data to a temp file in the same dir then renames
func writeFileAtomic(dst string, data []byte, perm os.FileMode, replace bool) error {
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}

	tmpName := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}

	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return err
	}

	if err := tmp.Sync(); err != nil {
		cleanup()
		return err
	}

	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}

	// chmod is best-effort on Windows and on Unix we want it
	if err := os.Chmod(tmpName, perm); err != nil && runtime.GOOS != "windows" {
		cleanup()
		return err
	}

	// If not replacing and dst already exists, just drop the temp file.
	if !replace {
		if _, err := os.Stat(dst); err == nil {
			_ = os.Remove(tmpName)
			return nil
		}
	}

	// Rename with retries on Windows. Under concurrency, antivirus/indexers can briefly lock files/directories, causing "Access is denied" / sharing violations.
	attempts := 1
	if runtime.GOOS == "windows" {
		attempts = 12
	}

	var lastErr error
	for i := 0; i < attempts; i++ {
		// On Windows, os.Rename does not replace existing files.
		if replace && runtime.GOOS == "windows" {
			_ = os.Remove(dst) // best-effort (may fail if another process has it open)
		}

		if err := os.Rename(tmpName, dst); err == nil {
			return nil
		} else {
			lastErr = err
		}

		if runtime.GOOS != "windows" || !isRetryableWindowsRenameErr(lastErr) {
			break
		}

		// simple backoff: 5ms,10ms,...60ms
		sleep := time.Duration(5*(i+1)) * time.Millisecond
		time.Sleep(sleep)
	}

	_ = os.Remove(tmpName)
	return lastErr
}

func isRetryableWindowsRenameErr(err error) bool {
	// Windows-specific transient errors:
	// - ERROR_ACCESS_DENIED (5)
	// - ERROR_SHARING_VIOLATION (32)
	// - ERROR_LOCK_VIOLATION (33)
	var le *os.LinkError
	if errors.As(err, &le) {
		if errno, ok := le.Err.(syscall.Errno); ok {
			switch errno {
			case syscall.Errno(5), syscall.Errno(32), syscall.Errno(33):
				return true
			}
		}
	}

	// Fallback: string match (covers cases where the underlying errno isn't exposed as syscall.Errno)
	s := strings.ToLower(err.Error())
	if strings.Contains(s, "access is denied") || strings.Contains(s, "sharing violation") || strings.Contains(s, "used by another process") {
		return true
	}

	return false
}

func (s *MemoryArtifactStore) ListByRun(ctx context.Context, runID string, limit int) ([]ArtifactMeta, error) {
	_ = ctx

	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, errors.New("run_id is required")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]ArtifactMeta, 0, 16)
	for _, m := range s.metas {
		if m.RunID == runID {
			out = append(out, m)
		}
	}

	// newest first
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})

	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}

	return out, nil
}
