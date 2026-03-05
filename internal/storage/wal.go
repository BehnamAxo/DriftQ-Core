package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Describes what kind of thing this entry represents
type RecordType uint8

const (
	RecordTypeMessage            RecordType = 1
	RecordTypeOffset             RecordType = 2
	RecordTypeTopic              RecordType = 3 // topic/partition metadata (optional for later)
	RecordTypeRetryState         RecordType = 4 // (topic, group, partition, offset) -> last_error (+ timestamp)
	RecordTypeConsumeIdempotency RecordType = 5 // (scope=consume, tenant, topic, group, idempotency_key) state transitions
)

type Entry struct {
	Type RecordType `json:"type"`

	Topic     string `json:"topic,omitempty"`
	Partition int    `json:"partition,omitempty"`
	Offset    int64  `json:"offset,omitempty"`

	Group string `json:"group,omitempty"`

	Key   []byte `json:"key,omitempty"`
	Value []byte `json:"value,omitempty"`

	// routing metadata
	RoutingLabel string            `json:"routing_label,omitempty"`
	RoutingMeta  map[string]string `json:"routing_meta,omitempty"`

	// envelope fields
	RunID        string            `json:"run_id,omitempty"`
	StepID       string            `json:"step_id,omitempty"`
	ParentStepID string            `json:"parent_step_id,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`

	TargetTopic       string `json:"target_topic,omitempty"`
	PartitionOverride *int   `json:"partition_override,omitempty"`

	IdempotencyKey string     `json:"idempotency_key,omitempty"`
	Deadline       *time.Time `json:"deadline,omitempty"`

	RetryMaxAttempts  int   `json:"retry_max_attempts,omitempty"`
	RetryBackoffMs    int64 `json:"retry_backoff_ms,omitempty"`
	RetryMaxBackoffMs int64 `json:"retry_max_backoff_ms,omitempty"`

	TenantID string `json:"tenant_id,omitempty"`

	// DLQ metadata
	DLQOriginalTopic     string `json:"dlq_original_topic,omitempty"`
	DLQOriginalPartition int    `json:"dlq_original_partition,omitempty"`
	DLQOriginalOffset    int64  `json:"dlq_original_offset,omitempty"`
	DLQAttempts          int    `json:"dlq_attempts,omitempty"`
	DLQLastError         string `json:"dlq_last_error,omitempty"`
	DLQRoutedAtMs        int64  `json:"dlq_routed_at_ms,omitempty"`

	// Retry state records (RecordTypeRetryState)
	LastError   string     `json:"last_error,omitempty"`
	LastErrorAt *time.Time `json:"last_error_at,omitempty"`

	// Consume idempotency records (RecordTypeConsumeIdempotency)
	IdempotencyScope  string `json:"idempotency_scope,omitempty"`  // "consume"
	IdempotencyStatus string `json:"idempotency_status,omitempty"` // "PENDING"|"COMMITTED"|"FAILED"

	LeaseOwner string     `json:"lease_owner,omitempty"`
	LeaseUntil *time.Time `json:"lease_until,omitempty"`

	Result    []byte     `json:"result,omitempty"`
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
}

type WAL interface {
	Append(e Entry) error     // This one writes new stuff
	Replay() ([]Entry, error) // This one restores state after crash
	Close() error             // This one cleans shutdown or releases resources
}

type FileWAL struct {
	mu           sync.Mutex
	f            *os.File
	bw           *bufio.Writer
	enc          *json.Encoder
	path         string
	syncInterval time.Duration
	lastSync     time.Time
}

type FileWALOptions struct {
	// SyncInterval controls fsync cadence.
	// 0 keeps strict durability (fsync every append).
	SyncInterval time.Duration

	// BufferBytes controls the in-process write buffer size.
	// If <= 0, a default is used.
	BufferBytes int
}

func OpenFileWAL(path string) (*FileWAL, error) {
	return OpenFileWALWithOptions(path, FileWALOptions{})
}

func OpenFileWALWithOptions(path string, opts FileWALOptions) (*FileWAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	bufferBytes := opts.BufferBytes
	if bufferBytes <= 0 {
		bufferBytes = 256 * 1024
	}

	syncInterval := opts.SyncInterval
	if syncInterval < 0 {
		syncInterval = 0
	}

	bw := bufio.NewWriterSize(f, bufferBytes)
	enc := json.NewEncoder(bw)
	enc.SetEscapeHTML(false)

	return &FileWAL{
		f:            f,
		bw:           bw,
		enc:          enc,
		path:         path,
		syncInterval: syncInterval,
		lastSync:     time.Now(),
	}, nil
}

func (w *FileWAL) Append(e Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.enc.Encode(e); err != nil {
		return fmt.Errorf("encode WAL entry: %w", err)
	}

	if w.syncInterval > 0 {
		if time.Since(w.lastSync) < w.syncInterval {
			return nil
		}
	}

	if err := w.flushAndSyncLocked(); err != nil {
		return err
	}

	return nil
}

func (w *FileWAL) flushAndSyncLocked() error {
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("flush WAL buffer: %w", err)
	}

	// Durability guarantee in strict mode (or periodic group-commit if configured)
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("fsync WAL: %w", err)
	}

	w.lastSync = time.Now()
	return nil
}

func (w *FileWAL) Replay() ([]Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.bw.Flush(); err != nil {
		return nil, fmt.Errorf("flush WAL buffer before replay: %w", err)
	}

	// Rewind to start
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek WAL start: %w", err)
	}

	var entries []Entry
	scanner := bufio.NewScanner(w.f)

	// ✅ IMPORTANT: increase scanner buffer so large messages (DLQ / big payloads) don't break replay
	// Default is around 64KB token limit
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 10*1024*1024) // allow up to 10MB per line (tune later)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var e Entry
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("decode WAL entry: %w", err)
		}
		entries = append(entries, e)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan WAL: %w", err)
	}

	// Move back to end for future appends
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seek WAL end: %w", err)
	}

	return entries, nil
}

func (w *FileWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.flushAndSyncLocked(); err != nil {
		_ = w.f.Close()
		return err
	}

	return w.f.Close()
}
