package broker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/storage"
)

func TestWALReplayDoesNotResurrectCommittedRetryState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "driftq-retry-cleanup.wal")

	wal, err := storage.OpenFileWAL(path)
	if err != nil {
		t.Fatalf("OpenFileWAL: %v", err)
	}
	defer func() { _ = wal.Close() }()

	b := NewInMemoryBrokerWithWAL(wal)
	b.redeliverTick = 5 * time.Millisecond

	ctx := context.Background()

	if err := b.CreateTopic(ctx, "t1", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	if err := b.Produce(ctx, "t1", Message{Value: []byte("v")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	cctx, cancel := context.WithCancel(ctx)
	ch, err := b.ConsumeWithLease(cctx, "t1", "g1", "ownerA", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("ConsumeWithLease: %v", err)
	}

	m1 := recvMsg(t, ch, 500*time.Millisecond)
	if m1.Offset != 0 {
		t.Fatalf("expected offset 0, got %d", m1.Offset)
	}

	// Stop consumer so Nack won't immediately redeliver (avoids races)
	cancel()
	waitClosed(t, ch, 500*time.Millisecond)

	// Write a retry state record (via Nack) and then commit the message (via AckIfOwner)
	if err := b.Nack(ctx, "t1", "g1", m1.Partition, m1.Offset, "ownerA", "boom"); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if err := b.AckIfOwner(ctx, "t1", "g1", m1.Partition, m1.Offset, "ownerA"); err != nil {
		t.Fatalf("AckIfOwner: %v", err)
	}

	// Close WAL so all bytes are durable before replay
	if err := wal.Close(); err != nil {
		t.Fatalf("Close WAL: %v", err)
	}

	// Re-open and replay into a new broker instance
	wal2, err := storage.OpenFileWAL(path)
	if err != nil {
		t.Fatalf("OpenFileWAL(reopen): %v", err)
	}
	defer func() { _ = wal2.Close() }()

	b2, err := NewInMemoryBrokerFromWAL(wal2)
	if err != nil {
		t.Fatalf("NewInMemoryBrokerFromWAL: %v", err)
	}

	// Offset should be committed
	if b2.consumerOffsets["t1"] == nil || b2.consumerOffsets["t1"]["g1"] == nil {
		t.Fatalf("expected offsets map to exist after replay")
	}
	off := b2.consumerOffsets["t1"]["g1"][m1.Partition]

	// NOTE: This broker stores "last acked offset", not "next offset to read"
	// So after acking offset 0, the committed value is 0 (not 1)
	if off != 0 {
		t.Fatalf("expected committed offset=0 after replay, got %d", off)
	}

	// Retry state for offsets <= committed offset should be purged on replay
	if byTopic, ok := b2.retryState["t1"]; ok {
		if byGroup, ok := byTopic["g1"]; ok {
			if byPart, ok := byGroup[m1.Partition]; ok {
				if _, ok := byPart[m1.Offset]; ok {
					t.Fatalf("expected retry state for committed offset to be purged after replay")
				}
			}
		}
	}

	_ = os.Remove(path) // best-effort cleanup
}
