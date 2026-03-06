package broker

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/storage"
)

func TestConsumerLag_ComputesFromBrokerState(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "demo", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := b.Produce(ctx, "demo", Message{Value: []byte("x")}); err != nil {
			t.Fatalf("Produce(%d): %v", i, err)
		}
	}

	rows, err := b.ConsumerLag(ctx, "bench", "demo")
	if err != nil {
		t.Fatalf("ConsumerLag: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	r := rows[0]
	if r.HeadOffset != 3 {
		t.Fatalf("head_offset=%d want=3", r.HeadOffset)
	}
	if r.CommittedOffset != 0 {
		t.Fatalf("committed_offset=%d want=0", r.CommittedOffset)
	}
	if r.Lag != 3 {
		t.Fatalf("lag=%d want=3", r.Lag)
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch, err := b.ConsumeWithLease(cctx, "demo", "bench", "c1", 5*time.Second)
	if err != nil {
		t.Fatalf("ConsumeWithLease: %v", err)
	}

	var m Message
	select {
	case m = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for consumed message")
	}

	if err := b.AckIfOwner(ctx, "demo", "bench", m.Partition, m.Offset, "c1"); err != nil {
		t.Fatalf("AckIfOwner: %v", err)
	}

	rows, err = b.ConsumerLag(ctx, "bench", "demo")
	if err != nil {
		t.Fatalf("ConsumerLag after ack: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after ack, got %d", len(rows))
	}

	r = rows[0]
	if r.CommittedOffset != m.Offset+1 {
		t.Fatalf("committed_offset=%d want=%d", r.CommittedOffset, m.Offset+1)
	}
	if r.HeadOffset != 3 {
		t.Fatalf("head_offset=%d want=3", r.HeadOffset)
	}
	if r.Inflight < 0 {
		t.Fatalf("inflight=%d want>=0", r.Inflight)
	}
}

func TestConsumerLag_WALReplayStillReturnsRows(t *testing.T) {
	ctx := context.Background()
	walPath := filepath.Join(t.TempDir(), "broker.wal")

	wal, err := storage.OpenFileWAL(walPath)
	if err != nil {
		t.Fatalf("OpenFileWAL(1): %v", err)
	}

	b := NewInMemoryBrokerWithWAL(wal)
	if err := b.CreateTopic(ctx, "demo", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	for i := 0; i < 2; i++ {
		if err := b.Produce(ctx, "demo", Message{Value: []byte("x")}); err != nil {
			t.Fatalf("Produce(%d): %v", i, err)
		}
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("Close(1): %v", err)
	}

	wal2, err := storage.OpenFileWAL(walPath)
	if err != nil {
		t.Fatalf("OpenFileWAL(2): %v", err)
	}
	defer wal2.Close()

	b2, err := NewInMemoryBrokerFromWAL(wal2)
	if err != nil {
		t.Fatalf("NewInMemoryBrokerFromWAL: %v", err)
	}

	rows, err := b2.ConsumerLag(ctx, "bench", "demo")
	if err != nil {
		t.Fatalf("ConsumerLag: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 lag row after replay, got %d", len(rows))
	}

	r := rows[0]
	if r.HeadOffset != 2 {
		t.Fatalf("head_offset=%d want=2", r.HeadOffset)
	}
	if r.CommittedOffset != 0 {
		t.Fatalf("committed_offset=%d want=0", r.CommittedOffset)
	}
	if r.Lag != 2 {
		t.Fatalf("lag=%d want=2", r.Lag)
	}
}
