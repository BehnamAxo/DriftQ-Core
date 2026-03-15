package broker

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/driftq-org/DriftQ-Core/internal/storage"
)

func TestCreateTopicWithConfigAndDescribeTopic(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBroker()
	ctx := context.Background()

	if err := b.CreateTopicWithConfig(ctx, "rt", 2, TopicConfig{Mode: TopicModeRealtime}); err != nil {
		t.Fatalf("CreateTopicWithConfig: %v", err)
	}

	desc, err := b.DescribeTopic(ctx, "rt")
	if err != nil {
		t.Fatalf("DescribeTopic: %v", err)
	}

	if desc.Config.Mode != TopicModeRealtime {
		t.Fatalf("mode=%q want=%q", desc.Config.Mode, TopicModeRealtime)
	}
	if desc.Partitions != 2 {
		t.Fatalf("partitions=%d want=2", desc.Partitions)
	}
}

func TestRealtimeTopicModePersistsAcrossWALReplay(t *testing.T) {
	t.Parallel()

	walPath := filepath.Join(t.TempDir(), "broker-realtime.wal")
	wal, err := storage.OpenFileWAL(walPath)
	if err != nil {
		t.Fatalf("OpenFileWAL: %v", err)
	}

	b := NewInMemoryBrokerWithWAL(wal)
	ctx := context.Background()
	if err := b.CreateTopicWithConfig(ctx, "rt", 1, TopicConfig{Mode: TopicModeRealtime}); err != nil {
		t.Fatalf("CreateTopicWithConfig: %v", err)
	}

	if err := b.Close(); err != nil {
		t.Fatalf("broker Close: %v", err)
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("wal Close: %v", err)
	}

	wal2, err := storage.OpenFileWAL(walPath)
	if err != nil {
		t.Fatalf("OpenFileWAL replay: %v", err)
	}
	defer wal2.Close()

	restored, err := NewInMemoryBrokerFromWAL(wal2)
	if err != nil {
		t.Fatalf("NewInMemoryBrokerFromWAL: %v", err)
	}

	desc, err := restored.DescribeTopic(ctx, "rt")
	if err != nil {
		t.Fatalf("DescribeTopic restored: %v", err)
	}

	if desc.Config.Mode != TopicModeRealtime {
		t.Fatalf("restored mode=%q want=%q", desc.Config.Mode, TopicModeRealtime)
	}
}
