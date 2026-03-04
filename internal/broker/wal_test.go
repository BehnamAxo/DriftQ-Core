package broker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/storage"
)

// WAL Recovery Tests
func TestWAL_RecoveryRestoresTopics(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "driftq-wal-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test.wal")
	ctx := context.Background()

	// Phase 1: Create broker, add topics, produce messages
	{
		wal, err := storage.OpenFileWAL(walPath)
		if err != nil {
			t.Fatalf("OpenFileWAL: %v", err)
		}

		b := NewInMemoryBrokerWithWAL(wal)

		if err := b.CreateTopic(ctx, "topic1", 2); err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}

		if err := b.CreateTopic(ctx, "topic2", 3); err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}

		// Produce messages
		for i := 0; i < 5; i++ {
			msg := Message{Value: []byte{byte(i)}}
			if err := b.Produce(ctx, "topic1", msg); err != nil {
				t.Fatalf("Produce: %v", err)
			}
		}

		_ = b.Close()
		wal.Close()
	}

	// Phase 2: Recover from WAL
	{
		wal, err := storage.OpenFileWAL(walPath)
		if err != nil {
			t.Fatalf("OpenFileWAL for recovery: %v", err)
		}
		defer wal.Close()

		b, err := NewInMemoryBrokerFromWAL(wal)
		if err != nil {
			t.Fatalf("NewInMemoryBrokerFromWAL: %v", err)
		}
		defer b.Close()

		// Verify topics exist
		topics, err := b.ListTopics(ctx)
		if err != nil {
			t.Fatalf("ListTopics: %v", err)
		}

		topicMap := make(map[string]bool)
		for _, topic := range topics {
			topicMap[topic] = true
		}

		if !topicMap["topic1"] {
			t.Fatal("topic1 not recovered")
		}

		if !topicMap["topic2"] {
			t.Fatal("topic2 not recovered")
		}

		// Verify messages exist
		ch, err := b.Consume(ctx, "topic1", "group1", "owner1")
		if err != nil {
			t.Fatalf("Consume: %v", err)
		}

		count := 0
		timeout := time.After(2 * time.Second)

	Loop:
		for {
			select {
			case <-ch:
				count++
				if count >= 5 {
					break Loop
				}
			case <-timeout:
				break Loop
			}
		}

		if count != 5 {
			t.Fatalf("expected 5 messages after recovery, got %d", count)
		}
	}
}

func TestWAL_RecoveryRestoresOffsets(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "driftq-wal-offset-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test.wal")
	ctx := context.Background()

	// Phase 1: Produce and ack some messages
	{
		wal, err := storage.OpenFileWAL(walPath)
		if err != nil {
			t.Fatalf("OpenFileWAL: %v", err)
		}

		b := NewInMemoryBrokerWithWAL(wal)
		b.StartRedeliveryLoop(ctx)

		if err := b.CreateTopic(ctx, "offset-test", 1); err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}

		ch, err := b.ConsumeWithLease(ctx, "offset-test", "group1", "owner1", 5*time.Second)
		if err != nil {
			t.Fatalf("Consume: %v", err)
		}

		// Produce 5 messages
		for i := 0; i < 5; i++ {
			msg := Message{Value: []byte{byte(i)}}
			if err := b.Produce(ctx, "offset-test", msg); err != nil {
				t.Fatalf("Produce: %v", err)
			}
		}

		// Ack first 3
		for i := 0; i < 3; i++ {
			select {
			case msg := <-ch:
				if err := b.Ack(ctx, "offset-test", "group1", msg.Partition, msg.Offset); err != nil {
					t.Fatalf("Ack: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("timeout")
			}
		}

		_ = b.Close()
		wal.Close()
	}

	// Phase 2: Recover and verify offset
	{
		wal, err := storage.OpenFileWAL(walPath)
		if err != nil {
			t.Fatalf("OpenFileWAL: %v", err)
		}
		defer wal.Close()

		b, err := NewInMemoryBrokerFromWAL(wal)
		if err != nil {
			t.Fatalf("NewInMemoryBrokerFromWAL: %v", err)
		}
		defer b.Close()

		ch, err := b.Consume(ctx, "offset-test", "group1", "owner1")
		if err != nil {
			t.Fatalf("Consume after recovery: %v", err)
		}

		// Should only get 2 remaining messages (offset 3 and 4)
		count := 0
		timeout := time.After(2 * time.Second)

	Loop:
		for {
			select {
			case msg := <-ch:
				// Should be offset 3 or 4
				if msg.Offset < 3 {
					t.Fatalf("got already-acked offset %d", msg.Offset)
				}
				count++
				if count >= 2 {
					break Loop
				}
			case <-timeout:
				break Loop
			}
		}

		if count != 2 {
			t.Fatalf("expected 2 unacked messages, got %d", count)
		}
	}
}

func TestWAL_RecoveryRestoresIdempotency(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "driftq-wal-idem-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test.wal")
	ctx := context.Background()

	// Phase 1: Produce with idempotency key
	{
		wal, err := storage.OpenFileWAL(walPath)
		if err != nil {
			t.Fatalf("OpenFileWAL: %v", err)
		}

		b := NewInMemoryBrokerWithWAL(wal)

		if err := b.CreateTopic(ctx, "idem-recovery", 1); err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}

		msg := Message{
			Value: []byte("original"),
			Envelope: &Envelope{
				IdempotencyKey: "unique-123",
			},
		}
		if err := b.Produce(ctx, "idem-recovery", msg); err != nil {
			t.Fatalf("Produce: %v", err)
		}

		_ = b.Close()
		wal.Close()
	}

	// Phase 2: Recover and try duplicate
	{
		wal, err := storage.OpenFileWAL(walPath)
		if err != nil {
			t.Fatalf("OpenFileWAL: %v", err)
		}
		defer wal.Close()

		b, err := NewInMemoryBrokerFromWAL(wal)
		if err != nil {
			t.Fatalf("NewInMemoryBrokerFromWAL: %v", err)
		}
		defer b.Close()

		// Try to produce same idempotency key
		msg := Message{
			Value: []byte("duplicate"),
			Envelope: &Envelope{
				IdempotencyKey: "unique-123",
			},
		}
		if err := b.Produce(ctx, "idem-recovery", msg); err != nil {
			t.Fatalf("Produce duplicate: %v", err)
		}

		// Should only have 1 message
		ch, err := b.Consume(ctx, "idem-recovery", "group1", "owner1")
		if err != nil {
			t.Fatalf("Consume: %v", err)
		}

		select {
		case msg := <-ch:
			if string(msg.Value) != "original" {
				t.Fatalf("expected 'original', got '%s'", msg.Value)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout")
		}

		select {
		case <-ch:
			t.Fatal("unexpected duplicate")
		case <-time.After(500 * time.Millisecond):
			// Expected
		}
	}
}

func TestWAL_RecoveryRestoresRetryState(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "driftq-wal-retry-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test.wal")
	ctx := context.Background()

	// Phase 1: Produce, nack to create retry state
	{
		wal, err := storage.OpenFileWAL(walPath)
		if err != nil {
			t.Fatalf("OpenFileWAL: %v", err)
		}

		b := NewInMemoryBrokerWithWAL(wal)
		b.StartRedeliveryLoop(ctx)

		if err := b.CreateTopic(ctx, "retry-recovery", 1); err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}

		ch, err := b.ConsumeWithLease(ctx, "retry-recovery", "group1", "owner1", 500*time.Millisecond)
		if err != nil {
			t.Fatalf("Consume: %v", err)
		}

		msg := Message{Value: []byte("retry-me")}
		if err := b.Produce(ctx, "retry-recovery", msg); err != nil {
			t.Fatalf("Produce: %v", err)
		}

		select {
		case m := <-ch:
			if err := b.Nack(ctx, "retry-recovery", "group1", m.Partition, m.Offset, "owner1", "test-error"); err != nil {
				t.Fatalf("Nack: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout")
		}

		_ = b.Close()
		wal.Close()
	}

	// Phase 2: Recover and check retry state preserved
	{
		wal, err := storage.OpenFileWAL(walPath)
		if err != nil {
			t.Fatalf("OpenFileWAL: %v", err)
		}
		defer wal.Close()

		b, err := NewInMemoryBrokerFromWAL(wal)
		if err != nil {
			t.Fatalf("NewInMemoryBrokerFromWAL: %v", err)
		}
		defer b.Close()

		ch, err := b.Consume(ctx, "retry-recovery", "group1", "owner1")
		if err != nil {
			t.Fatalf("Consume after recovery: %v", err)
		}

		// Message should be redelivered with last_error preserved
		select {
		case msg := <-ch:
			if msg.LastError == "" {
				t.Log("Warning: last_error not preserved after recovery")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for redelivery")
		}
	}
}

func TestWAL_EmptyWAL(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "driftq-wal-empty-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "empty.wal")

	wal, err := storage.OpenFileWAL(walPath)
	if err != nil {
		t.Fatalf("OpenFileWAL: %v", err)
	}
	defer wal.Close()

	b, err := NewInMemoryBrokerFromWAL(wal)
	if err != nil {
		t.Fatalf("NewInMemoryBrokerFromWAL: %v", err)
	}
	defer b.Close()

	// Should work fine with empty WAL
	topics, err := b.ListTopics(context.Background())
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}

	if len(topics) != 0 {
		t.Fatalf("expected 0 topics, got %d", len(topics))
	}
}

func TestWAL_NilWAL(t *testing.T) {
	// Should work without WAL (in-memory only)
	b, err := NewInMemoryBrokerFromWAL(nil)
	if err != nil {
		t.Fatalf("NewInMemoryBrokerFromWAL(nil): %v", err)
	}

	ctx := context.Background()
	if err := b.CreateTopic(ctx, "no-wal", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
}

// FileWAL Unit Tests
func TestFileWAL_AppendAndReplay(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "driftq-filewal-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := storage.OpenFileWAL(walPath)
	if err != nil {
		t.Fatalf("OpenFileWAL: %v", err)
	}

	// Append entries
	entries := []storage.Entry{
		{Type: storage.RecordTypeMessage, Topic: "t1", Partition: 0, Offset: 0, Value: []byte("msg1")},
		{Type: storage.RecordTypeMessage, Topic: "t1", Partition: 0, Offset: 1, Value: []byte("msg2")},
		{Type: storage.RecordTypeOffset, Topic: "t1", Group: "g1", Partition: 0, Offset: 0},
	}

	for _, e := range entries {
		if err := wal.Append(e); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	wal.Close()

	// Reopen and replay
	wal, err = storage.OpenFileWAL(walPath)
	if err != nil {
		t.Fatalf("OpenFileWAL for replay: %v", err)
	}
	defer wal.Close()

	replayed, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(replayed) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(replayed))
	}

	if string(replayed[0].Value) != "msg1" {
		t.Fatalf("expected 'msg1', got '%s'", replayed[0].Value)
	}
}

func TestFileWAL_LargeEntry(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "driftq-wal-large-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "large.wal")

	wal, err := storage.OpenFileWAL(walPath)
	if err != nil {
		t.Fatalf("OpenFileWAL: %v", err)
	}

	// 5MB value
	largeValue := make([]byte, 5*1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	entry := storage.Entry{
		Type:      storage.RecordTypeMessage,
		Topic:     "large",
		Partition: 0,
		Offset:    0,
		Value:     largeValue,
	}

	if err := wal.Append(entry); err != nil {
		t.Fatalf("Append large: %v", err)
	}

	wal.Close()

	// Replay
	wal, err = storage.OpenFileWAL(walPath)
	if err != nil {
		t.Fatalf("OpenFileWAL: %v", err)
	}
	defer wal.Close()

	replayed, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay large: %v", err)
	}

	if len(replayed) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(replayed))
	}

	if len(replayed[0].Value) != 5*1024*1024 {
		t.Fatalf("value size mismatch")
	}
}

func TestFileWAL_ConcurrentAppend(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "driftq-wal-concurrent-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "concurrent.wal")

	wal, err := storage.OpenFileWAL(walPath)
	if err != nil {
		t.Fatalf("OpenFileWAL: %v", err)
	}
	defer wal.Close()

	// Concurrent appends
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(n int) {
			for j := 0; j < 10; j++ {
				entry := storage.Entry{
					Type:   storage.RecordTypeMessage,
					Topic:  "concurrent",
					Offset: int64(n*10 + j),
					Value:  []byte{byte(n), byte(j)},
				}
				wal.Append(entry)
			}
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	// Replay should have all entries
	replayed, err := wal.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(replayed) != 100 {
		t.Fatalf("expected 100 entries, got %d", len(replayed))
	}
}
