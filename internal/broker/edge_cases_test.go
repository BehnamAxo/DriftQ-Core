package broker

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Broker Edge Cases
func TestEdgeCase_EmptyTopicName(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	err := b.CreateTopic(ctx, "", 1)
	if err == nil {
		t.Fatal("expected error for empty topic name")
	}
}

func TestEdgeCase_TopicWith1000Partitions(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	// This may be rejected or accepted depending on limits
	err := b.CreateTopic(ctx, "many-partitions", 1000)
	if err != nil {
		t.Logf("1000 partitions rejected: %v", err)
	} else {
		t.Logf("1000 partitions accepted")
	}
}

func TestEdgeCase_MessageValueOver10MB(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "large-msg", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// 15MB message
	largeValue := make([]byte, 15*1024*1024)
	msg := Message{Value: largeValue}

	err := b.Produce(ctx, "large-msg", msg)
	if err != nil {
		t.Logf("Large message rejected: %v", err)
	} else {
		t.Logf("Large message accepted")
	}
}

func TestEdgeCase_MessageKeySpecialChars(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "special-key", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	specialKeys := [][]byte{
		[]byte("\x00\x01\x02"),            // Binary
		[]byte("key with spaces"),         // Spaces
		[]byte("key\nwith\nnewlines"),     // Newlines
		[]byte("key\twith\ttabs"),         // Tabs
		[]byte("key/with/slashes"),        // Slashes
		[]byte("key:with:colons"),         // Colons
		[]byte("key=with=equals"),         // Equals
		[]byte("key\"with\"quotes"),       // Quotes
		[]byte("key\\with\\backslashes"),  // Backslashes
		[]byte("日本語キー"),                   // Unicode
		[]byte(strings.Repeat("x", 1000)), // Very long key
	}

	for _, key := range specialKeys {
		msg := Message{Key: key, Value: []byte("test")}
		if err := b.Produce(ctx, "special-key", msg); err != nil {
			t.Logf("Key %q rejected: %v", key[:min(len(key), 20)], err)
		}
	}
}

func TestEdgeCase_BinaryDataInValue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "binary", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// All possible byte values
	binaryValue := make([]byte, 256)
	for i := range binaryValue {
		binaryValue[i] = byte(i)
	}

	msg := Message{Value: binaryValue}
	if err := b.Produce(ctx, "binary", msg); err != nil {
		t.Fatalf("Produce binary: %v", err)
	}

	ch, _ := b.Consume(ctx, "binary", "g1", "o1")
	select {
	case received := <-ch:
		for i, v := range received.Value {
			if v != byte(i) {
				t.Fatalf("binary corruption at %d: %d", i, v)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestEdgeCase_ConsumeFromNonExistentTopic(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	_, err := b.Consume(ctx, "nonexistent", "g1", "o1")
	if err == nil {
		t.Fatal("expected error for non-existent topic")
	}
}

func TestEdgeCase_AckAlreadyAckedMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()
	b.StartRedeliveryLoop(ctx)

	if err := b.CreateTopic(ctx, "double-ack", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch, _ := b.ConsumeWithLease(ctx, "double-ack", "g1", "o1", 5*time.Second)
	b.Produce(ctx, "double-ack", Message{Value: []byte("test")})

	var msg Message
	select {
	case msg = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	// First ack
	if err := b.Ack(ctx, "double-ack", "g1", msg.Partition, msg.Offset); err != nil {
		t.Fatalf("First ack: %v", err)
	}

	// Second ack - should be no-op or error
	err := b.Ack(ctx, "double-ack", "g1", msg.Partition, msg.Offset)
	// Either no error (idempotent) or error is acceptable
	_ = err
}

func TestEdgeCase_NackWithEmptyReason(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()
	b.StartRedeliveryLoop(ctx)

	if err := b.CreateTopic(ctx, "empty-nack", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch, _ := b.ConsumeWithLease(ctx, "empty-nack", "g1", "o1", 5*time.Second)
	b.Produce(ctx, "empty-nack", Message{Value: []byte("test")})

	var msg Message
	select {
	case msg = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	// Nack with empty reason
	err := b.Nack(ctx, "empty-nack", "g1", msg.Partition, msg.Offset, "o1", "")
	if err != nil {
		t.Logf("Empty reason nack: %v", err)
	}
}

func TestEdgeCase_LeaseTimeout_VeryLarge(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "huge-lease", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// 1 hour lease
	_, err := b.ConsumeWithLease(ctx, "huge-lease", "g1", "o1", time.Hour)
	if err != nil {
		t.Logf("1 hour lease rejected: %v", err)
	}
}

func TestEdgeCase_RetryWithZeroMaxAttempts(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "zero-retry", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	msg := Message{
		Value: []byte("test"),
		Envelope: &Envelope{
			RetryPolicy: &RetryPolicy{
				MaxAttempts: 0, // Should mean no retries
			},
		},
	}

	if err := b.Produce(ctx, "zero-retry", msg); err != nil {
		t.Fatalf("Produce: %v", err)
	}
}

func TestEdgeCase_RetryWithNegativeBackoff(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "neg-backoff", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	msg := Message{
		Value: []byte("test"),
		Envelope: &Envelope{
			RetryPolicy: &RetryPolicy{
				MaxAttempts: 3,
				BackoffMs:   -100, // Negative
			},
		},
	}

	err := b.Produce(ctx, "neg-backoff", msg)
	// Should handle gracefully
	_ = err
}

func TestEdgeCase_DeadlineInPast(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "past-deadline", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	past := time.Now().Add(-time.Hour)
	msg := Message{
		Value: []byte("test"),
		Envelope: &Envelope{
			Deadline: &past,
		},
	}

	err := b.Produce(ctx, "past-deadline", msg)
	// May be rejected or accepted with immediate expiry
	_ = err
}

func TestEdgeCase_DeadlineExactlyNow(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "now-deadline", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	now := time.Now()
	msg := Message{
		Value: []byte("test"),
		Envelope: &Envelope{
			Deadline: &now,
		},
	}

	err := b.Produce(ctx, "now-deadline", msg)
	_ = err
}

func TestEdgeCase_ConcurrentProduceSamePartition(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "concurrent-produce", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	done := make(chan bool, 100)
	for i := 0; i < 100; i++ {
		go func(n int) {
			msg := Message{
				Key:   []byte("same-key"), // Same key = same partition
				Value: []byte{byte(n)},
			}

			b.Produce(ctx, "concurrent-produce", msg)
			done <- true
		}(i)
	}

	// Wait for all
	for i := 0; i < 100; i++ {
		<-done
	}
}

func TestEdgeCase_ConcurrentConsumeSamePartition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "concurrent-consume", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Produce some messages first
	for i := 0; i < 10; i++ {
		b.Produce(ctx, "concurrent-consume", Message{Value: []byte{byte(i)}})
	}

	// Multiple consumers trying to consume same partition
	for i := 0; i < 5; i++ {
		go func(n int) {
			ch, err := b.Consume(ctx, "concurrent-consume", "g1", string(rune('a'+n)))

			if err != nil {
				return
			}

			for {
				select {
				case <-ch:
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	time.Sleep(500 * time.Millisecond)
}

func TestEdgeCase_ConsumerDisconnectsMidStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "disconnect", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch, err := b.Consume(ctx, "disconnect", "g1", "o1")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// Start producing
	go func() {
		for i := 0; i < 100; i++ {
			b.Produce(context.Background(), "disconnect", Message{Value: []byte{byte(i)}})
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Consume a few then disconnect
consumeLoop:
	for i := 0; i < 5; i++ {
		select {
		case <-ch:
		case <-time.After(time.Second):
			break consumeLoop
		}
	}

	cancel() // Disconnect

	// Should not panic or leak
	time.Sleep(100 * time.Millisecond)
}

func TestEdgeCase_IdempotencyKeyEmptyString(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "empty-idem", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Empty string should not trigger deduplication
	for i := 0; i < 3; i++ {
		msg := Message{
			Value: []byte{byte(i)},
			Envelope: &Envelope{
				IdempotencyKey: "",
			},
		}

		b.Produce(ctx, "empty-idem", msg)
	}

	ch, _ := b.Consume(ctx, "empty-idem", "g1", "o1")
	count := 0
	timeout := time.After(time.Second)

Loop:
	for {
		select {
		case <-ch:
			count++
		case <-timeout:
			break Loop
		}
	}

	if count != 3 {
		t.Fatalf("expected 3 messages (no dedup), got %d", count)
	}
}

func TestEdgeCase_IdempotencyKeyVeryLong(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "long-idem", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// 10KB key
	longKey := strings.Repeat("x", 10*1024)
	msg := Message{
		Value: []byte("test"),
		Envelope: &Envelope{
			IdempotencyKey: longKey,
		},
	}

	err := b.Produce(ctx, "long-idem", msg)
	if err != nil {
		t.Logf("Long key rejected: %v", err)
	}
}

// Helper for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
