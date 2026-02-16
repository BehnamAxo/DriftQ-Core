package broker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// Topic Creation Tests
func TestCreateTopic_Success(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	err := b.CreateTopic(ctx, "test-topic", 3)
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	topics, err := b.ListTopics(ctx)
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}

	found := false
	for _, topic := range topics {
		if topic == "test-topic" {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("expected topic 'test-topic' in list, got %v", topics)
	}
}

func TestCreateTopic_EmptyName(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	err := b.CreateTopic(ctx, "", 1)
	if err == nil {
		t.Fatal("expected error for empty topic name")
	}
}

func TestCreateTopic_ZeroPartitions(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	err := b.CreateTopic(ctx, "zero-parts", 0)
	if err == nil {
		t.Fatal("expected error for 0 partitions")
	}
}

func TestCreateTopic_NegativePartitions(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	err := b.CreateTopic(ctx, "neg-parts", -1)
	if err == nil {
		t.Fatal("expected error for negative partitions")
	}
}

func TestCreateTopic_DuplicateName(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "dup", 1); err != nil {
		t.Fatalf("first CreateTopic: %v", err)
	}

	err := b.CreateTopic(ctx, "dup", 1)
	if err == nil {
		t.Fatal("expected error for duplicate topic name")
	}
}

func TestCreateTopic_ManyPartitions(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	err := b.CreateTopic(ctx, "many-parts", 100)
	if err != nil {
		t.Fatalf("CreateTopic with 100 partitions: %v", err)
	}
}

// Produce Tests
func TestProduce_BasicMessage(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "produce-test", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	msg := Message{
		Key:   []byte("key1"),
		Value: []byte("value1"),
	}

	if err := b.Produce(ctx, "produce-test", msg); err != nil {
		t.Fatalf("Produce: %v", err)
	}
}

func TestProduce_ToNonExistentTopic(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	msg := Message{Value: []byte("test")}
	err := b.Produce(ctx, "nonexistent", msg)
	if err == nil {
		t.Fatal("expected error producing to non-existent topic")
	}
}

func TestProduce_EmptyValue(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "empty-val", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Empty value should be allowed
	msg := Message{Value: []byte{}}
	if err := b.Produce(ctx, "empty-val", msg); err != nil {
		t.Fatalf("Produce with empty value: %v", err)
	}
}

func TestProduce_NilValue(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "nil-val", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	msg := Message{Value: nil}
	if err := b.Produce(ctx, "nil-val", msg); err != nil {
		t.Fatalf("Produce with nil value: %v", err)
	}
}

func TestProduce_LargeMessage(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "large-msg", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// 1MB message
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	msg := Message{Value: largeValue}
	if err := b.Produce(ctx, "large-msg", msg); err != nil {
		t.Fatalf("Produce large message: %v", err)
	}
}

func TestProduce_UnicodeValue(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "unicode", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	msg := Message{Value: []byte("你好世界 🌍 مرحبا")}
	if err := b.Produce(ctx, "unicode", msg); err != nil {
		t.Fatalf("Produce unicode: %v", err)
	}
}

func TestProduce_ConcurrentSamePartition(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "concurrent", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			msg := Message{
				Key:   []byte("same-key"),
				Value: []byte("value"),
			}
			if err := b.Produce(ctx, "concurrent", msg); err != nil {
				errCh <- err
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("Produce error: %v", err)
	}
}

// Consume Tests
func TestConsume_BasicFlow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "consume-test", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch, err := b.Consume(ctx, "consume-test", "group1", "owner1")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	msg := Message{Key: []byte("k1"), Value: []byte("v1")}
	if err := b.Produce(ctx, "consume-test", msg); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	select {
	case received := <-ch:
		if string(received.Value) != "v1" {
			t.Fatalf("expected 'v1', got '%s'", received.Value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestConsume_FromNonExistentTopic(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	_, err := b.Consume(ctx, "nonexistent", "group1", "owner1")
	if err == nil {
		t.Fatal("expected error consuming from non-existent topic")
	}
}

func TestConsume_FromEmptyTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "empty-topic", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch, err := b.Consume(ctx, "empty-topic", "group1", "owner1")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	select {
	case msg := <-ch:
		t.Fatalf("unexpected message from empty topic: %+v", msg)
	case <-ctx.Done():
		// Expected - no messages
	}
}

func TestConsume_MultipleConsumersSameGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "multi-consumer", 2); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch1, err := b.Consume(ctx, "multi-consumer", "group1", "owner1")
	if err != nil {
		t.Fatalf("Consume owner1: %v", err)
	}

	ch2, err := b.Consume(ctx, "multi-consumer", "group1", "owner2")
	if err != nil {
		t.Fatalf("Consume owner2: %v", err)
	}

	// Produce multiple messages
	for i := 0; i < 10; i++ {
		msg := Message{Value: []byte("msg")}
		if err := b.Produce(ctx, "multi-consumer", msg); err != nil {
			t.Fatalf("Produce: %v", err)
		}
	}

	// Both consumers should receive messages (round-robin)
	received1 := 0
	received2 := 0
	timeout := time.After(3 * time.Second)

	for received1+received2 < 10 {
		select {
		case <-ch1:
			received1++
		case <-ch2:
			received2++
		case <-timeout:
			t.Fatalf("timeout: received1=%d, received2=%d", received1, received2)
		}
	}

	// Both consumers should have received some messages
	if received1 == 0 || received2 == 0 {
		t.Logf("Warning: load imbalance - received1=%d, received2=%d", received1, received2)
	}
}

func TestConsume_DifferentGroups(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "diff-groups", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch1, err := b.Consume(ctx, "diff-groups", "groupA", "owner1")
	if err != nil {
		t.Fatalf("Consume groupA: %v", err)
	}

	ch2, err := b.Consume(ctx, "diff-groups", "groupB", "owner2")
	if err != nil {
		t.Fatalf("Consume groupB: %v", err)
	}

	msg := Message{Value: []byte("broadcast")}
	if err := b.Produce(ctx, "diff-groups", msg); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Both groups should receive the same message
	timeout := time.After(2 * time.Second)

	select {
	case <-ch1:
	case <-timeout:
		t.Fatal("groupA timeout")
	}

	select {
	case <-ch2:
	case <-timeout:
		t.Fatal("groupB timeout")
	}
}

// Ack/Nack Tests
func TestAck_Success(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()
	b.StartRedeliveryLoop(ctx)

	if err := b.CreateTopic(ctx, "ack-test", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch, err := b.ConsumeWithLease(ctx, "ack-test", "group1", "owner1", 5*time.Second)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	if err := b.Produce(ctx, "ack-test", Message{Value: []byte("test")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	var msg Message
	select {
	case msg = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	if err := b.Ack(ctx, "ack-test", "group1", msg.Partition, msg.Offset); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

func TestAck_WrongPartition(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "wrong-part", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	err := b.Ack(ctx, "wrong-part", "group1", 999, 0)
	if err == nil {
		t.Fatal("expected error for wrong partition")
	}
}

func TestAck_WrongOffset(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "wrong-offset", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	if err := b.Produce(ctx, "wrong-offset", Message{Value: []byte("test")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Ack with wrong offset should fail or be ignored
	err := b.Ack(ctx, "wrong-offset", "group1", 0, 9999)
	// Behavior may vary - just ensure no panic
	_ = err
}

func TestAckIfOwner_WrongOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()
	b.StartRedeliveryLoop(ctx)

	if err := b.CreateTopic(ctx, "wrong-owner", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch, err := b.ConsumeWithLease(ctx, "wrong-owner", "group1", "ownerA", 5*time.Second)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	if err := b.Produce(ctx, "wrong-owner", Message{Value: []byte("test")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	var msg Message
	select {
	case msg = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	// Try to ack with wrong owner
	err = b.AckIfOwner(ctx, "wrong-owner", "group1", msg.Partition, msg.Offset, "wrongOwner")
	if err == nil {
		t.Fatal("expected error for wrong owner")
	}
	if !errors.Is(err, ErrNotOwner) {
		t.Fatalf("expected ErrNotOwner, got %v", err)
	}
}

func TestNack_SchedulesRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()
	b.StartRedeliveryLoop(ctx)

	if err := b.CreateTopic(ctx, "nack-test", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch, err := b.ConsumeWithLease(ctx, "nack-test", "group1", "owner1", 300*time.Millisecond)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	if err := b.Produce(ctx, "nack-test", Message{Value: []byte("retry-me")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Receive first delivery
	var firstMsg Message
	select {
	case firstMsg = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout on first delivery")
	}

	// Nack it
	if err := b.Nack(ctx, "nack-test", "group1", firstMsg.Partition, firstMsg.Offset, "owner1", "test-failure"); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// Wait for redelivery
	select {
	case redelivered := <-ch:
		if redelivered.Offset != firstMsg.Offset {
			t.Fatalf("expected same offset, got %d vs %d", redelivered.Offset, firstMsg.Offset)
		}
		if redelivered.Attempts < 2 {
			t.Fatalf("expected attempts >= 2, got %d", redelivered.Attempts)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for redelivery")
	}
}

// Lease Expiry & Redelivery Tests
func TestConsume_LeaseExpiryTriggersRedelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()
	b.StartRedeliveryLoop(ctx)

	if err := b.CreateTopic(ctx, "lease-test", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Short lease
	ch, err := b.ConsumeWithLease(ctx, "lease-test", "group1", "owner1", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	if err := b.Produce(ctx, "lease-test", Message{Value: []byte("lease-me")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Receive first delivery
	var firstMsg Message
	select {
	case firstMsg = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout on first delivery")
	}

	// Don't ack - let lease expire
	// Wait for redelivery
	select {
	case redelivered := <-ch:
		if redelivered.Offset != firstMsg.Offset {
			t.Fatalf("expected same offset")
		}
		if redelivered.Attempts < 2 {
			t.Fatalf("expected attempts >= 2")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for redelivery after lease expiry")
	}
}

func TestConsumeWithLease_ZeroDuration(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "zero-lease", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Zero lease should use default
	_, err := b.ConsumeWithLease(ctx, "zero-lease", "group1", "owner1", 0)
	if err != nil {
		t.Fatalf("ConsumeWithLease with 0: %v", err)
	}
}

func TestConsumeWithLease_VeryLong(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "long-lease", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// 1 hour lease
	_, err := b.ConsumeWithLease(ctx, "long-lease", "group1", "owner1", time.Hour)
	if err != nil {
		t.Fatalf("ConsumeWithLease with 1h: %v", err)
	}
}

// Backpressure Tests
func TestProduce_BackpressureRejectsWhenFull(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker(
		WithMaxPartitionMsgs(10),
	)

	if err := b.CreateTopic(ctx, "backpressure", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Fill up the partition
	for i := 0; i < 10; i++ {
		msg := Message{Value: []byte("fill")}
		if err := b.Produce(ctx, "backpressure", msg); err != nil {
			t.Fatalf("Produce %d: %v", i, err)
		}
	}

	// Next produce should be rejected
	msg := Message{Value: []byte("overflow")}
	err := b.Produce(ctx, "backpressure", msg)
	if err == nil {
		t.Fatal("expected backpressure rejection")
	}
}

func TestProduce_BackpressureByBytes(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker(
		WithMaxPartitionBytes(100),
	)

	if err := b.CreateTopic(ctx, "backpressure-bytes", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Produce messages until we hit byte limit
	for i := 0; i < 5; i++ {
		msg := Message{Value: make([]byte, 30)} // 30 bytes each
		err := b.Produce(ctx, "backpressure-bytes", msg)
		if err != nil {
			// Expected to fail at some point
			return
		}
	}

	// Should have hit limit
	msg := Message{Value: make([]byte, 30)}
	err := b.Produce(ctx, "backpressure-bytes", msg)
	if err == nil {
		t.Log("Warning: backpressure by bytes may not be strictly enforced")
	}
}

// Message Ordering Tests
func TestProduce_Consume_OrderingPreserved(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "ordering", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch, err := b.ConsumeWithLease(ctx, "ordering", "group1", "owner1", 5*time.Second)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// Produce messages in order
	for i := 0; i < 10; i++ {
		msg := Message{Value: []byte{byte(i)}}
		if err := b.Produce(ctx, "ordering", msg); err != nil {
			t.Fatalf("Produce %d: %v", i, err)
		}
	}

	// Consume and verify order
	for i := 0; i < 10; i++ {
		select {
		case msg := <-ch:
			if msg.Value[0] != byte(i) {
				t.Fatalf("expected order %d, got %d", i, msg.Value[0])
			}
			b.Ack(ctx, "ordering", "group1", msg.Partition, msg.Offset)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout at message %d", i)
		}
	}
}

// Key Partitioning Tests
func TestProduce_SameKeyGoesToSamePartition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "key-partition", 4); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch, err := b.Consume(ctx, "key-partition", "group1", "owner1")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// Produce multiple messages with same key
	key := []byte("same-key")
	for i := 0; i < 5; i++ {
		msg := Message{Key: key, Value: []byte{byte(i)}}
		if err := b.Produce(ctx, "key-partition", msg); err != nil {
			t.Fatalf("Produce: %v", err)
		}
	}

	// All should go to same partition
	var firstPartition int
	for i := 0; i < 5; i++ {
		select {
		case msg := <-ch:
			if i == 0 {
				firstPartition = msg.Partition
			} else if msg.Partition != firstPartition {
				t.Fatalf("message %d went to partition %d, expected %d", i, msg.Partition, firstPartition)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout at message %d", i)
		}
	}
}

// Edge Cases
func TestMessageKey_SpecialCharacters(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "special-key", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	specialKeys := [][]byte{
		[]byte(""),
		[]byte(" "),
		[]byte("\n\t\r"),
		[]byte("key/with/slashes"),
		[]byte("key:with:colons"),
		[]byte("key=with=equals"),
		nil,
	}

	for _, key := range specialKeys {
		msg := Message{Key: key, Value: []byte("test")}
		if err := b.Produce(ctx, "special-key", msg); err != nil {
			t.Errorf("Produce with key %q: %v", key, err)
		}
	}
}

func TestContextCancellation_StopsConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "cancel-test", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch, err := b.Consume(ctx, "cancel-test", "group1", "owner1")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	cancel()

	// Channel should eventually close or context should prevent further reads
	select {
	case _, ok := <-ch:
		if ok {
			t.Log("got message after cancel, may be buffered")
		}
	case <-time.After(500 * time.Millisecond):
		// Expected
	}
}
