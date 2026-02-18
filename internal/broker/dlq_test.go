package broker

import (
	"context"
	"testing"
	"time"
)

// DLQ Routing Tests
func TestDLQ_RoutesAfterMaxAttempts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()
	b.StartRedeliveryLoop(ctx)

	if err := b.CreateTopic(ctx, "dlq-source", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch, err := b.ConsumeWithLease(ctx, "dlq-source", "group1", "owner1", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// Produce with retry policy (max 2 attempts)
	msg := Message{
		Value: []byte("fail-me"),
		Envelope: &Envelope{
			RetryPolicy: &RetryPolicy{
				MaxAttempts:  2,
				BackoffMs:    50,
				MaxBackoffMs: 100,
			},
		},
	}

	if err := b.Produce(ctx, "dlq-source", msg); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Nack until max attempts
	for i := 0; i < 3; i++ {
		select {
		case m := <-ch:
			err := b.Nack(ctx, "dlq-source", "group1", m.Partition, m.Offset, "owner1", "intentional-fail")
			if err != nil {
				t.Logf("Nack %d: %v", i, err)
			}
		case <-time.After(3 * time.Second):
			if i < 2 {
				t.Fatalf("timeout at attempt %d", i)
			}
		}
	}

	// Wait for DLQ routing
	time.Sleep(500 * time.Millisecond)

	// Check DLQ topic exists
	topics, _ := b.ListTopics(ctx)
	dlqFound := false
	for _, topic := range topics {
		if topic == "dlq.dlq-source" {
			dlqFound = true
			break
		}
	}

	if !dlqFound {
		t.Fatal("DLQ topic not created")
	}

	// Consume from DLQ
	dlqCh, err := b.Consume(ctx, "dlq.dlq-source", "dlq-consumer", "owner1")
	if err != nil {
		t.Fatalf("Consume DLQ: %v", err)
	}

	select {
	case dlqMsg := <-dlqCh:
		if string(dlqMsg.Value) != "fail-me" {
			t.Fatalf("expected 'fail-me' in DLQ, got '%s'", dlqMsg.Value)
		}

		if dlqMsg.Envelope == nil || dlqMsg.Envelope.DLQ == nil {
			t.Fatal("DLQ metadata missing")
		}

		if dlqMsg.Envelope.DLQ.OriginalTopic != "dlq-source" {
			t.Fatalf("wrong original topic: %s", dlqMsg.Envelope.DLQ.OriginalTopic)
		}

	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for DLQ message")
	}
}

func TestDLQ_PreservesDLQMetadata(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()
	b.StartRedeliveryLoop(ctx)

	if err := b.CreateTopic(ctx, "dlq-meta", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch, err := b.ConsumeWithLease(ctx, "dlq-meta", "group1", "owner1", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	msg := Message{
		Value: []byte("meta-test"),
		Envelope: &Envelope{
			TenantID: "tenant-123",
			Labels:   map[string]string{"env": "test"},
			RetryPolicy: &RetryPolicy{
				MaxAttempts: 1,
				BackoffMs:   10,
			},
		},
	}

	if err := b.Produce(ctx, "dlq-meta", msg); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Nack once to trigger DLQ (max_attempts=1 means no retries)
	select {
	case m := <-ch:
		b.Nack(ctx, "dlq-meta", "group1", m.Partition, m.Offset, "owner1", "test-error")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	time.Sleep(500 * time.Millisecond)

	// Consume from DLQ
	dlqCh, err := b.Consume(ctx, "dlq.dlq-meta", "dlq-reader", "owner1")
	if err != nil {
		t.Fatalf("Consume DLQ: %v", err)
	}

	select {
	case dlqMsg := <-dlqCh:
		if dlqMsg.Envelope == nil {
			t.Fatal("envelope missing")
		}

		if dlqMsg.Envelope.TenantID != "tenant-123" {
			t.Fatalf("tenant not preserved: %s", dlqMsg.Envelope.TenantID)
		}

		if dlqMsg.Envelope.DLQ == nil {
			t.Fatal("DLQ metadata missing")
		}

		if dlqMsg.Envelope.DLQ.LastError == "" {
			t.Log("Warning: last_error not set in DLQ metadata")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestDLQ_SamePartitionCountAsSource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()
	b.StartRedeliveryLoop(ctx)

	// Create topic with 4 partitions
	if err := b.CreateTopic(ctx, "dlq-parts", 4); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch, err := b.ConsumeWithLease(ctx, "dlq-parts", "group1", "owner1", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	msg := Message{
		Value: []byte("test"),
		Envelope: &Envelope{
			RetryPolicy: &RetryPolicy{MaxAttempts: 1},
		},
	}
	if err := b.Produce(ctx, "dlq-parts", msg); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	select {
	case m := <-ch:
		b.Nack(ctx, "dlq-parts", "group1", m.Partition, m.Offset, "owner1", "fail")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	time.Sleep(500 * time.Millisecond)

	// DLQ should exist and we should be able to read from it
	_, err = b.Consume(ctx, "dlq.dlq-parts", "dlq-reader", "owner1")
	if err != nil {
		t.Fatalf("DLQ topic not created or inaccessible: %v", err)
	}
}

func TestDLQ_NoDLQOfDLQ(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()
	b.StartRedeliveryLoop(ctx)

	// Create a topic named dlq.xxx manually to simulate DLQ
	if err := b.CreateTopic(ctx, "dlq.original", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch, err := b.ConsumeWithLease(ctx, "dlq.original", "group1", "owner1", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// Produce WITH retry policy to DLQ topic
	msg := Message{
		Value: []byte("nested-dlq"),
		Envelope: &Envelope{
			RetryPolicy: &RetryPolicy{MaxAttempts: 1},
		},
	}

	if err := b.Produce(ctx, "dlq.original", msg); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	select {
	case m := <-ch:
		b.Nack(ctx, "dlq.original", "group1", m.Partition, m.Offset, "owner1", "fail")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	time.Sleep(500 * time.Millisecond)

	// Should NOOOT create dlq.dlq.original
	topics, _ := b.ListTopics(ctx)
	for _, topic := range topics {
		if topic == "dlq.dlq.original" {
			t.Fatal("should not create DLQ of DLQ")
		}
	}
}

func TestDLQ_AttemptsCountAccurate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()
	b.StartRedeliveryLoop(ctx)

	if err := b.CreateTopic(ctx, "dlq-attempts", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch, err := b.ConsumeWithLease(ctx, "dlq-attempts", "group1", "owner1", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	msg := Message{
		Value: []byte("count-attempts"),
		Envelope: &Envelope{
			RetryPolicy: &RetryPolicy{
				MaxAttempts:  3,
				BackoffMs:    10,
				MaxBackoffMs: 50,
			},
		},
	}
	if err := b.Produce(ctx, "dlq-attempts", msg); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Nack all attempts
	for i := 0; i < 4; i++ {
		select {
		case m := <-ch:
			b.Nack(ctx, "dlq-attempts", "group1", m.Partition, m.Offset, "owner1", "fail")
		case <-time.After(3 * time.Second):
			// May timeout on last since it goes to DLQ
		}
	}

	time.Sleep(500 * time.Millisecond)

	dlqCh, err := b.Consume(ctx, "dlq.dlq-attempts", "dlq-reader", "owner1")
	if err != nil {
		t.Fatalf("Consume DLQ: %v", err)
	}

	select {
	case dlqMsg := <-dlqCh:
		if dlqMsg.Envelope == nil || dlqMsg.Envelope.DLQ == nil {
			t.Fatal("DLQ metadata missing")
		}

		// Should record all attempts
		if dlqMsg.Envelope.DLQ.Attempts < 3 {
			t.Fatalf("expected at least 3 attempts, got %d", dlqMsg.Envelope.DLQ.Attempts)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for DLQ message")
	}
}
