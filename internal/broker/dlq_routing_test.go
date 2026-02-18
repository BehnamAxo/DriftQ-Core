package broker

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestDLQRoutingAfterMaxAttempts(t *testing.T) {
	b := NewInMemoryBroker()
	// Make timing tight so the test runs fast.
	b.redeliverTick = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.StartRedeliveryLoop(ctx)

	if err := b.CreateTopic(ctx, "t1", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// MaxAttempts=2 means:
	// - attempt 1: initial delivery
	// - attempt 2: first retry delivery
	// - next cycle: message is routed to DLQ (no 3rd delivery to main consumer)
	msg := Message{
		Key:   []byte("k"),
		Value: []byte("v"),
		Envelope: &Envelope{
			TenantID:       "tenant1",
			IdempotencyKey: "idem-1",
			RetryPolicy:    &RetryPolicy{MaxAttempts: 2},
		},
	}

	if err := b.Produce(ctx, "t1", msg); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	cctx, ccancel := context.WithCancel(ctx)
	defer ccancel()

	ch, err := b.ConsumeWithLease(cctx, "t1", "g1", "ownerA", 300*time.Millisecond)
	if err != nil {
		t.Fatalf("ConsumeWithLease: %v", err)
	}

	m1 := recvMsg(t, ch, 500*time.Millisecond)
	if m1.Offset != 0 {
		t.Fatalf("expected first offset=0, got %+v", m1)
	}

	if m1.Attempts != 1 {
		t.Fatalf("expected Attempts=1 on first delivery, got %d", m1.Attempts)
	}

	if err := b.Nack(ctx, "t1", "g1", m1.Partition, m1.Offset, "ownerA", "boom1"); err != nil {
		t.Fatalf("Nack #1: %v", err)
	}

	m2 := recvMsg(t, ch, 500*time.Millisecond)
	if m2.Offset != m1.Offset || m2.Partition != m1.Partition {
		t.Fatalf("expected same message redelivery, got %+v", m2)
	}

	if m2.Attempts != 2 {
		t.Fatalf("expected Attempts=2 on second delivery, got %d", m2.Attempts)
	}

	if err := b.Nack(ctx, "t1", "g1", m2.Partition, m2.Offset, "ownerA", "boom2"); err != nil {
		t.Fatalf("Nack #2: %v", err)
	}

	// At this point the next redelivery cycle should DLQ the message (no third delivery).
	assertNoMsg(t, ch, 80*time.Millisecond)

	dlqTopic := dlqTopicName("t1")

	// IMPORTANT: DLQ messages may still carry IdempotencyKey, and ConsumeWithLease enforces
	// a minimum lease duration (>=250ms) for consume-scope idempotency leasing.
	dlqCh, err := b.ConsumeWithLease(ctx, dlqTopic, "gdlq", "ownerDLQ", 300*time.Millisecond)
	if err != nil {
		t.Fatalf("ConsumeWithLease(dlq): %v", err)
	}

	d := recvMsg(t, dlqCh, 1*time.Second)
	if d.Envelope == nil || d.Envelope.DLQ == nil {
		t.Fatalf("expected DLQ metadata to be present, got envelope=%+v", d.Envelope)
	}

	if d.Envelope.RetryPolicy != nil {
		t.Fatalf("expected RetryPolicy to be cleared when routed to DLQ, got %+v", d.Envelope.RetryPolicy)
	}

	md := d.Envelope.DLQ
	if md.OriginalTopic != "t1" || md.OriginalPartition != m1.Partition || md.OriginalOffset != m1.Offset {
		t.Fatalf("DLQ original pointers unexpected: %+v", md)
	}

	if md.Attempts != 2 {
		t.Fatalf("expected DLQ attempts=2, got %d", md.Attempts)
	}

	if !strings.Contains(md.LastError, "boom1") || !strings.Contains(md.LastError, "boom2") {
		t.Fatalf("expected DLQ last_error to include both reasons; got %q", md.LastError)
	}

	if md.RoutedAtMs <= 0 {
		t.Fatalf("expected DLQ routed_at_ms to be set, got %d", md.RoutedAtMs)
	}

	// Verify in-flight entry was cleared and offset committed.
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.inFlight["t1"] != nil && b.inFlight["t1"]["g1"] != nil && b.inFlight["t1"]["g1"][m1.Partition] != nil {
		if _, ok := b.inFlight["t1"]["g1"][m1.Partition][m1.Offset]; ok {
			t.Fatalf("expected in-flight entry to be cleared after DLQ routing")
		}
	}

	// NOTE: broker stores "last committed offset" (not "next offset to read").
	topicOffsets, ok := b.consumerOffsets["t1"]
	if !ok {
		t.Fatalf("expected consumerOffsets[t1] to exist")
	}

	groupOffsets, ok := topicOffsets["g1"]
	if !ok {
		t.Fatalf("expected consumerOffsets[t1][g1] to exist")
	}

	off, ok := groupOffsets[m1.Partition]
	if !ok {
		t.Fatalf("expected committed offset entry for partition=%d to exist", m1.Partition)
	}

	if off != m1.Offset {
		t.Fatalf("expected committed offset=%d after DLQ routing, got %d", m1.Offset, off)
	}
}
