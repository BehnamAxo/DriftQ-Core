package broker

import (
	"context"
	"testing"
	"time"
)

func TestBackoffGatesRedelivery(t *testing.T) {
	b := NewInMemoryBroker()
	b.redeliverTick = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.StartRedeliveryLoop(ctx)

	if err := b.CreateTopic(ctx, "t1", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Lease is short (20ms), but backoff is longer (150ms). We should see:
	// - attempt 1 (initial)
	// - attempt 2 (after lease expiry)
	// - then NO attempt 3 until backoff elapses
	msg := Message{
		Key:   []byte("k"),
		Value: []byte("v"),
		Envelope: &Envelope{
			RetryPolicy: &RetryPolicy{
				BackoffMs:    150,
				MaxBackoffMs: 150,
			},
		},
	}

	if err := b.Produce(ctx, "t1", msg); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	ch, err := b.ConsumeWithLease(ctx, "t1", "g1", "ownerA", 20*time.Millisecond)
	if err != nil {
		t.Fatalf("ConsumeWithLease: %v", err)
	}

	m1 := recvMsg(t, ch, 500*time.Millisecond)
	if m1.Attempts != 1 {
		t.Fatalf("expected Attempts=1, got %d", m1.Attempts)
	}

	m2 := recvMsg(t, ch, 500*time.Millisecond)
	if m2.Attempts != 2 {
		t.Fatalf("expected Attempts=2, got %d", m2.Attempts)
	}
	t2 := time.Now()

	// Should NOT see attempt 3 within a window shorter than the backoff.
	assertNoMsg(t, ch, 100*time.Millisecond)

	m3 := recvMsg(t, ch, 700*time.Millisecond)
	if m3.Attempts != 3 {
		t.Fatalf("expected Attempts=3, got %d", m3.Attempts)
	}

	if dt := time.Since(t2); dt < 130*time.Millisecond {
		t.Fatalf("expected backoff to delay redelivery (~150ms), got %v", dt)
	}
}
