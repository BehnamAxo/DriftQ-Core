package broker

import (
	"context"
	"errors"
	"testing"
)

func TestProduce_Backpressure_WhenPartitionBufferFull(t *testing.T) {
	b := NewInMemoryBroker()
	ctx := context.Background()

	if err := b.CreateTopic(ctx, "t1", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	limit := b.MaxPartitionMsgs()
	if limit <= 0 {
		t.Fatalf("expected positive default maxPartitionMsgs, got %d", limit)
	}

	// Fill to the broker's default maxPartitionMsgs.
	for i := 0; i < limit; i++ {
		if err := b.Produce(ctx, "t1", Message{Value: []byte("x")}); err != nil {
			t.Fatalf("produce %d unexpected err: %v", i, err)
		}
	}

	// Next should be rejected with ProducerOverloadError (ErrProducerOverloaded)
	err := b.Produce(ctx, "t1", Message{Value: []byte("y")})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	if !errors.Is(err, ErrProducerOverloaded) {
		t.Fatalf("expected ErrProducerOverloaded, got %v", err)
	}

	var poe *ProducerOverloadError
	if !errors.As(err, &poe) {
		t.Fatalf("expected *ProducerOverloadError, got %T", err)
	}

	if poe.Reason == "" {
		t.Fatalf("expected overload reason to be set")
	}

	if poe.RetryAfter <= 0 {
		t.Fatalf("expected retry_after > 0, got %v", poe.RetryAfter)
	}
}
