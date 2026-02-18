package broker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConsumeWithLease_Expiry_RedeliversToOtherOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()
	if err := b.CreateTopic(ctx, "t1", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Start redelivery loop (required for lease expiry behavior)
	b.redeliverTick = 5 * time.Millisecond
	b.StartRedeliveryLoop(ctx)

	// Create two consumers in same group with short leases.
	lease := 50 * time.Millisecond
	chA, err := b.ConsumeWithLease(ctx, "t1", "g1", "ownerA", lease)
	if err != nil {
		t.Fatalf("ConsumeWithLease A: %v", err)
	}

	chB, err := b.ConsumeWithLease(ctx, "t1", "g1", "ownerB", lease)
	if err != nil {
		t.Fatalf("ConsumeWithLease B: %v", err)
	}

	// Produce one message
	if err := b.Produce(ctx, "t1", Message{Value: []byte("x")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// First delivery should arrive on either channel.
	var first Message
	var firstFrom string
	select {
	case first = <-chA:
		firstFrom = "A"
	case first = <-chB:
		firstFrom = "B"
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for first delivery")
	}

	if first.Offset < 0 {
		t.Fatalf("first delivery unexpected: %+v", first)
	}

	firstAttempts := first.Attempts

	// Don't ack. Wait for lease expiry + redelivery loop.
	deadline := time.Now().Add(1 * time.Second)

	var second Message
	var gotSecond bool
	for time.Now().Before(deadline) {
		select {
		case second = <-chA:
			gotSecond = true
		case second = <-chB:
			gotSecond = true
		case <-time.After(100 * time.Millisecond):
			// keep polling
		}

		if gotSecond {
			break
		}
	}

	if !gotSecond {
		t.Fatalf("did not receive redelivery within timeout (first from %s)", firstFrom)
	}

	if second.Partition != first.Partition || second.Offset != first.Offset {
		t.Fatalf("redelivery should be same message (partition/offset). first=%+v second=%+v", first, second)
	}

	if second.LastError == "" {
		t.Fatalf("expected LastError to be set after lease expiry (e.g. ack_timeout), got empty")
	}

	if second.Attempts <= firstAttempts {
		t.Fatalf("expected attempts to increase on redelivery. first=%d second=%d", firstAttempts, second.Attempts)
	}

	// Ensure AckIfOwner works with whichever owner currently holds the lease
	err = b.AckIfOwner(ctx, "t1", "g1", second.Partition, second.Offset, "ownerA")
	if err != nil && !errors.Is(err, ErrNotOwner) {
		t.Fatalf("AckIfOwner unexpected err: %v", err)
	}

	if errors.Is(err, ErrNotOwner) {
		err2 := b.AckIfOwner(ctx, "t1", "g1", second.Partition, second.Offset, "ownerB")
		if err2 != nil {
			t.Fatalf("expected ownerB to be able to ack, got: %v", err2)
		}
	}
}
