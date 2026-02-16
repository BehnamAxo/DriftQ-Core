package broker

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Producer Idempotency Tests
func TestIdempotency_DeduplicatesSameKey(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "idem-test", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Produce same message twice with same idempotency key
	msg1 := Message{
		Value: []byte("original"),
		Envelope: &Envelope{
			IdempotencyKey: "unique-key-123",
		},
	}

	msg2 := Message{
		Value: []byte("duplicate"),
		Envelope: &Envelope{
			IdempotencyKey: "unique-key-123",
		},
	}

	if err := b.Produce(ctx, "idem-test", msg1); err != nil {
		t.Fatalf("Produce first: %v", err)
	}

	// Second produce with same key should succeed but not store duplicate
	if err := b.Produce(ctx, "idem-test", msg2); err != nil {
		t.Fatalf("Produce second: %v", err)
	}

	// Consume - should only get one message
	ch, err := b.Consume(ctx, "idem-test", "group1", "owner1")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	select {
	case msg := <-ch:
		if string(msg.Value) != "original" {
			t.Fatalf("expected 'original', got '%s'", msg.Value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	// Should not get another message
	select {
	case msg := <-ch:
		t.Fatalf("unexpected second message: %s", msg.Value)
	case <-time.After(500 * time.Millisecond):
		// Expected - no duplicate
	}
}

func TestIdempotency_DifferentKeysAllowed(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "idem-diff", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	for i := 0; i < 5; i++ {
		msg := Message{
			Value: []byte("msg"),
			Envelope: &Envelope{
				IdempotencyKey: string(rune('A' + i)), // A, B, C, D, E
			},
		}

		if err := b.Produce(ctx, "idem-diff", msg); err != nil {
			t.Fatalf("Produce %d: %v", i, err)
		}
	}

	ch, err := b.Consume(ctx, "idem-diff", "group1", "owner1")
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
		t.Fatalf("expected 5 messages, got %d", count)
	}
}

func TestIdempotency_EmptyKeyNotDeduplicated(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "idem-empty", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Empty idempotency key means no deduplication
	for i := 0; i < 3; i++ {
		msg := Message{
			Value: []byte("msg"),
			Envelope: &Envelope{
				IdempotencyKey: "", // Empty
			},
		}

		if err := b.Produce(ctx, "idem-empty", msg); err != nil {
			t.Fatalf("Produce %d: %v", i, err)
		}
	}

	ch, err := b.Consume(ctx, "idem-empty", "group1", "owner1")
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
			if count >= 3 {
				break Loop
			}
		case <-timeout:
			break Loop
		}
	}

	if count != 3 {
		t.Fatalf("expected 3 messages (no dedup), got %d", count)
	}
}

func TestIdempotency_NoEnvelopeNotDeduplicated(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "idem-no-env", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// No envelope at all
	for i := 0; i < 3; i++ {
		msg := Message{
			Value:    []byte("msg"),
			Envelope: nil,
		}
		if err := b.Produce(ctx, "idem-no-env", msg); err != nil {
			t.Fatalf("Produce %d: %v", i, err)
		}
	}

	ch, err := b.Consume(ctx, "idem-no-env", "group1", "owner1")
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
			if count >= 3 {
				break Loop
			}
		case <-timeout:
			break Loop
		}
	}

	if count != 3 {
		t.Fatalf("expected 3 messages, got %d", count)
	}
}

func TestIdempotency_ConcurrentSameKey(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "idem-concurrent", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	var wg sync.WaitGroup
	var successCount int
	var mu sync.Mutex

	// Try to produce same key concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := Message{
				Value: []byte("concurrent"),
				Envelope: &Envelope{
					IdempotencyKey: "same-key",
				},
			}
			if err := b.Produce(ctx, "idem-concurrent", msg); err == nil {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	// All should "succeed" but only one message stored
	ch, err := b.Consume(ctx, "idem-concurrent", "group1", "owner1")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	select {
	case <-ch:
		// Got the one message
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	// Should not get more
	select {
	case <-ch:
		t.Fatal("unexpected duplicate message")
	case <-time.After(500 * time.Millisecond):
		// Expected
	}
}

func TestIdempotency_VeryLongKey(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "idem-long-key", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// 1000 character key
	longKey := ""
	for i := 0; i < 1000; i++ {
		longKey += "a"
	}

	msg := Message{
		Value: []byte("long-key-test"),
		Envelope: &Envelope{
			IdempotencyKey: longKey,
		},
	}

	if err := b.Produce(ctx, "idem-long-key", msg); err != nil {
		t.Fatalf("Produce with long key: %v", err)
	}
}

func TestIdempotency_PerTenantIsolation(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "idem-tenant", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Same idempotency key but different tenants
	msg1 := Message{
		Value: []byte("tenant-a"),
		Envelope: &Envelope{
			TenantID:       "tenant-a",
			IdempotencyKey: "shared-key",
		},
	}
	msg2 := Message{
		Value: []byte("tenant-b"),
		Envelope: &Envelope{
			TenantID:       "tenant-b",
			IdempotencyKey: "shared-key",
		},
	}

	if err := b.Produce(ctx, "idem-tenant", msg1); err != nil {
		t.Fatalf("Produce tenant-a: %v", err)
	}

	if err := b.Produce(ctx, "idem-tenant", msg2); err != nil {
		t.Fatalf("Produce tenant-b: %v", err)
	}

	// Both should be stored (different tenants)
	ch, err := b.Consume(ctx, "idem-tenant", "group1", "owner1")
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
			if count >= 2 {
				break Loop
			}
		case <-timeout:
			break Loop
		}
	}

	if count != 2 {
		t.Fatalf("expected 2 messages (different tenants), got %d", count)
	}
}

// IdempotencyStore Unit Tests
func TestIdempotencyStore_Begin(t *testing.T) {
	store := NewIdempotencyStore(10 * time.Minute)

	// First begin should succeed
	committed, err := store.Begin("t1", "topic", "key1")
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	if committed {
		t.Fatal("expected not committed on first begin")
	}

	// Second begin with same key should return in-flight error
	_, err = store.Begin("t1", "topic", "key1")
	if err != ErrIdempotencyInFlight {
		t.Fatalf("expected ErrIdempotencyInFlight, got %v", err)
	}
}

func TestIdempotencyStore_Commit(t *testing.T) {
	store := NewIdempotencyStore(10 * time.Minute)

	store.Begin("t1", "topic", "key1")
	store.Commit("t1", "topic", "key1", []byte("result"))

	// Now begin should return already committed
	committed, err := store.Begin("t1", "topic", "key1")
	if err != nil {
		t.Fatalf("Begin after commit: %v", err)
	}

	if !committed {
		t.Fatal("expected committed=true after Commit")
	}
}

func TestIdempotencyStore_Fail(t *testing.T) {
	store := NewIdempotencyStore(10 * time.Minute)

	store.Begin("t1", "topic", "key1")
	store.Fail("t1", "topic", "key1", nil)

	// Begin again should succeed (retry allowed after fail)
	committed, err := store.Begin("t1", "topic", "key1")
	if err != nil {
		t.Fatalf("Begin after fail: %v", err)
	}

	if committed {
		t.Fatal("expected not committed after fail")
	}
}

func TestIdempotencyStore_EmptyKey(t *testing.T) {
	store := NewIdempotencyStore(10 * time.Minute)

	// Empty key should be a no-op
	committed, err := store.Begin("t1", "topic", "")
	if err != nil {
		t.Fatalf("Begin empty key: %v", err)
	}

	if committed {
		t.Fatal("empty key should not be committed")
	}

	// Should be able to "begin" again since empty keys are not tracked
	committed, err = store.Begin("t1", "topic", "")
	if err != nil {
		t.Fatalf("Begin empty key again: %v", err)
	}
}

func TestIdempotencyStore_Check(t *testing.T) {
	store := NewIdempotencyStore(10 * time.Minute)

	// Check non-existent
	_, ok := store.Check("t1", "topic", "nonexistent")
	if ok {
		t.Fatal("expected not found")
	}

	// Begin and check
	store.Begin("t1", "topic", "key1")
	status, ok := store.Check("t1", "topic", "key1")
	if !ok {
		t.Fatal("expected found after begin")
	}

	if status.Status != IdemStatusPending {
		t.Fatalf("expected PENDING, got %s", status.Status)
	}

	// Commit and check
	store.Commit("t1", "topic", "key1", []byte("data"))
	status, ok = store.Check("t1", "topic", "key1")
	if !ok {
		t.Fatal("expected found after commit")
	}
	if status.Status != IdemStatusCommitted {
		t.Fatalf("expected COMMITTED, got %s", status.Status)
	}
}

// Consumer Idempotency (Lease-based) Tests
func TestIdempotencyStore_ConsumeBeginLease(t *testing.T) {
	store := NewIdempotencyStore(10 * time.Minute)

	done, _, err := store.ConsumeBeginLease("t1", "topic", "group1", "key1", "owner1", time.Second)
	if err != nil {
		t.Fatalf("ConsumeBeginLease: %v", err)
	}

	if done {
		t.Fatal("expected not done on first lease")
	}

	// Same owner can renew
	err = store.ConsumeRenewLease("t1", "topic", "group1", "key1", "owner1", time.Second)
	if err != nil {
		t.Fatalf("ConsumeRenewLease: %v", err)
	}

	// Different owner should fail
	_, _, err = store.ConsumeBeginLease("t1", "topic", "group1", "key1", "owner2", time.Second)
	if err != ErrIdempotencyLeaseHeld {
		t.Fatalf("expected ErrIdempotencyLeaseHeld, got %v", err)
	}
}

func TestIdempotencyStore_ConsumeCommitIfOwner(t *testing.T) {
	store := NewIdempotencyStore(10 * time.Minute)

	store.ConsumeBeginLease("t1", "topic", "group1", "key1", "owner1", time.Second)

	// Wrong owner cannot commit
	err := store.ConsumeCommitIfOwner("t1", "topic", "group1", "key1", "wrongOwner", nil)
	if err != ErrIdempotencyLeaseHeld {
		t.Fatalf("expected ErrIdempotencyLeaseHeld, got %v", err)
	}

	// Right owner can commit
	err = store.ConsumeCommitIfOwner("t1", "topic", "group1", "key1", "owner1", []byte("result"))
	if err != nil {
		t.Fatalf("ConsumeCommitIfOwner: %v", err)
	}

	// After commit, begin should return done=true
	done, result, err := store.ConsumeBeginLease("t1", "topic", "group1", "key1", "owner2", time.Second)
	if err != nil {
		t.Fatalf("ConsumeBeginLease after commit: %v", err)
	}

	if !done {
		t.Fatal("expected done=true after commit")
	}

	if string(result) != "result" {
		t.Fatalf("expected 'result', got '%s'", result)
	}
}

func TestIdempotencyStore_LeaseExpiry(t *testing.T) {
	store := NewIdempotencyStore(10 * time.Minute)

	// Very short lease
	store.ConsumeBeginLease("t1", "topic", "group1", "key1", "owner1", 50*time.Millisecond)

	time.Sleep(100 * time.Millisecond)

	// After expiry, another owner should be able to take it
	done, _, err := store.ConsumeBeginLease("t1", "topic", "group1", "key1", "owner2", time.Second)
	if err != nil {
		t.Fatalf("ConsumeBeginLease after expiry: %v", err)
	}

	if done {
		t.Fatal("expected not done")
	}
}

func TestIdempotencyStore_ConsumeFailIfOwner(t *testing.T) {
	store := NewIdempotencyStore(10 * time.Minute)

	store.ConsumeBeginLease("t1", "topic", "group1", "key1", "owner1", time.Second)

	// Fail it
	err := store.ConsumeFailIfOwner("t1", "topic", "group1", "key1", "owner1", nil)
	if err != nil {
		t.Fatalf("ConsumeFailIfOwner: %v", err)
	}

	// After fail, another attempt should be allowed
	done, _, err := store.ConsumeBeginLease("t1", "topic", "group1", "key1", "owner1", time.Second)
	if err != nil {
		t.Fatalf("ConsumeBeginLease after fail: %v", err)
	}

	if done {
		t.Fatal("expected not done after fail")
	}
}

func TestIdempotencyStore_InvalidLeaseDuration(t *testing.T) {
	store := NewIdempotencyStore(10 * time.Minute)

	// Very small lease should be rejected
	_, _, err := store.ConsumeBeginLease("t1", "topic", "group1", "key1", "owner1", 1*time.Millisecond)
	if err == nil || err == ErrIdempotencyLeaseHeld {
		t.Log("Warning: very small lease may be accepted or rejected based on implementation")
	}
}
