package broker

import (
	"context"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/storage"
)

func TestConsumerLag_ComputesFromBrokerState(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "demo", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := b.Produce(ctx, "demo", Message{Value: []byte("x")}); err != nil {
			t.Fatalf("Produce(%d): %v", i, err)
		}
	}

	rows, err := b.ConsumerLag(ctx, "bench", "demo")
	if err != nil {
		t.Fatalf("ConsumerLag: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	r := rows[0]
	if r.HeadOffset != 3 {
		t.Fatalf("head_offset=%d want=3", r.HeadOffset)
	}

	if r.CommittedOffset != 0 {
		t.Fatalf("committed_offset=%d want=0", r.CommittedOffset)
	}

	if r.Lag != 3 {
		t.Fatalf("lag=%d want=3", r.Lag)
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch, err := b.ConsumeWithLease(cctx, "demo", "bench", "c1", 5*time.Second)
	if err != nil {
		t.Fatalf("ConsumeWithLease: %v", err)
	}

	var m Message
	select {
	case m = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for consumed message")
	}

	rows, err = b.ConsumerLag(ctx, "bench", "demo")
	if err != nil {
		t.Fatalf("ConsumerLag after lease: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("expected 1 row after lease, got %d", len(rows))
	}

	r = rows[0]
	if !slices.Contains(r.LeaseOwners, "c1") {
		t.Fatalf("lease_owners=%v want c1", r.LeaseOwners)
	}

	if r.LastOwner != "c1" {
		t.Fatalf("last_owner=%q want c1", r.LastOwner)
	}

	if r.LastDeliveredAt <= 0 {
		t.Fatalf("last_delivered_at_ms=%d want > 0", r.LastDeliveredAt)
	}

	if r.LeaseDurationMs <= 0 {
		t.Fatalf("lease_duration_ms=%d want > 0", r.LeaseDurationMs)
	}

	if r.LeaseExpiresAt <= 0 {
		t.Fatalf("lease_expires_at_ms=%d want > 0", r.LeaseExpiresAt)
	}

	if err := b.AckIfOwner(ctx, "demo", "bench", m.Partition, m.Offset, "c1"); err != nil {
		t.Fatalf("AckIfOwner: %v", err)
	}

	rows, err = b.ConsumerLag(ctx, "bench", "demo")
	if err != nil {
		t.Fatalf("ConsumerLag after ack: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("expected 1 row after ack, got %d", len(rows))
	}

	r = rows[0]
	if r.CommittedOffset != m.Offset+1 {
		t.Fatalf("committed_offset=%d want=%d", r.CommittedOffset, m.Offset+1)
	}

	if r.HeadOffset != 3 {
		t.Fatalf("head_offset=%d want=3", r.HeadOffset)
	}

	if r.Inflight < 0 {
		t.Fatalf("inflight=%d want>=0", r.Inflight)
	}
}

func TestConsumerLag_WALReplayStillReturnsRows(t *testing.T) {
	ctx := context.Background()
	walPath := filepath.Join(t.TempDir(), "broker.wal")

	wal, err := storage.OpenFileWAL(walPath)
	if err != nil {
		t.Fatalf("OpenFileWAL(1): %v", err)
	}

	b := NewInMemoryBrokerWithWAL(wal)
	if err := b.CreateTopic(ctx, "demo", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	for i := 0; i < 2; i++ {
		if err := b.Produce(ctx, "demo", Message{Value: []byte("x")}); err != nil {
			t.Fatalf("Produce(%d): %v", i, err)
		}
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("Close(1): %v", err)
	}

	wal2, err := storage.OpenFileWAL(walPath)
	if err != nil {
		t.Fatalf("OpenFileWAL(2): %v", err)
	}
	defer wal2.Close()

	b2, err := NewInMemoryBrokerFromWAL(wal2)
	if err != nil {
		t.Fatalf("NewInMemoryBrokerFromWAL: %v", err)
	}

	rows, err := b2.ConsumerLag(ctx, "bench", "demo")
	if err != nil {
		t.Fatalf("ConsumerLag: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 lag row after replay, got %d", len(rows))
	}

	r := rows[0]
	if r.HeadOffset != 2 {
		t.Fatalf("head_offset=%d want=2", r.HeadOffset)
	}

	if r.CommittedOffset != 0 {
		t.Fatalf("committed_offset=%d want=0", r.CommittedOffset)
	}

	if r.Lag != 2 {
		t.Fatalf("lag=%d want=2", r.Lag)
	}
}

func TestMessageStates_ClassifiesBrokerMessages(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker(WithMaxInFlight(1))

	if err := b.CreateTopic(ctx, "demo", 1); err != nil {
		t.Fatalf("CreateTopic demo: %v", err)
	}

	if err := b.CreateTopic(ctx, "dlq.demo", 1); err != nil {
		t.Fatalf("CreateTopic dlq: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := b.Produce(ctx, "demo", Message{Value: []byte("x")}); err != nil {
			t.Fatalf("Produce demo(%d): %v", i, err)
		}
	}

	if err := b.Produce(ctx, "dlq.demo", Message{Value: []byte("dead")}); err != nil {
		t.Fatalf("Produce dlq: %v", err)
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, err := b.ConsumeWithLease(cctx, "demo", "bench", "ownerA", 5*time.Second)

	if err != nil {
		t.Fatalf("ConsumeWithLease: %v", err)
	}

	first := <-ch
	if err := b.AckIfOwner(ctx, "demo", "bench", first.Partition, first.Offset, "ownerA"); err != nil {
		t.Fatalf("AckIfOwner: %v", err)
	}

	var second Message
	select {
	case second = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for second delivery")
	}

	retries := b.ensureRetryState("demo", "bench", 0)
	retries[2] = &retryStateEntry{LastError: "boom", LastErrorAt: time.Now()}

	rows, err := b.MessageStates(ctx, "bench", "", "all", "", 20)
	if err != nil {
		t.Fatalf("MessageStates: %v", err)
	}

	stateByKey := map[string]string{}
	for _, row := range rows {
		key := row.Topic + ":" + row.State + ":" + strconv.FormatInt(row.Offset, 10)
		stateByKey[key] = row.State
	}

	if stateByKey["demo:acked:0"] != "acked" {
		t.Fatalf("expected acked row for demo offset 0, got %+v", rows)
	}

	if stateByKey["demo:in_flight:1"] != "in_flight" {
		t.Fatalf("expected inflight row for demo offset 1, got %+v", rows)
	}

	if stateByKey["demo:retried:2"] != "retried" {
		t.Fatalf("expected retried row for demo offset 2, got %+v", rows)
	}

	if stateByKey["dlq.demo:dead_lettered:0"] != "dead_lettered" {
		t.Fatalf("expected dead letter row for dlq.demo offset 0, got %+v", rows)
	}

	inflightRows, err := b.MessageStates(ctx, "bench", "demo", "in_flight", "ownerA", 10)
	if err != nil {
		t.Fatalf("MessageStates inflight: %v", err)
	}

	if len(inflightRows) != 1 || inflightRows[0].Owner != "ownerA" || inflightRows[0].Offset != second.Offset {
		t.Fatalf("unexpected inflight rows: %+v", inflightRows)
	}
}
