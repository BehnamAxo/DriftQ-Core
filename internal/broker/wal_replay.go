package broker

import (
	"fmt"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/storage"
)

// NewInMemoryBrokerFromWAL builds a broker, then replays whatever is in the WAL
// so I can restore topics, partitions, messages and consumer offsets on startup
func NewInMemoryBrokerFromWAL(wal storage.WAL, opts ...BrokerOption) (*InMemoryBroker, error) {
	b := NewInMemoryBrokerWithWAL(wal, opts...)

	if wal == nil {
		return b, nil
	}

	entries, err := wal.Replay()
	if err != nil {
		return nil, err
	}

	// Rebuild in-memory state from the log
	for _, e := range entries {
		switch e.Type {

		case storage.RecordTypeTopic:
			// Restore topics even if they had zero messages produced. Convention: for RecordTypeTopic, Entry.Partition stores the partition COUNT
			if e.Topic == "" || e.Partition <= 0 {
				continue
			}

			if ts, ok := b.topics[e.Topic]; !ok {
				// Fresh topic from metadata
				_ = b.createTopicLocked(e.Topic, e.Partition)
			} else {
				// Topic already exists (maybe created implicitly from message replay) so ensure it has at least e.Partition partitions.
				for len(ts.partitions) < e.Partition {
					ts.partitions = append(ts.partitions, nil)
					ts.partitionByteSums = append(ts.partitionByteSums, nil)
				}
			}

		case storage.RecordTypeMessage:
			ts, ok := b.topics[e.Topic]
			if !ok {
				ts = &TopicState{
					partitions:        make([][]Message, 0),
					partitionByteSums: make([][]int64, 0),
				}

				b.topics[e.Topic] = ts
			}

			for len(ts.partitions) <= e.Partition {
				ts.partitions = append(ts.partitions, nil)
				ts.partitionByteSums = append(ts.partitionByteSums, nil)
			}

			expected := int64(len(ts.partitions[e.Partition]))
			if e.Offset != expected {
				// NOTE: This almost certainly means the WAL was written by an older version
				// where offsets were not per-partition
				return nil, fmt.Errorf(
					"wal replay: unexpected offset for topic=%q partition=%d: got=%d expected=%d (WAL likely from older version; reset it)",
					e.Topic, e.Partition, e.Offset, expected,
				)
			}

			m := Message{
				Key:       e.Key,
				Partition: e.Partition,
				Value:     e.Value,
				Offset:    expected, // (same as e.Offset, but tied to invariant)
				Envelope:  envelopeFromEntry(e),
			}

			// Restore routing metadata if present
			if e.RoutingLabel != "" || len(e.RoutingMeta) > 0 {
				m.Routing = &RoutingMetadata{
					Label: e.RoutingLabel,
					Meta:  e.RoutingMeta,
				}
			}

			// Rebuild PRODUCE-scope idempotency committed state from WAL (legacy behavior)
			if b.idem != nil && m.Envelope != nil && m.Envelope.IdempotencyKey != "" {
				tenantID := m.Envelope.TenantID // FYI: may be ""
				b.idem.Commit(tenantID, e.Topic, m.Envelope.IdempotencyKey, nil)
			}

			ts.partitions[e.Partition] = append(ts.partitions[e.Partition], m)
			msgBytes := int64(len(m.Key) + len(m.Value))
			sums := ts.partitionByteSums[e.Partition]

			if len(sums) == 0 {
				sums = append(sums, msgBytes)
			} else {
				sums = append(sums, sums[len(sums)-1]+msgBytes)
			}

			ts.partitionByteSums[e.Partition] = sums

		case storage.RecordTypeOffset:
			if _, ok := b.consumerOffsets[e.Topic]; !ok {
				b.consumerOffsets[e.Topic] = make(map[string]map[int]int64)
			}

			if _, ok := b.consumerOffsets[e.Topic][e.Group]; !ok {
				b.consumerOffsets[e.Topic][e.Group] = make(map[int]int64)
			}

			cur, ok := b.consumerOffsets[e.Topic][e.Group][e.Partition]
			if !ok || e.Offset > cur {
				b.consumerOffsets[e.Topic][e.Group][e.Partition] = e.Offset
			}

		case storage.RecordTypeRetryState:
			// (topic, group, partition, offset) -> last_error (+ timestamp)
			if e.Topic == "" || e.Group == "" {
				// group is required for retry state; ignore malformed entries
				continue
			}

			rs := b.ensureRetryState(e.Topic, e.Group, e.Partition)

			at := retryStateEntry{
				LastError: e.LastError,
			}

			if e.LastErrorAt != nil {
				at.LastErrorAt = *e.LastErrorAt
			}

			// Last-write-wins naturally since WAL is replayed in order
			rs[e.Offset] = &at

		case storage.RecordTypeConsumeIdempotency:
			// Durable CONSUME-scope idempotency across restarts
			// Rule: treat all PENDING leases as expired on restart => ignore PENDING on replay
			if b.idem == nil {
				continue
			}

			if e.IdempotencyScope != IdemScopeConsume {
				continue
			}

			if e.Topic == "" || e.Group == "" || e.IdempotencyKey == "" {
				// group + key required for consume-scope
				continue
			}

			updatedAt := time.Now()
			if e.UpdatedAt != nil {
				updatedAt = *e.UpdatedAt
			}

			b.idem.mu.Lock()
			k := idempotencyKey{
				Scope:    IdemScopeConsume,
				TenantID: e.TenantID,
				Topic:    e.Topic,
				Group:    e.Group,
				Key:      e.IdempotencyKey,
			}

			switch e.IdempotencyStatus {
			case IdemStatusCommitted:
				b.idem.items[k] = IdempotencyStatus{
					Status:     IdemStatusCommitted,
					Result:     e.Result,
					LastError:  "",
					UpdatedAt:  updatedAt,
					Owner:      "",
					LeaseUntil: time.Time{},
				}

			case IdemStatusFailed:
				b.idem.items[k] = IdempotencyStatus{
					Status:     IdemStatusFailed,
					Result:     nil,
					LastError:  e.LastError,
					UpdatedAt:  updatedAt,
					Owner:      "",
					LeaseUntil: time.Time{},
				}

			case IdemStatusPending:
				// ignored: pending leases expire on restart
			default:
				// ignore unknown statuses
			}
			b.idem.mu.Unlock()

		default:
			// ignore unknown record types for now
		}
	}

	// IMPORTANT: purge retry state for anything already acked, so old errors don’t "resurrect" after restart
	for topic, byGroup := range b.consumerOffsets {
		for group, byPart := range byGroup {
			for partition, ackedOffset := range byPart {
				if byTopicRS, ok := b.retryState[topic]; ok {
					if byGroupRS, ok := byTopicRS[group]; ok {
						if byPartRS, ok := byGroupRS[partition]; ok {
							for off := range byPartRS {
								if off <= ackedOffset {
									delete(byPartRS, off)
								}
							}

							if len(byPartRS) == 0 {
								delete(byGroupRS, partition)
							}

							if len(byGroupRS) == 0 {
								delete(byTopicRS, group)
							}

							if len(byTopicRS) == 0 {
								delete(b.retryState, topic)
							}
						}
					}
				}
			}
		}
	}

	return b, nil
}

func envelopeFromEntry(e storage.Entry) *Envelope {
	// Decide if we should allocate an envelope at all
	// DLQ presence: avoid "0 is valid" traps for partition/offset. Our DLQ publisher always sets OriginalTopic, and RoutedAtMs is also a strong signal
	hasDLQ := e.DLQOriginalTopic != "" || e.DLQRoutedAtMs != 0 || e.DLQLastError != ""

	has := e.RunID != "" ||
		e.StepID != "" ||
		e.ParentStepID != "" ||
		len(e.Labels) > 0 ||
		e.TargetTopic != "" ||
		e.PartitionOverride != nil ||
		e.IdempotencyKey != "" ||
		e.Deadline != nil ||
		e.RetryMaxAttempts != 0 ||
		e.RetryBackoffMs != 0 ||
		e.RetryMaxBackoffMs != 0 ||
		e.TenantID != "" ||
		hasDLQ

	if !has {
		return nil
	}

	env := &Envelope{
		RunID:             e.RunID,
		StepID:            e.StepID,
		ParentStepID:      e.ParentStepID,
		Labels:            e.Labels,
		TargetTopic:       e.TargetTopic,
		PartitionOverride: e.PartitionOverride,
		IdempotencyKey:    e.IdempotencyKey,
		Deadline:          e.Deadline,
		TenantID:          e.TenantID,
	}

	if e.RetryMaxAttempts != 0 || e.RetryBackoffMs != 0 || e.RetryMaxBackoffMs != 0 {
		env.RetryPolicy = &RetryPolicy{
			MaxAttempts:  e.RetryMaxAttempts,
			BackoffMs:    e.RetryBackoffMs,
			MaxBackoffMs: e.RetryMaxBackoffMs,
		}
	}

	if hasDLQ {
		env.DLQ = &DLQMetadata{
			OriginalTopic:     e.DLQOriginalTopic,
			OriginalPartition: e.DLQOriginalPartition,
			OriginalOffset:    e.DLQOriginalOffset,
			Attempts:          e.DLQAttempts,
			LastError:         e.DLQLastError,
			RoutedAtMs:        e.DLQRoutedAtMs,
		}
	}

	return env
}
