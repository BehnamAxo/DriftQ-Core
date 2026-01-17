package broker

import (
	"context"
	"errors"
	"sort"
	"strings"
)

// ConsumerLagRows is intentionally plain (no engine imports)
// It returns per-partition lag for a (group, topic)
type ConsumerLagRow struct {
	Group           string `json:"group"`
	Topic           string `json:"topic"`
	Partition       int    `json:"partition"`
	HeadOffset      int64  `json:"head_offset"`
	CommittedOffset int64  `json:"committed_offset"`
	Lag             int64  `json:"lag"`
	Inflight        int64  `json:"inflight"`
}

// IMPORTANT: the signature MUST match what /debug/topics/lag type-asserts to
func (b *InMemoryBroker) ConsumerLag(ctx context.Context, group string, topic string) ([]ConsumerLagRow, error) {
	_ = ctx

	group = strings.TrimSpace(group)
	topic = strings.TrimSpace(topic)

	if group == "" {
		return nil, errors.New("group is required")
	}
	if topic == "" {
		return nil, errors.New("topic is required")
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	ts, ok := b.topics[topic]
	if !ok {
		return nil, errors.New("topic does not exist")
	}

	// committed offsets (next-to-consume) per partition
	var committedByPart map[int]int64
	if byTopic, ok := b.consumerOffsets[topic]; ok {
		if byGroup, ok := byTopic[group]; ok {
			committedByPart = byGroup
		}
	}

	// inflight per partition: map[offset]*inflightEntry
	var inflightByPart map[int]map[int64]*inflightEntry
	if byTopic, ok := b.inFlight[topic]; ok {
		if byGroup, ok := byTopic[group]; ok {
			inflightByPart = byGroup
		}
	}

	parts := len(ts.partitions)
	out := make([]ConsumerLagRow, 0, parts)

	for p := range parts {
		msgs := ts.partitions[p]

		head := int64(0)
		if len(msgs) > 0 {
			head = msgs[len(msgs)-1].Offset + 1
		}

		committed := int64(0)
		if committedByPart != nil {
			committed = committedByPart[p]
		}

		inflight := int64(0)
		if inflightByPart != nil {
			if m := inflightByPart[p]; m != nil {
				inflight = int64(len(m))
			}
		}

		lag := head - committed
		if lag < 0 {
			lag = 0
		}

		out = append(out, ConsumerLagRow{
			Group:           group,
			Topic:           topic,
			Partition:       p,
			HeadOffset:      head,
			CommittedOffset: committed,
			Lag:             lag,
			Inflight:        inflight,
		})
	}

	// keep output stable for tests/CLI
	sort.Slice(out, func(i, j int) bool { return out[i].Partition < out[j].Partition })

	return out, nil
}
