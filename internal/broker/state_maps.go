package broker

import (
	"sort"
	"time"
)

func (b *InMemoryBroker) ensureInFlight(topic, group string, partition int) map[int64]*inflightEntry {
	if _, ok := b.inFlight[topic]; !ok {
		b.inFlight[topic] = make(map[string]map[int]map[int64]*inflightEntry)
	}

	if _, ok := b.inFlight[topic][group]; !ok {
		b.inFlight[topic][group] = make(map[int]map[int64]*inflightEntry)
	}

	if _, ok := b.inFlight[topic][group][partition]; !ok {
		b.inFlight[topic][group][partition] = make(map[int64]*inflightEntry)
	}

	return b.inFlight[topic][group][partition]
}

func (b *InMemoryBroker) ensureNextIndex(topic, group string) map[int]int {
	if _, ok := b.nextIndex[topic]; !ok {
		b.nextIndex[topic] = make(map[string]map[int]int)
	}

	if _, ok := b.nextIndex[topic][group]; !ok {
		b.nextIndex[topic][group] = make(map[int]int)
	}
	return b.nextIndex[topic][group]
}

func (b *InMemoryBroker) ensureLastDelivery(topic, group string) map[int]deliverySnapshot {
	if _, ok := b.lastDelivery[topic]; !ok {
		b.lastDelivery[topic] = make(map[string]map[int]deliverySnapshot)
	}

	if _, ok := b.lastDelivery[topic][group]; !ok {
		b.lastDelivery[topic][group] = make(map[int]deliverySnapshot)
	}

	return b.lastDelivery[topic][group]
}

func (b *InMemoryBroker) recordDeliveryLocked(topic, group string, partition int, owner string, at time.Time) {
	lastByPart := b.ensureLastDelivery(topic, group)
	lastByPart[partition] = deliverySnapshot{
		Owner: owner,
		At:    at,
	}
}

func (b *InMemoryBroker) ownerLeaseByTopicGroupLocked(topic, group string) map[string]time.Duration {
	out := map[string]time.Duration{}
	if byGroup, ok := b.consumerChans[topic]; ok {
		if chans, ok := byGroup[group]; ok {
			for _, stream := range chans {
				lease := stream.Lease
				if lease <= 0 {
					lease = b.ackTimeout
				}
				out[stream.Owner] = lease
			}
		}
	}
	return out
}

func (b *InMemoryBroker) partitionLeaseSnapshotLocked(topic, group string, partition int, now time.Time) (owners []string, oldestAgeMs int64, leaseDurationMs int64, leaseExpiresAtMs int64, stalled bool) {
	byGroup, ok := b.inFlight[topic]
	if !ok {
		return nil, 0, 0, 0, false
	}

	byPart, ok := byGroup[group]
	if !ok {
		return nil, 0, 0, 0, false
	}

	byOff, ok := byPart[partition]
	if !ok || len(byOff) == 0 {
		return nil, 0, 0, 0, false
	}

	ownerLease := b.ownerLeaseByTopicGroupLocked(topic, group)
	ownerSet := map[string]struct{}{}
	var oldest *inflightEntry

	for _, entry := range byOff {
		if entry == nil {
			continue
		}

		if entry.Owner != "" {
			ownerSet[entry.Owner] = struct{}{}
		}

		if oldest == nil || entry.SentAt.Before(oldest.SentAt) {
			oldest = entry
		}
	}

	if len(ownerSet) > 0 {
		owners = make([]string, 0, len(ownerSet))
		for owner := range ownerSet {
			owners = append(owners, owner)
		}

		sort.Strings(owners)
	}

	if oldest == nil || oldest.SentAt.IsZero() {
		return owners, 0, 0, 0, false
	}

	oldestAgeMs = now.Sub(oldest.SentAt).Milliseconds()
	leaseDuration := ownerLease[oldest.Owner]

	if leaseDuration <= 0 {
		leaseDuration = b.ackTimeout
	}

	leaseDurationMs = leaseDuration.Milliseconds()
	leaseExpiresAtMs = oldest.SentAt.Add(leaseDuration).UnixMilli()
	stalled = leaseDurationMs > 0 && oldestAgeMs > leaseDurationMs

	return owners, oldestAgeMs, leaseDurationMs, leaseExpiresAtMs, stalled
}
