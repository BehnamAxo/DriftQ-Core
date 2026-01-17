package broker

import (
	"sync"

	"github.com/driftq-org/DriftQ-Core/internal/debugtypes"
)

type lagKey struct {
	topic     string
	partition int
}

type inflightKey struct {
	group     string
	topic     string
	partition int
}

type LagTracker struct {
	mu        sync.RWMutex
	head      map[lagKey]int64
	committed map[inflightKey]int64
	inflight  map[inflightKey]map[int64]struct{}
}

func NewLagTracker() *LagTracker {
	return &LagTracker{
		head:      make(map[lagKey]int64),
		committed: make(map[inflightKey]int64),
		inflight:  make(map[inflightKey]map[int64]struct{}),
	}
}

func (t *LagTracker) OnProduced(topic string, partition int, offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	k := lagKey{topic: topic, partition: partition}
	next := offset + 1
	if cur := t.head[k]; next > cur {
		t.head[k] = next
	}
}

func (t *LagTracker) OnLeased(group, topic string, partition int, offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	k := inflightKey{group: group, topic: topic, partition: partition}
	set := t.inflight[k]
	if set == nil {
		set = make(map[int64]struct{})
		t.inflight[k] = set
	}
	set[offset] = struct{}{}
}

func (t *LagTracker) OnAcked(group, topic string, partition int, offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	k := inflightKey{group: group, topic: topic, partition: partition}
	if set := t.inflight[k]; set != nil {
		delete(set, offset)
		if len(set) == 0 {
			delete(t.inflight, k)
		}
	}

	// simple monotonic commit (good enough for CLI debugging)
	next := offset + 1
	if cur := t.committed[k]; next > cur {
		t.committed[k] = next
	}
}

func (t *LagTracker) OnNacked(group, topic string, partition int, offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	k := inflightKey{group: group, topic: topic, partition: partition}
	if set := t.inflight[k]; set != nil {
		delete(set, offset)
		if len(set) == 0 {
			delete(t.inflight, k)
		}
	}
}

func (t *LagTracker) Snapshot(group string, topic string) []debugtypes.ConsumerLagRow {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// collect all partitions we know about from head map
	out := make([]debugtypes.ConsumerLagRow, 0, len(t.head))

	for hk, head := range t.head {
		if topic != "" && hk.topic != topic {
			continue
		}

		ik := inflightKey{group: group, topic: hk.topic, partition: hk.partition}

		comm := t.committed[ik]
		inflight := int64(0)
		if set := t.inflight[ik]; set != nil {
			inflight = int64(len(set))
		}

		out = append(out, debugtypes.ConsumerLagRow{
			Group:           group,
			Topic:           hk.topic,
			Partition:       hk.partition,
			HeadOffset:      head,
			CommittedOffset: comm,
			Inflight:        inflight,
		})
	}

	return out
}
