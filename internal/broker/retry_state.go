package broker

import "time"

type retryStateEntry struct {
	LastError   string
	LastErrorAt time.Time
}

func (b *InMemoryBroker) getRetryState(topic, group string, partition int) map[int64]*retryStateEntry {
	byTopic, ok := b.retryState[topic]
	if !ok {
		return nil
	}

	byGroup, ok := byTopic[group]
	if !ok {
		return nil
	}

	byPart, ok := byGroup[partition]
	if !ok {
		return nil
	}

	return byPart
}

func (b *InMemoryBroker) ensureRetryState(topic, group string, partition int) map[int64]*retryStateEntry {
	if _, ok := b.retryState[topic]; !ok {
		b.retryState[topic] = make(map[string]map[int]map[int64]*retryStateEntry)
	}

	if _, ok := b.retryState[topic][group]; !ok {
		b.retryState[topic][group] = make(map[int]map[int64]*retryStateEntry)
	}

	if _, ok := b.retryState[topic][group][partition]; !ok {
		b.retryState[topic][group][partition] = make(map[int64]*retryStateEntry)
	}

	return b.retryState[topic][group][partition]
}

func (b *InMemoryBroker) purgeRetryStateLocked(topic, group string, partition int, ackedOffset int64) {
	byPart := b.getRetryState(topic, group, partition)
	if byPart == nil {
		return
	}

	for off := range byPart {
		if off <= ackedOffset {
			delete(byPart, off)
		}
	}

	if len(byPart) != 0 {
		return
	}

	byGroup := b.retryState[topic][group]
	delete(byGroup, partition)
	if len(byGroup) != 0 {
		return
	}

	byTopic := b.retryState[topic]
	delete(byTopic, group)
	if len(byTopic) != 0 {
		return
	}

	delete(b.retryState, topic)
}
