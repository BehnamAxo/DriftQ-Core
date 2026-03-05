package broker

import (
	"math"
	"strings"
)

func pickPartition(key []byte, numPartitions int) int {
	if len(key) == 0 {
		return 0
	}

	// Allocation-free FNV-1a hash for hot produce path
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)

	h := uint32(offset32)
	for _, b := range key {
		h ^= uint32(b)
		h *= prime32
	}

	return int(h % uint32(numPartitions))
}

func bufferedCount(partMsgs []Message, slowestAck int64) int {
	start := int(slowestAck) + 1
	if start < 0 {
		start = 0
	}

	if start >= len(partMsgs) {
		return 0
	}

	return len(partMsgs) - start
}

func bufferedBytesCount(partMsgs []Message, partByteSums []int64, slowestAck int64) int {
	if len(partMsgs) == 0 {
		return 0
	}

	// Safety fallback for older/partial state, should not happen in normal flow.
	if len(partByteSums) != len(partMsgs) {
		bytes := 0
		start := int(slowestAck) + 1
		if start < 0 {
			start = 0
		}

		for i := start; i < len(partMsgs); i++ {
			bytes += len(partMsgs[i].Key) + len(partMsgs[i].Value)
		}
		return bytes
	}

	start := int(slowestAck) + 1
	if start < 0 {
		start = 0
	}

	if start >= len(partMsgs) {
		return 0
	}

	total := partByteSums[len(partByteSums)-1]
	if start == 0 {
		return int(total)
	}

	before := partByteSums[start-1]
	return int(total - before)
}

func (b *InMemoryBroker) slowestAckLocked(topic string, partition int) int64 {
	byGroup, ok := b.consumerOffsets[topic]
	if !ok || len(byGroup) == 0 {
		return -1
	}

	slowest := int64(math.MaxInt64)
	seen := false

	for _, byPart := range byGroup {
		off, ok := byPart[partition]
		if !ok {
			off = -1
		}

		if !seen || off < slowest {
			slowest = off
			seen = true
		}
	}

	if !seen {
		return -1
	}

	return slowest
}

func appendLastError(existing, addition string) string {
	addition = strings.TrimSpace(addition)
	if addition == "" {
		return existing
	}

	existing = strings.TrimSpace(existing)
	if existing == "" {
		return addition
	}

	return existing + " | " + addition
}
