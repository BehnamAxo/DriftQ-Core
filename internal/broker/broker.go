package broker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/debugtypes"
	"github.com/driftq-org/DriftQ-Core/internal/storage"
)

// Broker Options (functional options)
type BrokerOption func(*InMemoryBroker)

func applyBrokerOptions(b *InMemoryBroker, opts []BrokerOption) {
	for _, opt := range opts {
		if opt != nil {
			opt(b)
		}
	}
}

func WithMaxPartitionBytes(n int) BrokerOption {
	return func(b *InMemoryBroker) {
		if n < 0 {
			n = 0
		}
		b.maxPartitionBytes = n
	}
}

// Optional but useful
func WithMaxPartitionMsgs(n int) BrokerOption {
	return func(b *InMemoryBroker) {
		if n < 0 {
			n = 0
		}
		b.maxPartitionMsgs = n
	}
}

func WithMaxInFlight(n int) BrokerOption {
	return func(b *InMemoryBroker) {
		if n < 0 {
			n = 0
		}
		b.maxInFlight = n
	}
}

func WithAckTimeout(d time.Duration) BrokerOption {
	return func(b *InMemoryBroker) {
		if d <= 0 {
			return
		}
		b.ackTimeout = d
	}
}

func WithRedeliverTick(d time.Duration) BrokerOption {
	return func(b *InMemoryBroker) {
		if d <= 0 {
			return
		}
		b.redeliverTick = d
	}
}

func WithRouter(r Router) BrokerOption {
	return func(b *InMemoryBroker) {
		b.router = r
	}
}

func WithMetricsSink(m MetricsSink) BrokerOption {
	return func(b *InMemoryBroker) {
		b.metrics = m
	}
}

// TODO: Move to types
type consumerStream struct {
	Owner string
	Lease time.Duration
	Ch    chan Message // returned to the caller
	Q     chan Message // internal per-consumer FIFO to preserve ordering
}

type offsetFlushKey struct {
	Topic     string
	Group     string
	Partition int
}

// TODO: Move to types
// InMemoryBroker is our first implementation. For sure later we'll replace pieces with WAL, scheduler, partitions, etc
type InMemoryBroker struct {
	mu     sync.RWMutex
	topics map[string]*TopicState

	// topic -> group -> offset -> offset
	consumerOffsets map[string]map[string]map[int]int64

	// topic -> group -> list of chans
	consumerChans map[string]map[string][]consumerStream

	rrCursor map[string]map[string]int

	// max unacked messages allowed per (topic, group, partition)
	maxInFlight int

	// topic -> group -> partition -> set(offset) of currently in-flight (delivered but not acked)
	inFlight map[string]map[string]map[int]map[int64]*inflightEntry

	// topic -> group -> partition -> next index in ts.partitions[partition] to attempt dispatch
	nextIndex map[string]map[string]map[int]int

	wal    storage.WAL
	router Router // If nil, "no brain configured"

	ackTimeout    time.Duration
	redeliverTick time.Duration

	maxPartitionMsgs  int
	maxPartitionBytes int

	idem *IdempotencyStore

	retryState map[string]map[string]map[int]map[int64]*retryStateEntry

	metrics MetricsSink

	lag *LagTracker

	pendingOffsets      map[offsetFlushKey]int64
	offsetFlushInterval time.Duration
	offsetFlushStop     chan struct{}
	offsetFlushDone     chan struct{}
	closeOnce           sync.Once
}

func (b *InMemoryBroker) SetRouter(r Router) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.router = r
}

func (b *InMemoryBroker) SetMetricsSink(m MetricsSink) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.metrics = m
}

func (b *InMemoryBroker) observeWALAppend(kind string, d time.Duration) {
	if obs, ok := b.metrics.(TimingMetricsSink); ok {
		obs.ObserveWALAppend(kind, d)
	}
}

func (b *InMemoryBroker) observeDispatch(d time.Duration, staged int) {
	if obs, ok := b.metrics.(TimingMetricsSink); ok {
		obs.ObserveDispatch(d, staged)
	}
}

func (b *InMemoryBroker) appendWALEntry(kind string, entry storage.Entry) error {
	if b.wal == nil {
		return nil
	}
	start := time.Now()
	err := b.wal.Append(entry)
	b.observeWALAppend(kind, time.Since(start))
	return err
}

func (b *InMemoryBroker) ensureOffsetFlushLoopLocked() {
	if b.wal == nil || b.offsetFlushInterval <= 0 || b.offsetFlushStop != nil {
		return
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	b.offsetFlushStop = stop
	b.offsetFlushDone = done

	go func(stop <-chan struct{}, done chan struct{}) {
		ticker := time.NewTicker(b.offsetFlushInterval)
		defer ticker.Stop()
		defer close(done)

		for {
			select {
			case <-ticker.C:
				_ = b.flushPendingOffsets()
			case <-stop:
				_ = b.flushPendingOffsets()
				return
			}
		}
	}(stop, done)
}

func (b *InMemoryBroker) queueOffsetPersistLocked(topic, group string, partition int, offset int64) {
	if b.wal == nil {
		return
	}

	b.ensureOffsetFlushLoopLocked()

	key := offsetFlushKey{
		Topic:     topic,
		Group:     group,
		Partition: partition,
	}
	if cur, ok := b.pendingOffsets[key]; !ok || offset > cur {
		b.pendingOffsets[key] = offset
	}
}

func (b *InMemoryBroker) flushPendingOffsets() error {
	if b.wal == nil {
		return nil
	}

	b.mu.Lock()
	if len(b.pendingOffsets) == 0 {
		b.mu.Unlock()
		return nil
	}

	pending := b.pendingOffsets
	b.pendingOffsets = make(map[offsetFlushKey]int64)
	b.mu.Unlock()

	keys := make([]offsetFlushKey, 0, len(pending))
	for key := range pending {
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Topic != keys[j].Topic {
			return keys[i].Topic < keys[j].Topic
		}

		if keys[i].Group != keys[j].Group {
			return keys[i].Group < keys[j].Group
		}

		return keys[i].Partition < keys[j].Partition
	})

	failed := make(map[offsetFlushKey]int64)
	for i, key := range keys {
		entry := storage.Entry{
			Type:      storage.RecordTypeOffset,
			Topic:     key.Topic,
			Group:     key.Group,
			Partition: key.Partition,
			Offset:    pending[key],
		}

		if err := b.appendWALEntry("offset", entry); err != nil {
			failed[key] = pending[key]
			for _, rest := range keys[i+1:] {
				failed[rest] = pending[rest]
			}

			b.mu.Lock()
			for failedKey, failedOffset := range failed {
				if cur, ok := b.pendingOffsets[failedKey]; !ok || failedOffset > cur {
					b.pendingOffsets[failedKey] = failedOffset
				}
			}

			b.mu.Unlock()
			return err
		}
	}

	return nil
}

func (b *InMemoryBroker) Close() error {
	var err error

	b.closeOnce.Do(func() {
		b.mu.Lock()
		stop := b.offsetFlushStop
		done := b.offsetFlushDone
		b.mu.Unlock()

		if stop != nil {
			close(stop)
			<-done
		}

		err = b.flushPendingOffsets()
	})

	return err
}

func (b *InMemoryBroker) ConsumerLag(ctx context.Context, group string, topic string) ([]debugtypes.ConsumerLagRow, error) {
	if b.lag == nil {
		return nil, nil
	}
	return b.lag.Snapshot(group, topic), nil
}

// NewInMemoryBroker constructs a broker with defaults, then applies any options.
func NewInMemoryBroker(opts ...BrokerOption) *InMemoryBroker {
	return NewInMemoryBrokerWithWALAndRouter(nil, nil, opts...)
}

// Creates a broker that uses the given WAL but NO router
func NewInMemoryBrokerWithWAL(wal storage.WAL, opts ...BrokerOption) *InMemoryBroker {
	return NewInMemoryBrokerWithWALAndRouter(wal, nil, opts...)
}

// This now lets me plug in both durability and a brain, so passing nil for either is fine (pure in-memory/no routing)
func NewInMemoryBrokerWithWALAndRouter(wal storage.WAL, r Router, opts ...BrokerOption) *InMemoryBroker {
	b := &InMemoryBroker{
		topics:              make(map[string]*TopicState),
		consumerOffsets:     make(map[string]map[string]map[int]int64),
		consumerChans:       make(map[string]map[string][]consumerStream),
		rrCursor:            make(map[string]map[string]int),
		maxInFlight:         32, // default, tune via flags
		inFlight:            make(map[string]map[string]map[int]map[int64]*inflightEntry),
		nextIndex:           make(map[string]map[string]map[int]int),
		wal:                 wal,
		router:              r,
		ackTimeout:          2 * time.Second,
		redeliverTick:       1 * time.Second,
		maxPartitionMsgs:    1024,            // default, tune via flags
		maxPartitionBytes:   4 * 1024 * 1024, // 4MB default, tune via flags
		retryState:          make(map[string]map[string]map[int]map[int64]*retryStateEntry),
		lag:                 NewLagTracker(),
		pendingOffsets:      make(map[offsetFlushKey]int64),
		offsetFlushInterval: 100 * time.Millisecond,
	}

	// default idempotency store (depends on WAL)
	b.idem = NewIdempotencyStoreWithWAL(wal, 10*time.Minute)

	// apply options last so they override defaults (including router/metrics/maxPartitionBytes/etc.)
	applyBrokerOptions(b, opts)

	return b
}

func (b *InMemoryBroker) CreateTopic(_ context.Context, name string, partitions int) error {
	if name == "" {
		return errors.New("topic name cannot be empty")
	}

	if partitions <= 0 {
		return errors.New("partitions must be > 0")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.topics[name]; ok {
		return fmt.Errorf("%w: %s", ErrTopicExists, name)
	}

	// Create in memory first
	if err := b.createTopicLocked(name, partitions); err != nil {
		return err
	}

	// Persist topic metadata so topics with zero messages still exist after restart (this was a bug)
	// Convention: for RecordTypeTopic, Entry.Partition stores the partition count
	if b.wal != nil {
		if err := b.appendWALEntry("topic", storage.Entry{
			Type:      storage.RecordTypeTopic,
			Topic:     name,
			Partition: partitions,
		}); err != nil {
			// Best-effort rollback so caller does NOT think the topic is durable when it is NOT
			delete(b.topics, name)
			return err
		}
	}

	return nil
}

// ListTopics returns the list of topic names (sorted for stability).
func (b *InMemoryBroker) ListTopics(_ context.Context) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.topics))
	for name := range b.topics {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func (b *InMemoryBroker) Produce(ctx context.Context, topic string, msg Message) error {
	if topic == "" {
		return errors.New("topic cannot be empty")
	}

	// Router hook: routing metadata + optional routing controls (target_topic/partition_override)
	// IMPORTANT: skip routing for DLQ messages (DLQ publish must be deterministic)
	if b.router != nil && (msg.Envelope == nil || msg.Envelope.DLQ == nil) {
		decision, err := b.router.Route(ctx, topic, msg)
		if err == nil {
			msg.Routing = &RoutingMetadata{
				Label: decision.Label,
				Meta:  decision.Meta,
			}

			// routing controls -> envelope (router wins)
			if decision.TargetTopic != "" || decision.PartitionOverride != nil {
				if msg.Envelope == nil {
					msg.Envelope = &Envelope{}
				}
				if decision.TargetTopic != "" {
					msg.Envelope.TargetTopic = decision.TargetTopic
				}
				if decision.PartitionOverride != nil {
					msg.Envelope.PartitionOverride = decision.PartitionOverride
				}
			}
		}
	}

	// Deadline enforcement (Reject if already expired)
	if msg.Envelope != nil && msg.Envelope.Deadline != nil {
		if time.Now().After(*msg.Envelope.Deadline) {
			return errors.New("message deadline exceeded")
		}
	}

	// Final topic: envelope target_topic overrides producer topic
	finalTopic := topic
	if msg.Envelope != nil && msg.Envelope.TargetTopic != "" {
		finalTopic = msg.Envelope.TargetTopic
	}

	// Idempotency gate (before any side-effects)
	tenantID := ""
	idemKey := ""
	if msg.Envelope != nil {
		tenantID = msg.Envelope.TenantID
		idemKey = msg.Envelope.IdempotencyKey
	}

	startedIdem := false
	if b.idem != nil && idemKey != "" {
		alreadyCommitted, err := b.idem.Begin(tenantID, finalTopic, idemKey)
		if err != nil {
			return err
		}
		if alreadyCommitted {
			// Treat as success, but do NOT produce a duplicate message
			return nil
		}
		startedIdem = true
	}

	failIdem := func(cause error) {
		if startedIdem && b.idem != nil && idemKey != "" {
			b.idem.Fail(tenantID, finalTopic, idemKey, cause)
		}
	}

	b.mu.Lock()
	err := b.produceLocked(ctx, finalTopic, msg)
	b.mu.Unlock()

	if err != nil {
		failIdem(err)
		return err
	}

	// Mark idempotency as committed ONLY after successful produce
	if startedIdem && b.idem != nil && idemKey != "" {
		b.idem.Commit(tenantID, finalTopic, idemKey, nil)
	}

	return nil
}

// Consume registers a streaming consumer channel for (topic, group, owner).
func (b *InMemoryBroker) Consume(ctx context.Context, topic, group, owner string) (<-chan Message, error) {
	if topic == "" || group == "" {
		return nil, errors.New("topic and group are required")
	}

	owner = strings.TrimSpace(owner)
	if owner == "" {
		return nil, errors.New("owner is required")
	}

	out := make(chan Message, 1024)

	b.mu.Lock()
	if _, ok := b.topics[topic]; !ok {
		b.mu.Unlock()
		close(out)
		return nil, fmt.Errorf("topic not found: %s", topic)
	}

	if b.consumerChans[topic] == nil {
		b.consumerChans[topic] = make(map[string][]consumerStream)
	}

	groupChans := b.consumerChans[topic]
	st := consumerStream{Owner: owner, Lease: 0, Ch: out, Q: out}
	groupChans[group] = append(groupChans[group], st)
	b.consumerChans[topic] = groupChans
	b.mu.Unlock()

	// When consumer disconnects, unregister it and close the stream channel.
	go func() {
		<-ctx.Done()

		b.mu.Lock()
		defer b.mu.Unlock()

		if groupChans, ok := b.consumerChans[topic]; ok {
			streams := groupChans[group]
			for i, st := range streams {
				if st.Ch == out {
					close(st.Q)
					streams[i] = streams[len(streams)-1]
					streams = streams[:len(streams)-1]
					break
				}
			}

			if len(streams) == 0 {
				delete(groupChans, group)
				// Clean up cursor state too
				if b.rrCursor[topic] != nil {
					delete(b.rrCursor[topic], group)
					if len(b.rrCursor[topic]) == 0 {
						delete(b.rrCursor, topic)
					}
				}
			} else {
				groupChans[group] = streams
			}
			if len(groupChans) == 0 {
				delete(b.consumerChans, topic)
			}
		}
	}()

	// Dispatch any pending messages now that a consumer is registered.
	b.mu.Lock()
	b.dispatchLocked(topic)
	b.mu.Unlock()

	return out, nil
}

func (b *InMemoryBroker) Ack(_ context.Context, topic, group string, partition int, offset int64) error {
	if topic == "" {
		return errors.New("topic cannot be empty")
	}

	if group == "" {
		return errors.New("group cannot be empty")
	}

	if partition < 0 {
		return errors.New("partition cannot be negative")
	}

	if offset < 0 {
		return errors.New("offset cannot be negative")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	return b.ackLocked(topic, group, partition, offset)
}

// ackLocked advances group offset/inflight bookkeeping for a single message
// b.mu must already be held by the caller
func (b *InMemoryBroker) ackLocked(topic, group string, partition int, offset int64) error {
	ts, ok := b.topics[topic]
	if !ok {
		return errors.New("topic does not exist")
	}

	if partition >= len(ts.partitions) {
		return errors.New("partition out of range")
	}

	// Verify offsets map exists
	groups, ok := b.consumerOffsets[topic]
	if !ok {
		groups = make(map[string]map[int]int64)
		b.consumerOffsets[topic] = groups
	}

	parts, ok := groups[group]
	if !ok {
		parts = make(map[int]int64)
		groups[group] = parts
	}

	if cur, ok := parts[partition]; ok && offset <= cur {
		// still remove from inflight if present, ack is "done" even if duplicate/late
		inFlight := b.ensureInFlight(topic, group, partition)
		if e, ok := inFlight[offset]; ok && e != nil {
			e.LastError = ""
			m := e.Msg
			m.LastError = ""
			e.Msg = m
		}

		delete(inFlight, offset)
		b.dispatchPartitionLocked(topic, partition)
		return nil
	}

	inFlight := b.ensureInFlight(topic, group, partition)
	if e, ok := inFlight[offset]; ok && e != nil {
		e.LastError = ""
		m := e.Msg
		m.LastError = ""
		e.Msg = m
	}
	delete(inFlight, offset)
	parts[partition] = offset
	b.queueOffsetPersistLocked(topic, group, partition, offset)
	b.dispatchPartitionLocked(topic, partition)

	return nil
}

func (b *InMemoryBroker) AckIfOwner(ctx context.Context, topic, group string, partition int, offset int64, owner string) error {
	if topic == "" {
		return errors.New("topic cannot be empty")
	}

	if group == "" {
		return errors.New("group cannot be empty")
	}

	if partition < 0 {
		return errors.New("partition cannot be negative")
	}

	if offset < 0 {
		return errors.New("offset cannot be negative")
	}

	owner = strings.TrimSpace(owner)
	if owner == "" {
		return errors.New("owner cannot be empty")
	}

	var tenantID, idk string
	b.mu.Lock()

	ts, ok := b.topics[topic]
	if !ok {
		b.mu.Unlock()
		return errors.New("topic does not exist")
	}

	if partition >= len(ts.partitions) {
		b.mu.Unlock()
		return errors.New("partition out of range")
	}

	inFlight := b.ensureInFlight(topic, group, partition)
	e, ok := inFlight[offset]
	if !ok || e == nil {
		b.mu.Unlock()
		return errors.New("message is not in-flight")
	}

	if e.Owner != owner {
		b.mu.Unlock()
		return ErrNotOwner
	}

	if b.idem != nil && e.Msg.Envelope != nil && e.Msg.Envelope.IdempotencyKey != "" {
		tenantID = e.Msg.Envelope.TenantID
		idk = e.Msg.Envelope.IdempotencyKey
	}

	if b.idem == nil || idk == "" {
		err := b.ackLocked(topic, group, partition, offset)
		b.mu.Unlock()
		return err
	}

	b.mu.Unlock()

	if b.idem != nil && idk != "" {
		if err := b.idem.ConsumeCommitIfOwner(tenantID, topic, group, idk, owner, nil); err != nil {

			if err == ErrIdempotencyLeaseHeld {
				return ErrNotOwner
			}

			if err != ErrIdempotencyNotFound {
				return err
			}
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	return b.ackLocked(topic, group, partition, offset)
}

func (b *InMemoryBroker) AckCumulativeIfOwner(ctx context.Context, topic, group string, partition int, offset int64, owner string) error {
	if topic == "" {
		return errors.New("topic cannot be empty")
	}

	if group == "" {
		return errors.New("group cannot be empty")
	}

	if partition < 0 {
		return errors.New("partition cannot be negative")
	}

	if offset < 0 {
		return errors.New("offset cannot be negative")
	}

	owner = strings.TrimSpace(owner)
	if owner == "" {
		return errors.New("owner cannot be empty")
	}

	var start int64

	b.mu.Lock()

	ts, ok := b.topics[topic]
	if !ok {
		b.mu.Unlock()
		return errors.New("topic does not exist")
	}

	if partition >= len(ts.partitions) {
		b.mu.Unlock()
		return errors.New("partition out of range")
	}

	groups, ok := b.consumerOffsets[topic]
	if !ok {
		groups = make(map[string]map[int]int64)
		b.consumerOffsets[topic] = groups
	}

	parts, ok := groups[group]
	if !ok {
		parts = make(map[int]int64)
		groups[group] = parts
	}

	cur, ok := parts[partition]
	if !ok {
		cur = -1
	}
	if offset <= cur {
		b.mu.Unlock()
		return nil
	}

	inFlight := b.ensureInFlight(topic, group, partition)
	for off := cur + 1; off <= offset; off++ {
		e, ok := inFlight[off]
		if !ok || e == nil {
			b.mu.Unlock()
			return errors.New("message range is not fully in-flight")
		}
		if e.Owner != owner {
			b.mu.Unlock()
			return ErrNotOwner
		}
	}

	start = cur + 1
	b.mu.Unlock()

	for off := start; off <= offset; off++ {
		if err := b.AckIfOwner(ctx, topic, group, partition, off, owner); err != nil {
			return err
		}
	}

	return nil
}

func (b *InMemoryBroker) IdempotencyHelper() *IdempotencyConsumerHelper {
	if b == nil {
		return nil
	}

	return NewIdempotencyConsumerHelper(b.idem)
}

func computeBackoff(p *RetryPolicy, retryNumber int) time.Duration {
	if p == nil || p.BackoffMs <= 0 || retryNumber <= 0 {
		return 0
	}

	backoff := time.Duration(p.BackoffMs) * time.Millisecond

	// clamp even for retryNumber=1
	if p.MaxBackoffMs > 0 {
		max := time.Duration(p.MaxBackoffMs) * time.Millisecond
		if backoff > max {
			return max
		}
	}

	// exponential: base * 2^(retryNumber-1)
	for i := 1; i < retryNumber; i++ {
		// If max is set and we're already at it, stop multiplying
		if p.MaxBackoffMs > 0 {
			max := time.Duration(p.MaxBackoffMs) * time.Millisecond
			if backoff >= max {
				return max
			}
		}

		backoff *= 2

		if p.MaxBackoffMs > 0 {
			max := time.Duration(p.MaxBackoffMs) * time.Millisecond
			if backoff > max {
				return max
			}
		}
	}

	return backoff
}

// advanceOffsetLocked updates the consumer offset for a given (topic, group, partition) without grabbing any locks
// Only call this if b.mu is already locked (for example, from inside redeliverExpiredLocked())
//
// Same behavior as Ack():
// - the stored offset is the "last acked offset" for that (topic, group, partition)
// - we only write to the WAL if the offset actually moves forward
// - after advancing, we call dispatchPartitionLocked(topic, partition) to keep messages moving
func (b *InMemoryBroker) advanceOffsetLocked(topic, group string, partition int, offset int64) error {
	if topic == "" {
		return errors.New("topic cannot be empty")
	}

	if group == "" {
		return errors.New("group cannot be empty")
	}

	if partition < 0 {
		return errors.New("partition cannot be negative")
	}

	if offset < 0 {
		return errors.New("offset cannot be negative")
	}

	ts, ok := b.topics[topic]
	if !ok {
		return errors.New("topic does not exist")
	}

	if partition >= len(ts.partitions) {
		return errors.New("partition out of range")
	}

	// Ensure offsets maps exist
	groups, ok := b.consumerOffsets[topic]
	if !ok {
		groups = make(map[string]map[int]int64)
		b.consumerOffsets[topic] = groups
	}

	parts, ok := groups[group]
	if !ok {
		parts = make(map[int]int64)
		groups[group] = parts
	}

	// If we're not advancing, do nothing
	if cur, ok := parts[partition]; ok && offset <= cur {
		return nil
	}

	parts[partition] = offset
	b.queueOffsetPersistLocked(topic, group, partition, offset)

	// Keep messages flowing
	b.dispatchPartitionLocked(topic, partition)
	return nil
}

func (b *InMemoryBroker) Nack(_ context.Context, topic, group string, partition int, offset int64, owner string, reason string) error {
	if topic == "" {
		return errors.New("topic cannot be empty")
	}

	if group == "" {
		return errors.New("group cannot be empty")
	}

	if partition < 0 {
		return errors.New("partition cannot be negative")
	}

	if offset < 0 {
		return errors.New("offset cannot be negative")
	}

	owner = strings.TrimSpace(owner)
	if owner == "" {
		return errors.New("owner cannot be empty")
	}

	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "nack"
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	ts, ok := b.topics[topic]
	if !ok {
		return errors.New("topic does not exist")
	}

	if partition >= len(ts.partitions) {
		return errors.New("partition out of range")
	}

	inFlight := b.ensureInFlight(topic, group, partition)
	e, ok := inFlight[offset]
	if !ok || e == nil {
		return errors.New("message is not in-flight")
	}

	if e.Owner != owner {
		return ErrNotOwner
	}

	now := time.Now()

	// Merge with any existing error (keep original + add new detail)
	merged := appendLastError(e.LastError, reason)

	// 1) Persist retry/failure state (durable) with merged error
	rs := b.ensureRetryState(topic, group, partition)
	rs[offset] = &retryStateEntry{
		LastError:   merged,
		LastErrorAt: now,
	}

	if b.wal != nil {
		at := now
		if err := b.appendWALEntry("retry_state", storage.Entry{
			Type:        storage.RecordTypeRetryState,
			Topic:       topic,
			Group:       group,
			Partition:   partition,
			Offset:      offset,
			LastError:   merged,
			LastErrorAt: &at,
		}); err != nil {
			return err
		}
	}

	// 2) Update inflight bookkeeping so next delivery surfaces merged error immediately
	e.LastError = merged
	m := e.Msg
	m.LastError = merged
	e.Msg = m

	// 3) Best-effort idempotency failure mark
	if b.idem != nil && e.Msg.Envelope != nil && e.Msg.Envelope.IdempotencyKey != "" {
		tenantID := e.Msg.Envelope.TenantID
		idk := e.Msg.Envelope.IdempotencyKey

		if err := b.idem.ConsumeFailIfOwner(tenantID, topic, group, idk, owner, errors.New(reason)); err != nil && err != ErrIdempotencyNotFound {
			// Don't fail the nack; just make it visible
			merged2 := appendLastError(merged, "idem_fail_failed: "+err.Error())

			e.LastError = merged2
			m := e.Msg
			m.LastError = merged2
			e.Msg = m

			// Keep retryState consistent (best-effort WAL append)
			rs[offset] = &retryStateEntry{LastError: merged2, LastErrorAt: now}
			if b.wal != nil {
				at := now
				_ = b.appendWALEntry("retry_state", storage.Entry{
					Type:        storage.RecordTypeRetryState,
					Topic:       topic,
					Group:       group,
					Partition:   partition,
					Offset:      offset,
					LastError:   merged2,
					LastErrorAt: &at,
				})
			}
		}
	}

	// 4) Make it eligible for immediate redelivery (respect per-consumer lease)
	e.NextDeliverAt = time.Time{}

	lease := b.ackTimeout
	if groupStreams, ok := b.consumerChans[topic]; ok {
		if streams, ok := groupStreams[group]; ok {
			for _, st := range streams {
				if st.Owner == e.Owner && st.Lease > 0 {
					lease = st.Lease
					break
				}
			}
		}
	}

	// Push SentAt far enough back so redelivery sees it as expired *now*
	e.SentAt = now.Add(-lease - time.Millisecond)

	// 5) Optional: resend now
	b.redeliverExpiredLocked()

	return nil
}

// Creates a topic assuming b.mu is held
func (b *InMemoryBroker) createTopicLocked(name string, partitions int) error {
	if name == "" {
		return errors.New("topic name cannot be empty")
	}

	if partitions <= 0 {
		return errors.New("partitions must be > 0")
	}

	if _, exists := b.topics[name]; exists {
		return nil
	}

	b.topics[name] = &TopicState{
		partitions:        make([][]Message, partitions),
		partitionByteSums: make([][]int64, partitions),
	}

	return nil
}

// produceLocked produces assuming b.mu is held.
// IMPORTANT: This does NOT run the router hook and does NOT do idempotency Begin/Commit
// Callers decide those behaviors (public Produce does, DLQ publish should NOT)
func (b *InMemoryBroker) produceLocked(_ context.Context, topic string, msg Message) error {
	if topic == "" {
		return errors.New("topic cannot be empty")
	}

	ts, exists := b.topics[topic]
	if !exists {
		return errors.New("topic does not exist (create it first)")
	}

	numPartitions := len(ts.partitions)
	if numPartitions <= 0 {
		return errors.New("topic has no partitions")
	}

	// Partition selection: override > hash(key)
	part := pickPartition(msg.Key, numPartitions)
	if msg.Envelope != nil && msg.Envelope.PartitionOverride != nil {
		po := *msg.Envelope.PartitionOverride
		if po < 0 || po >= numPartitions {
			return errors.New("partition_override out of range")
		}

		part = po
	}

	// Compute slowest ack only if we need it
	slowest := int64(-1)
	if b.maxPartitionMsgs > 0 || b.maxPartitionBytes > 0 {
		slowest = b.slowestAckLocked(topic, part)
	}

	// 1) message-count cap
	if b.maxPartitionMsgs > 0 {
		buffered := bufferedCount(ts.partitions[part], slowest)
		if buffered >= b.maxPartitionMsgs {
			// metrics hook
			if b.metrics != nil {
				b.metrics.IncProduceRejected("partition_buffer_full")
			}

			return &ProducerOverloadError{
				Reason:     "partition_buffer_full",
				RetryAfter: 1 * time.Second,
				Cause:      ErrBackpressure,
			}
		}
	}

	// 2) bytes cap
	if b.maxPartitionBytes > 0 {
		bufferedBytes := bufferedBytesCount(ts.partitions[part], ts.partitionByteSums[part], slowest)
		bufferedBytes += len(msg.Key) + len(msg.Value)

		if bufferedBytes >= b.maxPartitionBytes {
			// metrics hook
			if b.metrics != nil {
				b.metrics.IncProduceRejected("partition_buffer_bytes_full")
			}

			return &ProducerOverloadError{
				Reason:     "partition_buffer_bytes_full",
				RetryAfter: 1 * time.Second,
				Cause:      ErrBackpressure,
			}
		}
	}

	// Kafka-style per-partition offsets: offset == index in that partition’s log
	msg.Partition = part
	msg.Offset = int64(len(ts.partitions[part]))

	// WAL append first (if configured)
	if b.wal != nil {
		entry := storage.Entry{
			Type:      storage.RecordTypeMessage,
			Topic:     topic,
			Partition: part,
			Offset:    msg.Offset,
			Key:       msg.Key,
			Value:     msg.Value,
		}

		// routing metadata
		if msg.Routing != nil {
			entry.RoutingLabel = msg.Routing.Label
			entry.RoutingMeta = msg.Routing.Meta
		}

		// envelope fields
		if msg.Envelope != nil {
			entry.RunID = msg.Envelope.RunID
			entry.StepID = msg.Envelope.StepID
			entry.ParentStepID = msg.Envelope.ParentStepID
			entry.Labels = msg.Envelope.Labels

			entry.TargetTopic = msg.Envelope.TargetTopic
			entry.PartitionOverride = msg.Envelope.PartitionOverride

			entry.IdempotencyKey = msg.Envelope.IdempotencyKey
			entry.Deadline = msg.Envelope.Deadline

			if msg.Envelope.RetryPolicy != nil {
				entry.RetryMaxAttempts = msg.Envelope.RetryPolicy.MaxAttempts
				entry.RetryBackoffMs = msg.Envelope.RetryPolicy.BackoffMs
				entry.RetryMaxBackoffMs = msg.Envelope.RetryPolicy.MaxBackoffMs
			}

			entry.TenantID = msg.Envelope.TenantID

			if msg.Envelope.DLQ != nil {
				entry.DLQOriginalTopic = msg.Envelope.DLQ.OriginalTopic
				entry.DLQOriginalPartition = msg.Envelope.DLQ.OriginalPartition
				entry.DLQOriginalOffset = msg.Envelope.DLQ.OriginalOffset
				entry.DLQAttempts = msg.Envelope.DLQ.Attempts
				entry.DLQLastError = msg.Envelope.DLQ.LastError
				entry.DLQRoutedAtMs = msg.Envelope.DLQ.RoutedAtMs
			}
		}

		if err := b.appendWALEntry("message", entry); err != nil {
			return err
		}
	}

	// Commit to in-memory state (no ts.nextOffset anymore)
	ts.partitions[part] = append(ts.partitions[part], msg)
	msgBytes := int64(len(msg.Key) + len(msg.Value))
	sums := ts.partitionByteSums[part]
	if len(sums) == 0 {
		sums = append(sums, msgBytes)
	} else {
		sums = append(sums, sums[len(sums)-1]+msgBytes)
	}
	ts.partitionByteSums[part] = sums

	// Deliver what we can
	b.dispatchPartitionLocked(topic, part)

	return nil
}

func (b *InMemoryBroker) ConsumeWithLease(ctx context.Context, topic, group, owner string, lease time.Duration) (<-chan Message, error) {
	if topic == "" || group == "" {
		return nil, errors.New("topic and group are required")
	}

	owner = strings.TrimSpace(owner)
	if owner == "" {
		return nil, errors.New("owner is required")
	}

	if lease <= 0 {
		lease = b.ackTimeout
		if lease <= 0 {
			lease = 2 * time.Second
		}
	}

	out := make(chan Message, 1024)

	b.mu.Lock()
	if _, ok := b.topics[topic]; !ok {
		b.mu.Unlock()
		close(out)
		return nil, fmt.Errorf("topic not found: %s", topic)
	}

	if b.consumerChans[topic] == nil {
		b.consumerChans[topic] = make(map[string][]consumerStream)
	}

	groupChans := b.consumerChans[topic]
	st := consumerStream{Owner: owner, Lease: lease, Ch: out, Q: out}
	groupChans[group] = append(groupChans[group], st)
	b.consumerChans[topic] = groupChans
	b.mu.Unlock()

	// Unregister consumer on context cancellation and close the stream channel.
	go func() {
		<-ctx.Done()

		b.mu.Lock()
		defer b.mu.Unlock()

		if groupChans, ok := b.consumerChans[topic]; ok {
			streams := groupChans[group]
			for i, st := range streams {
				if st.Ch == out {
					close(st.Q)
					streams[i] = streams[len(streams)-1]
					streams = streams[:len(streams)-1]
					break
				}
			}

			if len(streams) == 0 {
				delete(groupChans, group)
				if b.rrCursor[topic] != nil {
					delete(b.rrCursor[topic], group)
					if len(b.rrCursor[topic]) == 0 {
						delete(b.rrCursor, topic)
					}
				}
			} else {
				groupChans[group] = streams
			}
			if len(groupChans) == 0 {
				delete(b.consumerChans, topic)
			}
		}
	}()

	// Dispatch any pending messages now that a consumer is registered.
	b.mu.Lock()
	b.dispatchLocked(topic)
	b.mu.Unlock()

	return out, nil
}

func (b *InMemoryBroker) WALEnabled() bool {
	return b.wal != nil
}

func (b *InMemoryBroker) MaxPartitionBytes() int { return b.maxPartitionBytes }

func (b *InMemoryBroker) MaxPartitionMsgs() int { return b.maxPartitionMsgs }

func (b *InMemoryBroker) MaxInFlight() int { return b.maxInFlight }
