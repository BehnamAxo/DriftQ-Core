package broker

import (
	"errors"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/storage"
)

func (b *InMemoryBroker) dispatchLocked(topic string) {
	start := time.Now()
	staged := 0
	defer func() {
		b.observeDispatch(time.Since(start), staged)
	}()

	ts, ok := b.topics[topic]
	if !ok {
		return
	}

	groupChans, ok := b.consumerChans[topic]
	if !ok {
		return
	}

	for group, chans := range groupChans {
		if len(chans) == 0 {
			continue
		}

		if _, ok := b.rrCursor[topic]; !ok {
			b.rrCursor[topic] = make(map[string]int)
		}

		nextByPart := b.ensureNextIndex(topic, group)

		for p := range ts.partitions {
			inflight := b.ensureInFlight(topic, group, p)

			// Resume after last ack if we haven't initialized next index yet
			if _, ok := nextByPart[p]; !ok {
				last := int64(-1)
				if byTopic, ok := b.consumerOffsets[topic]; ok {
					if byGroup, ok := byTopic[group]; ok {
						if v, ok := byGroup[p]; ok {
							last = v
						}
					}
				}

				idx := 0
				for idx < len(ts.partitions[p]) && ts.partitions[p][idx].Offset <= last {
					idx++
				}
				nextByPart[p] = idx
			}

			for nextByPart[p] < len(ts.partitions[p]) {
				// IMPORTANT: don't advance nextByPart until we either
				// (a) skip/advance, or (b) successfully stage delivery
				m := ts.partitions[p][nextByPart[p]]

				// If consume-scope idempotency is already COMMITTED for this (tenant,topic,group,key),
				// treat this message as already done and advance the group's offset without delivering
				if b.idem != nil && m.Envelope != nil && m.Envelope.IdempotencyKey != "" {
					tenantID := m.Envelope.TenantID
					idk := m.Envelope.IdempotencyKey

					if st, ok := b.idem.ConsumeCheck(tenantID, topic, group, idk); ok && st.Status == IdemStatusCommitted {
						if _, ok := b.consumerOffsets[topic]; !ok {
							b.consumerOffsets[topic] = make(map[string]map[int]int64)
						}
						if _, ok := b.consumerOffsets[topic][group]; !ok {
							b.consumerOffsets[topic][group] = make(map[int]int64)
						}

						cur, ok := b.consumerOffsets[topic][group][p]
						if !ok {
							cur = -1
						}

						if m.Offset > cur {
							b.consumerOffsets[topic][group][p] = m.Offset
							b.queueOffsetPersistLocked(topic, group, p, m.Offset)
						}

						b.purgeRetryStateLocked(topic, group, p, m.Offset)

						nextByPart[p]++
						continue
					}
				}

				// If it's already in-flight, DO NOT deliver it again here. Redelivery loop owns retries
				if e, ok := inflight[m.Offset]; ok && e != nil {
					nextByPart[p]++
					continue
				}

				start := b.rrCursor[topic][group] % len(chans)

				var (
					cs        consumerStream
					csIndex   = -1
					lease     time.Duration
					alreadyOK bool
				)

				leaseDefault := b.ackTimeout
				if leaseDefault <= 0 {
					leaseDefault = 2 * time.Second
				}

				hasIdem := b.idem != nil && m.Envelope != nil && m.Envelope.IdempotencyKey != ""

				if !hasIdem {
					csIndex = start
					cs = chans[csIndex]
					b.rrCursor[topic][group] = (csIndex + 1) % len(chans)

					lease = cs.Lease
					// VERY IMPORTANT:
					//   lease == 0  => simple Consume(): no acks, auto-commit
					//   lease < 0   => use default lease
					if lease < 0 {
						lease = leaseDefault
					}
				} else {
					tenantID := m.Envelope.TenantID
					idk := m.Envelope.IdempotencyKey

					var beginErr error
					simpleFallback := -1

					for tries := 0; tries < len(chans); tries++ {
						i := (start + tries) % len(chans)
						cand := chans[i]

						candLease := cand.Lease
						if candLease < 0 {
							candLease = leaseDefault
						}

						// If candLease == 0, this is simple Consume() mode; it can't participate in consume-scope leasing. Remember as fallback.
						if candLease == 0 {
							if simpleFallback == -1 {
								simpleFallback = i
							}
							continue
						}

						alreadyDone, _, err := b.idem.ConsumeBeginLease(tenantID, topic, group, idk, cand.Owner, candLease)
						if err == nil {
							csIndex = i
							cs = cand
							lease = candLease
							alreadyOK = alreadyDone
							b.rrCursor[topic][group] = (i + 1) % len(chans)
							break
						}

						if errors.Is(err, ErrIdempotencyLeaseHeld) {
							continue
						}

						beginErr = err
						break
					}

					if beginErr != nil {
						rs := b.ensureRetryState(topic, group, p)
						rs[m.Offset] = &retryStateEntry{
							LastError:   "idem_begin_failed: " + beginErr.Error(),
							LastErrorAt: time.Now(),
						}
						if b.wal != nil {
							at := time.Now()
							_ = b.appendWALEntry("retry_state", storage.Entry{
								Type:        storage.RecordTypeRetryState,
								Topic:       topic,
								Group:       group,
								Partition:   p,
								Offset:      m.Offset,
								LastError:   "idem_begin_failed: " + beginErr.Error(),
								LastErrorAt: &at,
							})
						}
						break
					}

					// If all candidates were lease-held (or none lease-capable), allow simple fallback
					// (no consume-scope leasing semantics in this mode).
					if csIndex == -1 && simpleFallback != -1 {
						csIndex = simpleFallback
						cs = chans[csIndex]
						lease = 0
						alreadyOK = false
						b.rrCursor[topic][group] = (csIndex + 1) % len(chans)
					}

					// if still no candidate, don't advance nextByPart (don't lose message)
					if csIndex == -1 {
						break
					}

					// If store says "already done", treat like committed skip (Option A)
					if alreadyOK {
						if _, ok := b.consumerOffsets[topic]; !ok {
							b.consumerOffsets[topic] = make(map[string]map[int]int64)
						}
						if _, ok := b.consumerOffsets[topic][group]; !ok {
							b.consumerOffsets[topic][group] = make(map[int]int64)
						}
						cur, ok := b.consumerOffsets[topic][group][p]
						if !ok {
							cur = -1
						}
						if m.Offset > cur {
							b.consumerOffsets[topic][group][p] = m.Offset
							b.queueOffsetPersistLocked(topic, group, p, m.Offset)
						}

						b.purgeRetryStateLocked(topic, group, p, m.Offset)

						nextByPart[p]++
						continue
					}
				}

				// seed error from retryState
				rs := b.ensureRetryState(topic, group, p)
				lastErr := ""
				if st, ok := rs[m.Offset]; ok && st != nil {
					lastErr = st.LastError
				}

				// ✅ SIMPLE MODE: no inflight, no acks, auto-commit offsets once enqueued
				if lease == 0 {
					send := m
					send.Attempts = 1
					send.LastError = lastErr

					// Enqueue to the per-consumer FIFO to preserve ordering
					select {
					case cs.Q <- send:
						staged++
						// commit offset immediately
						if _, ok := b.consumerOffsets[topic]; !ok {
							b.consumerOffsets[topic] = make(map[string]map[int]int64)
						}
						if _, ok := b.consumerOffsets[topic][group]; !ok {
							b.consumerOffsets[topic][group] = make(map[int]int64)
						}

						cur, ok := b.consumerOffsets[topic][group][p]
						if !ok {
							cur = -1
						}

						if m.Offset > cur {
							b.consumerOffsets[topic][group][p] = m.Offset
							b.queueOffsetPersistLocked(topic, group, p, m.Offset)
						}

						b.purgeRetryStateLocked(topic, group, p, m.Offset)

						nextByPart[p]++
						continue
					default:
						// consumer queue full; try later
						return
					}
				}

				// ACK/LEASE MODE: enforce maxInFlight only here
				if b.maxInFlight > 0 && len(inflight) >= b.maxInFlight {
					break
				}

				e := &inflightEntry{
					Msg:       m,
					SentAt:    time.Now(),
					Attempts:  1,
					LastError: lastErr,
					Owner:     cs.Owner,
				}
				inflight[m.Offset] = e

				send := m
				send.Attempts = e.Attempts
				send.LastError = e.LastError

				nextByPart[p]++

				// Enqueue to the per-consumer FIFO to preserve ordering
				select {
				case cs.Q <- send:
					staged++
					// staged
				default:
					// undo and try again later
					delete(inflight, m.Offset)
					nextByPart[p]--
					return
				}
			}
		}
	}
}
