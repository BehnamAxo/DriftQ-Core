package broker

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Chaos Tests (Simulating Real-World Failure Scenarios)
func TestChaos_SlowConsumerDoesNotBlockProducer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "slow", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Start slow consumer
	ch, _ := b.ConsumeWithLease(ctx, "slow", "g", "slow-owner", 10*time.Second)
	go func() {
		for msg := range ch {
			// VERY slow processing
			time.Sleep(5 * time.Second)
			_ = msg
		}
	}()

	// Producer should not block
	done := make(chan bool)
	go func() {
		for i := 0; i < 100; i++ {
			b.Produce(ctx, "slow", Message{Value: []byte("fast")})
		}
		done <- true
	}()

	select {
	case <-done:
		// Good - producer didn't block woohoo
	case <-time.After(5 * time.Second):
		t.Fatal("producer blocked on slow consumer")
	}
}

func TestChaos_ConcurrentProduceConsumeAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()
	b.StartRedeliveryLoop(ctx)

	if err := b.CreateTopic(ctx, "chaos", 4); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	var produced int32
	var consumed int32
	var acked int32

	// Multiple producers
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				if err := b.Produce(ctx, "chaos", Message{Value: []byte("msg")}); err == nil {
					atomic.AddInt32(&produced, 1)
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}

	// Multiple consumers
	for i := 0; i < 3; i++ {
		go func(n int) {
			ch, err := b.ConsumeWithLease(ctx, "chaos", "g1", string(rune('a'+n)), 5*time.Second)
			if err != nil {
				return
			}

			for {
				select {
				case msg := <-ch:
					atomic.AddInt32(&consumed, 1)
					// Random delay before ack
					time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
					if err := b.Ack(ctx, "chaos", "g1", msg.Partition, msg.Offset); err == nil {
						atomic.AddInt32(&acked, 1)
					}
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	// Run for 3 seconds
	time.Sleep(3 * time.Second)
	cancel()

	p := atomic.LoadInt32(&produced)
	c := atomic.LoadInt32(&consumed)
	a := atomic.LoadInt32(&acked)

	t.Logf("Produced: %d, Consumed: %d, Acked: %d", p, c, a)

	if p == 0 {
		t.Fatal("no messages produced")
	}
}

func TestChaos_RedeliveryUnderLoad(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()
	b.StartRedeliveryLoop(ctx)

	if err := b.CreateTopic(ctx, "redelivery-chaos", 2); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch, _ := b.ConsumeWithLease(ctx, "redelivery-chaos", "g1", "o1", 200*time.Millisecond)

	// Produce messages
	for i := 0; i < 50; i++ {
		b.Produce(ctx, "redelivery-chaos", Message{Value: []byte{byte(i)}})
	}

	// Randomly ack/nack/ignore
	var acked, nacked, ignored int32
	go func() {
		for {
			select {
			case msg := <-ch:
				r := rand.Intn(3)
				switch r {
				case 0:
					b.Ack(ctx, "redelivery-chaos", "g1", msg.Partition, msg.Offset)
					atomic.AddInt32(&acked, 1)
				case 1:
					b.Nack(ctx, "redelivery-chaos", "g1", msg.Partition, msg.Offset, "o1", "random-nack")
					atomic.AddInt32(&nacked, 1)
				default:
					// Ignoring it here to let lease expire
					atomic.AddInt32(&ignored, 1)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	time.Sleep(3 * time.Second)
	cancel()

	a := atomic.LoadInt32(&acked)
	n := atomic.LoadInt32(&nacked)
	ig := atomic.LoadInt32(&ignored)

	t.Logf("Acked: %d, Nacked: %d, Ignored: %d", a, n, ig)
}

func TestChaos_ConsumerGroupRebalance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()
	b.StartRedeliveryLoop(ctx)

	if err := b.CreateTopic(ctx, "rebalance", 4); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Produce messages
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				b.Produce(ctx, "rebalance", Message{Value: []byte("msg")})
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	var totalConsumed int32
	var mu sync.Mutex
	consumers := make(map[int]context.CancelFunc)

	// Dynamically add/remove consumers
	go func() {
		consumerID := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
				mu.Lock()
				if rand.Intn(2) == 0 && len(consumers) < 5 {
					// Add consumer
					cctx, ccancel := context.WithCancel(ctx)
					id := consumerID
					consumerID++
					consumers[id] = ccancel

					go func(consumerCtx context.Context, cid int) {
						ch, err := b.ConsumeWithLease(consumerCtx, "rebalance", "g1", string(rune('a'+cid)), time.Second)
						if err != nil {
							return
						}

						for {
							select {
							case msg := <-ch:
								atomic.AddInt32(&totalConsumed, 1)
								b.Ack(consumerCtx, "rebalance", "g1", msg.Partition, msg.Offset)
							case <-consumerCtx.Done():
								return
							}
						}
					}(cctx, id)
				} else if len(consumers) > 1 {
					// Remove random consumer
					for id, cancelFn := range consumers {
						cancelFn()
						delete(consumers, id)
						break
					}
				}
				mu.Unlock()
			}
		}
	}()

	time.Sleep(3 * time.Second)
	cancel()

	t.Logf("Total consumed across rebalances: %d", atomic.LoadInt32(&totalConsumed))
}

func TestChaos_HighContentionOnSinglePartition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()
	b.StartRedeliveryLoop(ctx)

	// Single partition = high contention
	if err := b.CreateTopic(ctx, "contention", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	var wg sync.WaitGroup
	var produced, errors int32

	// Many concurrent producers
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if err := b.Produce(ctx, "contention", Message{Value: []byte("x")}); err != nil {
					atomic.AddInt32(&errors, 1)
				} else {
					atomic.AddInt32(&produced, 1)
				}
			}
		}()
	}

	wg.Wait()

	p := atomic.LoadInt32(&produced)
	e := atomic.LoadInt32(&errors)

	t.Logf("Produced: %d, Errors: %d", p, e)

	if p == 0 {
		t.Fatal("no messages produced under contention")
	}
}

func TestChaos_BurstFollowedByQuiet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()
	b.StartRedeliveryLoop(ctx)

	if err := b.CreateTopic(ctx, "burst", 2); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch, _ := b.ConsumeWithLease(ctx, "burst", "g1", "o1", 5*time.Second)

	var consumed int32
	go func() {
		for {
			select {
			case msg := <-ch:
				atomic.AddInt32(&consumed, 1)
				b.Ack(ctx, "burst", "g1", msg.Partition, msg.Offset)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Burst
	for i := 0; i < 1000; i++ {
		b.Produce(ctx, "burst", Message{Value: []byte("burst")})
	}

	// Quiet period
	time.Sleep(2 * time.Second)

	// Another burst
	for i := 0; i < 500; i++ {
		b.Produce(ctx, "burst", Message{Value: []byte("burst2")})
	}

	time.Sleep(2 * time.Second)
	cancel()

	t.Logf("Consumed after burst patterns: %d", atomic.LoadInt32(&consumed))
}

func TestChaos_IdempotencyUnderConcurrentDuplicates(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	if err := b.CreateTopic(ctx, "idem-chaos", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	var wg sync.WaitGroup
	var succeeded int32

	// Many goroutines trying same idempotency key :)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := Message{
				Value: []byte("duplicate"),
				Envelope: &Envelope{
					IdempotencyKey: "same-key",
				},
			}

			if err := b.Produce(ctx, "idem-chaos", msg); err == nil {
				atomic.AddInt32(&succeeded, 1)
			}
		}()
	}

	wg.Wait()

	// All should "succeed" but only one message stored
	ch, _ := b.Consume(ctx, "idem-chaos", "g1", "o1")

	count := 0
	timeout := time.After(time.Second)
Loop:
	for {
		select {
		case <-ch:
			count++
		case <-timeout:
			break Loop
		}
	}

	t.Logf("Succeeded: %d, Actual messages: %d", atomic.LoadInt32(&succeeded), count)

	if count > 1 {
		t.Fatalf("idempotency violated: %d duplicates", count)
	}
}

func TestChaos_RapidCreateTopics(t *testing.T) {
	ctx := context.Background()
	b := NewInMemoryBroker()

	var wg sync.WaitGroup
	var created, errors int32

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			// Some duplicates
			name := string(rune('a' + n%26))

			if err := b.CreateTopic(ctx, name, 1); err != nil {
				atomic.AddInt32(&errors, 1)
			} else {
				atomic.AddInt32(&created, 1)
			}
		}(i)
	}

	wg.Wait()

	c := atomic.LoadInt32(&created)
	e := atomic.LoadInt32(&errors)
	t.Logf("Created: %d, Errors (expected some duplicates): %d", c, e)

	topics, _ := b.ListTopics(ctx)
	if len(topics) == 0 {
		t.Fatal("no topics created")
	}
}
