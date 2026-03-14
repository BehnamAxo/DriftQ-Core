package observability

import (
	"context"
	"strings"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/broker"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	ometric "go.opentelemetry.io/otel/metric"
	otrace "go.opentelemetry.io/otel/trace"
)

type BrokerMetricsSink struct {
	produceRejected ometric.Int64Counter
	dlqTotal        ometric.Int64Counter
	topicsCreated   ometric.Int64Counter
	acksTotal       ometric.Int64Counter
	nacksTotal      ometric.Int64Counter
	leaseTimeouts   ometric.Int64Counter
	redeliveries    ometric.Int64Counter
	walAppend       ometric.Int64Histogram
	dispatch        ometric.Int64Histogram
	dispatchStaged  ometric.Int64Counter
}

func NewBrokerMetricsSink(mp ometric.MeterProvider) *BrokerMetricsSink {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}

	meter := mp.Meter("github.com/driftq-org/DriftQ-Core/internal/broker")
	sink := &BrokerMetricsSink{}
	sink.produceRejected, _ = meter.Int64Counter("driftq.broker.produce.rejected")
	sink.dlqTotal, _ = meter.Int64Counter("driftq.broker.dlq.messages")
	sink.topicsCreated, _ = meter.Int64Counter("driftq.broker.topics.created")
	sink.acksTotal, _ = meter.Int64Counter("driftq.broker.acks")
	sink.nacksTotal, _ = meter.Int64Counter("driftq.broker.nacks")
	sink.leaseTimeouts, _ = meter.Int64Counter("driftq.broker.lease_timeouts")
	sink.redeliveries, _ = meter.Int64Counter("driftq.broker.redeliveries")
	sink.walAppend, _ = meter.Int64Histogram("driftq.broker.wal_append.duration_ms")
	sink.dispatch, _ = meter.Int64Histogram("driftq.broker.dispatch.duration_ms")
	sink.dispatchStaged, _ = meter.Int64Counter("driftq.broker.dispatch.staged")

	return sink
}

func (b *BrokerMetricsSink) IncProduceRejected(reason string) {
	if b != nil && b.produceRejected != nil {
		b.produceRejected.Add(context.Background(), 1, ometric.WithAttributes(attribute.String("reason", strings.TrimSpace(reason))))
	}
}

func (b *BrokerMetricsSink) IncDLQ(topic, reason string) {
	if b != nil && b.dlqTotal != nil {
		b.dlqTotal.Add(context.Background(), 1, ometric.WithAttributes(attribute.String("topic", strings.TrimSpace(topic)), attribute.String("reason", strings.TrimSpace(reason))))
	}
}

func (b *BrokerMetricsSink) IncTopicCreated(topic string) {
	if b != nil && b.topicsCreated != nil {
		b.topicsCreated.Add(context.Background(), 1, ometric.WithAttributes(attribute.String("topic", strings.TrimSpace(topic))))
	}
}

func (b *BrokerMetricsSink) IncAck(topic, group string) {
	if b != nil && b.acksTotal != nil {
		b.acksTotal.Add(context.Background(), 1, ometric.WithAttributes(attribute.String("topic", strings.TrimSpace(topic)), attribute.String("group", strings.TrimSpace(group))))
	}
}

func (b *BrokerMetricsSink) IncNack(topic, group, reason string) {
	if b != nil && b.nacksTotal != nil {
		b.nacksTotal.Add(context.Background(), 1, ometric.WithAttributes(attribute.String("topic", strings.TrimSpace(topic)), attribute.String("group", strings.TrimSpace(group)), attribute.String("reason", strings.TrimSpace(reason))))
	}
}

func (b *BrokerMetricsSink) IncLeaseTimeout(topic, group string) {
	if b != nil && b.leaseTimeouts != nil {
		b.leaseTimeouts.Add(context.Background(), 1, ometric.WithAttributes(attribute.String("topic", strings.TrimSpace(topic)), attribute.String("group", strings.TrimSpace(group))))
	}
}

func (b *BrokerMetricsSink) IncRedelivery(topic, group, cause string) {
	if b != nil && b.redeliveries != nil {
		b.redeliveries.Add(context.Background(), 1, ometric.WithAttributes(attribute.String("topic", strings.TrimSpace(topic)), attribute.String("group", strings.TrimSpace(group)), attribute.String("cause", strings.TrimSpace(cause))))
	}
}

func (b *BrokerMetricsSink) ObserveWALAppend(kind string, d time.Duration) {
	if b != nil && b.walAppend != nil {
		b.walAppend.Record(context.Background(), d.Milliseconds(), ometric.WithAttributes(attribute.String("kind", strings.TrimSpace(kind))))
	}
}

func (b *BrokerMetricsSink) ObserveDispatch(d time.Duration, staged int) {
	if b != nil && b.dispatch != nil {
		b.dispatch.Record(context.Background(), d.Milliseconds())
	}

	if b != nil && b.dispatchStaged != nil && staged > 0 {
		b.dispatchStaged.Add(context.Background(), int64(staged))
	}
}

type tracedBroker struct {
	next   broker.Broker
	tracer otrace.Tracer
	meter  *brokerRuntimeMetrics
}

type brokerRuntimeMetrics struct {
	opsTotal   ometric.Int64Counter
	opDuration ometric.Int64Histogram
}

func newBrokerRuntimeMetrics(mp ometric.MeterProvider) *brokerRuntimeMetrics {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}

	meter := mp.Meter("github.com/driftq-org/DriftQ-Core/internal/observability/broker")
	out := &brokerRuntimeMetrics{}
	out.opsTotal, _ = meter.Int64Counter("driftq.broker.operations")
	out.opDuration, _ = meter.Int64Histogram("driftq.broker.operation.duration_ms")
	return out
}

func WrapBroker(next broker.Broker, tp otrace.TracerProvider, mp ometric.MeterProvider) broker.Broker {
	if next == nil {
		return nil
	}

	if tp == nil {
		tp = otel.GetTracerProvider()
	}

	return &tracedBroker{
		next:   next,
		tracer: tp.Tracer("github.com/driftq-org/DriftQ-Core/internal/broker"),
		meter:  newBrokerRuntimeMetrics(mp),
	}
}

func (b *tracedBroker) CreateTopic(ctx context.Context, name string, partitions int) error {
	return b.withSpan(ctx, "driftq.broker.topic.create", []attribute.KeyValue{
		attribute.String("topic", strings.TrimSpace(name)),
		attribute.Int("partitions", partitions),
	}, func(ctx context.Context) error {
		return b.next.CreateTopic(ctx, name, partitions)
	})
}

func (b *tracedBroker) ListTopics(ctx context.Context) ([]string, error) {
	var topics []string
	err := b.withSpan(ctx, "driftq.broker.topic.list", nil, func(ctx context.Context) error {
		var err error
		topics, err = b.next.ListTopics(ctx)
		return err
	})

	return topics, err
}

func (b *tracedBroker) Produce(ctx context.Context, topic string, msg broker.Message) error {
	return b.withSpan(ctx, "driftq.broker.produce", brokerAttributes(topic, "", "", msg), func(ctx context.Context) error {
		return b.next.Produce(ctx, topic, msg)
	})
}

func (b *tracedBroker) Consume(ctx context.Context, topic, group, owner string) (<-chan broker.Message, error) {
	return b.ConsumeWithLease(ctx, topic, group, owner, 0)
}

func (b *tracedBroker) ConsumeWithLease(ctx context.Context, topic, group, owner string, lease time.Duration) (<-chan broker.Message, error) {
	var ch <-chan broker.Message
	err := b.withSpan(ctx, "driftq.broker.consume", append(brokerAttributes(topic, group, owner, broker.Message{}),
		attribute.Int64("lease_ms", lease.Milliseconds()),
	), func(ctx context.Context) error {
		var err error
		ch, err = b.next.ConsumeWithLease(ctx, topic, group, owner, lease)
		return err
	})
	return ch, err
}

func (b *tracedBroker) Ack(ctx context.Context, topic, group string, partition int, offset int64) error {
	return b.withSpan(ctx, "driftq.broker.ack", append(brokerAttributes(topic, group, "", broker.Message{}),
		attribute.Int("partition", partition),
		attribute.Int64("offset", offset),
	), func(ctx context.Context) error {
		return b.next.Ack(ctx, topic, group, partition, offset)
	})
}

func (b *tracedBroker) Nack(ctx context.Context, topic, group string, partition int, offset int64, owner string, reason string) error {
	return b.withSpan(ctx, "driftq.broker.nack", append(brokerAttributes(topic, group, owner, broker.Message{}),
		attribute.Int("partition", partition),
		attribute.Int64("offset", offset),
		attribute.String("reason", strings.TrimSpace(reason)),
	), func(ctx context.Context) error {
		return b.next.Nack(ctx, topic, group, partition, offset, owner, reason)
	})
}

func (b *tracedBroker) AckIfOwner(ctx context.Context, topic, group string, partition int, offset int64, owner string) error {
	return b.withSpan(ctx, "driftq.broker.ack_if_owner", append(brokerAttributes(topic, group, owner, broker.Message{}),
		attribute.Int("partition", partition),
		attribute.Int64("offset", offset),
	), func(ctx context.Context) error {
		return b.next.AckIfOwner(ctx, topic, group, partition, offset, owner)
	})
}

func (b *tracedBroker) AckCumulativeIfOwner(ctx context.Context, topic, group string, partition int, offset int64, owner string) error {
	return b.withSpan(ctx, "driftq.broker.ack_cumulative", append(brokerAttributes(topic, group, owner, broker.Message{}),
		attribute.Int("partition", partition),
		attribute.Int64("offset", offset),
	), func(ctx context.Context) error {
		return b.next.AckCumulativeIfOwner(ctx, topic, group, partition, offset, owner)
	})
}

func (b *tracedBroker) withSpan(ctx context.Context, name string, attrs []attribute.KeyValue, fn func(context.Context) error) error {
	start := time.Now()
	ctx, span := b.tracer.Start(ctx, name, otrace.WithAttributes(attrs...))
	err := fn(ctx)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}

	span.End()
	if b.meter != nil {
		result := "ok"
		if err != nil {
			result = "error"
		}

		mattrs := append([]attribute.KeyValue{}, attrs...)
		mattrs = append(mattrs, attribute.String("operation", name), attribute.String("result", result))
		if b.meter.opsTotal != nil {
			b.meter.opsTotal.Add(context.Background(), 1, ometric.WithAttributes(mattrs...))
		}

		if b.meter.opDuration != nil {
			b.meter.opDuration.Record(context.Background(), time.Since(start).Milliseconds(), ometric.WithAttributes(mattrs...))
		}
	}
	return err
}

func brokerAttributes(topic, group, owner string, msg broker.Message) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("topic", strings.TrimSpace(topic)),
		attribute.String("group", strings.TrimSpace(group)),
		attribute.String("owner", strings.TrimSpace(owner)),
	}

	if msg.Envelope != nil {
		attrs = append(attrs,
			attribute.String("tenant_id", strings.TrimSpace(msg.Envelope.TenantID)),
			attribute.String("run_id", strings.TrimSpace(msg.Envelope.RunID)),
			attribute.String("step_id", strings.TrimSpace(msg.Envelope.StepID)),
		)
	}

	out := attrs[:0]
	for _, attr := range attrs {
		if attr.Key == "" {
			continue
		}

		if attr.Value.Type() == attribute.STRING && strings.TrimSpace(attr.Value.AsString()) == "" {
			continue
		}

		out = append(out, attr)
	}
	return out
}

type tracedRouter struct {
	next   broker.Router
	tracer otrace.Tracer
}

func WrapRouter(next broker.Router, tp otrace.TracerProvider) broker.Router {
	if next == nil {
		return nil
	}

	if tp == nil {
		tp = otel.GetTracerProvider()
	}

	return &tracedRouter{
		next:   next,
		tracer: tp.Tracer("github.com/driftq-org/DriftQ-Core/internal/broker/router"),
	}
}

func (r *tracedRouter) Route(ctx context.Context, topic string, msg broker.Message) (broker.RoutingDecision, error) {
	ctx, span := r.tracer.Start(ctx, "driftq.broker.route", otrace.WithAttributes(brokerAttributes(topic, "", "", msg)...))
	defer span.End()
	decision, err := r.next.Route(ctx, topic, msg)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return broker.RoutingDecision{}, err
	}

	span.SetAttributes(
		attribute.String("route.label", strings.TrimSpace(decision.Label)),
		attribute.String("route.target_topic", strings.TrimSpace(decision.TargetTopic)),
	)

	span.SetStatus(codes.Ok, "")
	return decision, nil
}
