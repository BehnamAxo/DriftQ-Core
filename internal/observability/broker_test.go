package observability

import (
	"context"
	"testing"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/broker"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type fakeBroker struct{}

func (fakeBroker) CreateTopic(ctx context.Context, name string, partitions int) error  { return nil }
func (fakeBroker) ListTopics(ctx context.Context) ([]string, error)                    { return []string{"demo"}, nil }
func (fakeBroker) Produce(ctx context.Context, topic string, msg broker.Message) error { return nil }
func (fakeBroker) Consume(ctx context.Context, topic, group, owner string) (<-chan broker.Message, error) {
	ch := make(chan broker.Message)
	close(ch)
	return ch, nil
}

func (fakeBroker) ConsumeWithLease(ctx context.Context, topic, group, owner string, lease time.Duration) (<-chan broker.Message, error) {
	ch := make(chan broker.Message)
	close(ch)
	return ch, nil
}

func (fakeBroker) Ack(ctx context.Context, topic, group string, partition int, offset int64) error {
	return nil
}

func (fakeBroker) Nack(ctx context.Context, topic, group string, partition int, offset int64, owner string, reason string) error {
	return nil
}

func (fakeBroker) AckIfOwner(ctx context.Context, topic, group string, partition int, offset int64, owner string) error {
	return nil
}

func (fakeBroker) AckCumulativeIfOwner(ctx context.Context, topic, group string, partition int, offset int64, owner string) error {
	return nil
}

type fakeRouter struct{}

func (fakeRouter) Route(ctx context.Context, topic string, msg broker.Message) (broker.RoutingDecision, error) {
	return broker.RoutingDecision{Label: "test", TargetTopic: "target"}, nil
}

func TestWrapBrokerEmitsSpansAndMetrics(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	traceProvider := sdktrace.NewTracerProvider()
	traceProvider.RegisterSpanProcessor(recorder)

	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	tb := WrapBroker(fakeBroker{}, traceProvider, meterProvider)
	ctx := context.Background()
	msg := broker.Message{
		Envelope: &broker.Envelope{
			TenantID: "tenant-a",
			RunID:    "run-1",
			StepID:   "step-1",
		},
	}

	if err := tb.Produce(ctx, "topic-a", msg); err != nil {
		t.Fatalf("Produce failed: %v", err)
	}

	if err := tb.AckIfOwner(ctx, "topic-a", "group-a", 0, 1, "owner-a"); err != nil {
		t.Fatalf("AckIfOwner failed: %v", err)
	}

	assertSpanNames(t, recorder.Ended(), "driftq.broker.produce", "driftq.broker.ack_if_owner")

	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &metrics); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	assertMetricNames(t, metrics, "driftq.broker.operations", "driftq.broker.operation.duration_ms")
}

func TestBrokerMetricsSinkAndRouterTracing(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	traceProvider := sdktrace.NewTracerProvider()
	traceProvider.RegisterSpanProcessor(recorder)
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	prevProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(traceProvider)
	defer otel.SetTracerProvider(prevProvider)

	sink := NewBrokerMetricsSink(meterProvider)
	sink.IncTopicCreated("topic-a")
	sink.IncAck("topic-a", "group-a")
	sink.ObserveDispatch(12*time.Millisecond, 3)

	router := WrapRouter(fakeRouter{}, traceProvider)
	if _, err := router.Route(context.Background(), "topic-a", broker.Message{}); err != nil {
		t.Fatalf("Route failed: %v", err)
	}

	assertSpanNames(t, recorder.Ended(), "driftq.broker.route")

	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &metrics); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}

	assertMetricNames(t, metrics,
		"driftq.broker.topics.created",
		"driftq.broker.acks",
		"driftq.broker.dispatch.duration_ms",
		"driftq.broker.dispatch.staged",
	)
}

func assertSpanNames(t *testing.T, spans []sdktrace.ReadOnlySpan, want ...string) {
	t.Helper()
	got := map[string]bool{}

	for _, span := range spans {
		got[span.Name()] = true
	}

	for _, name := range want {
		if !got[name] {
			t.Fatalf("missing span %q", name)
		}
	}
}

func assertMetricNames(t *testing.T, metrics metricdata.ResourceMetrics, want ...string) {
	t.Helper()
	got := map[string]bool{}
	for _, scopeMetrics := range metrics.ScopeMetrics {
		for _, metric := range scopeMetrics.Metrics {
			got[metric.Name] = true
		}
	}

	for _, name := range want {
		if !got[name] {
			t.Fatalf("missing metric %q", name)
		}
	}
}
