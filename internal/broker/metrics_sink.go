package broker

import "time"

// MetricsSink is an optional hook for emitting broker metrics and keeps broker decoupled from Prometheus/OpenTelemetry...
type MetricsSink interface {
	IncProduceRejected(reason string)
	IncDLQ(topic string, reason string)
}

// TimingMetricsSink is an optional extension for hot-path latency metrics
type TimingMetricsSink interface {
	ObserveWALAppend(kind string, d time.Duration)
	ObserveDispatch(d time.Duration, staged int)
}
