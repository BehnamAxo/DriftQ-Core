package engine

import (
	"context"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	ometric "go.opentelemetry.io/otel/metric"
	otrace "go.opentelemetry.io/otel/trace"
)

type runtimeTelemetry struct {
	tracer otrace.Tracer

	runCounter              ometric.Int64Counter
	runDuration             ometric.Int64Histogram
	nodeCounter             ometric.Int64Counter
	nodeDuration            ometric.Int64Histogram
	authzCounter            ometric.Int64Counter
	riskCounter             ometric.Int64Counter
	governanceCounter       ometric.Int64Counter
	humanTaskCounter        ometric.Int64Counter
	humanWaitDuration       ometric.Int64Histogram
	replayCounter           ometric.Int64Counter
}

func newRuntimeTelemetry(tp otrace.TracerProvider, mp ometric.MeterProvider) *runtimeTelemetry {
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	if mp == nil {
		mp = otel.GetMeterProvider()
	}

	meter := mp.Meter("github.com/driftq-org/DriftQ-Core/internal/engine")
	out := &runtimeTelemetry{
		tracer: tp.Tracer("github.com/driftq-org/DriftQ-Core/internal/engine"),
	}

	out.runCounter, _ = meter.Int64Counter("driftq.engine.runs")
	out.runDuration, _ = meter.Int64Histogram("driftq.engine.run.duration_ms")
	out.nodeCounter, _ = meter.Int64Counter("driftq.engine.nodes")
	out.nodeDuration, _ = meter.Int64Histogram("driftq.engine.node.duration_ms")
	out.authzCounter, _ = meter.Int64Counter("driftq.engine.authz.checks")
	out.riskCounter, _ = meter.Int64Counter("driftq.engine.risk.checks")
	out.governanceCounter, _ = meter.Int64Counter("driftq.engine.governance.checks")
	out.humanTaskCounter, _ = meter.Int64Counter("driftq.engine.human.tasks")
	out.humanWaitDuration, _ = meter.Int64Histogram("driftq.engine.human.wait.duration_ms")
	out.replayCounter, _ = meter.Int64Counter("driftq.engine.replays")
	return out
}

func WithTelemetryProviders(tp otrace.TracerProvider, mp ometric.MeterProvider) RunnerOption {
	return func(r *Runner) {
		r.obs = newRuntimeTelemetry(tp, mp)
	}
}

func (r *Runner) startSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, otrace.Span) {
	if r == nil || r.obs == nil {
		return ctx, otrace.SpanFromContext(ctx)
	}
	return r.obs.startSpan(ctx, name, attrs...)
}

func (o *runtimeTelemetry) startSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, otrace.Span) {
	if o == nil {
		return ctx, otrace.SpanFromContext(ctx)
	}
	return o.tracer.Start(ctx, name, otrace.WithAttributes(filterAttributes(attrs...)...))
}

func (r *Runner) finishSpan(span otrace.Span, err error, attrs ...attribute.KeyValue) {
	if span == nil {
		return
	}
	if len(attrs) > 0 {
		span.SetAttributes(filterAttributes(attrs...)...)
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}

func (r *Runner) observeRunMetric(workflowID string, status RunStatus, dur time.Duration) {
	r.metrics.ObserveRun(status, dur)
	if r == nil || r.obs == nil {
		return
	}
	r.obs.observeRun(workflowID, status, dur)
}

func (r *Runner) observeNodeMetric(workflowID, nodeID string, succeeded bool, dur time.Duration) {
	r.metrics.ObserveNode(nodeID, succeeded, dur)
	if r == nil || r.obs == nil {
		return
	}
	r.obs.observeNode(workflowID, nodeID, succeeded, dur)
}

func (o *runtimeTelemetry) observeRun(workflowID string, status RunStatus, dur time.Duration) {
	attrs := filterAttributes(
		attribute.String("workflow_id", strings.TrimSpace(workflowID)),
		attribute.String("status", string(status)),
	)
	if o.runCounter != nil {
		o.runCounter.Add(context.Background(), 1, ometric.WithAttributes(attrs...))
	}
	if o.runDuration != nil && dur > 0 {
		o.runDuration.Record(context.Background(), dur.Milliseconds(), ometric.WithAttributes(attrs...))
	}
}

func (o *runtimeTelemetry) observeNode(workflowID, nodeID string, succeeded bool, dur time.Duration) {
	status := "failed"
	if succeeded {
		status = "succeeded"
	}
	attrs := filterAttributes(
		attribute.String("workflow_id", strings.TrimSpace(workflowID)),
		attribute.String("node_id", strings.TrimSpace(nodeID)),
		attribute.String("status", status),
	)
	if o.nodeCounter != nil {
		o.nodeCounter.Add(context.Background(), 1, ometric.WithAttributes(attrs...))
	}
	if o.nodeDuration != nil && dur >= 0 {
		o.nodeDuration.Record(context.Background(), dur.Milliseconds(), ometric.WithAttributes(attrs...))
	}
}

func (o *runtimeTelemetry) observeAuthorization(report WorkflowAuthorizationReport) {
	if o == nil || o.authzCounter == nil {
		return
	}
	outcome := "denied"
	if report.Allowed {
		outcome = "allowed"
	}
	o.authzCounter.Add(context.Background(), 1, ometric.WithAttributes(filterAttributes(
		attribute.String("workflow_id", strings.TrimSpace(report.WorkflowID)),
		attribute.String("outcome", outcome),
		attribute.String("mode", string(report.Mode)),
	)...))
}

func (o *runtimeTelemetry) observeRisk(report WorkflowRiskReport) {
	if o == nil || o.riskCounter == nil {
		return
	}
	o.riskCounter.Add(context.Background(), 1, ometric.WithAttributes(filterAttributes(
		attribute.String("workflow_id", strings.TrimSpace(report.WorkflowID)),
		attribute.String("action", string(report.Action)),
		attribute.String("allowed", boolString(report.Allowed)),
	)...))
}

func (o *runtimeTelemetry) observeGovernance(action string, allowed bool) {
	if o == nil || o.governanceCounter == nil {
		return
	}
	o.governanceCounter.Add(context.Background(), 1, ometric.WithAttributes(filterAttributes(
		attribute.String("action", strings.TrimSpace(action)),
		attribute.String("outcome", ternaryString(allowed, "allowed", "denied")),
	)...))
}

func (o *runtimeTelemetry) observeHumanTask(event string, task HumanTask, wait time.Duration) {
	if o == nil {
		return
	}
	attrs := filterAttributes(
		attribute.String("event", strings.TrimSpace(event)),
		attribute.String("mode", string(task.Mode)),
		attribute.String("source", string(task.Source)),
		attribute.String("status", string(task.Status)),
	)
	if o.humanTaskCounter != nil {
		o.humanTaskCounter.Add(context.Background(), 1, ometric.WithAttributes(attrs...))
	}
	if o.humanWaitDuration != nil && wait > 0 {
		o.humanWaitDuration.Record(context.Background(), wait.Milliseconds(), ometric.WithAttributes(attrs...))
	}
}

func (o *runtimeTelemetry) observeReplay(mode ReplayMode, success bool) {
	if o == nil || o.replayCounter == nil {
		return
	}
	o.replayCounter.Add(context.Background(), 1, ometric.WithAttributes(filterAttributes(
		attribute.String("mode", string(mode)),
		attribute.String("outcome", ternaryString(success, "succeeded", "failed")),
	)...))
}

func workflowSpanAttributes(runID, workflowID, tenantID string) []attribute.KeyValue {
	return filterAttributes(
		attribute.String("driftq.run_id", strings.TrimSpace(runID)),
		attribute.String("driftq.workflow_id", strings.TrimSpace(workflowID)),
		attribute.String("driftq.tenant_id", strings.TrimSpace(tenantID)),
	)
}

func nodeSpanAttributes(runID, workflowID, tenantID, nodeID, topic string, attempt int) []attribute.KeyValue {
	return filterAttributes(
		attribute.String("driftq.run_id", strings.TrimSpace(runID)),
		attribute.String("driftq.workflow_id", strings.TrimSpace(workflowID)),
		attribute.String("driftq.tenant_id", strings.TrimSpace(tenantID)),
		attribute.String("driftq.node_id", strings.TrimSpace(nodeID)),
		attribute.String("driftq.topic", strings.TrimSpace(topic)),
		attribute.Int("driftq.attempt", attempt),
	)
}

func principalSpanAttributes(principal Principal) []attribute.KeyValue {
	return filterAttributes(
		attribute.String("driftq.principal_id", strings.TrimSpace(principal.ID)),
		attribute.String("driftq.principal_type", strings.TrimSpace(principal.Type)),
		attribute.String("driftq.tenant_id", strings.TrimSpace(principal.TenantID)),
	)
}

func filterAttributes(attrs ...attribute.KeyValue) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, attr := range attrs {
		if attr.Key == "" {
			continue
		}
		switch attr.Value.Type() {
		case attribute.STRING:
			if strings.TrimSpace(attr.Value.AsString()) == "" {
				continue
			}
		}
		out = append(out, attr)
	}
	return out
}

func boolString(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func ternaryString(ok bool, yes, no string) string {
	if ok {
		return yes
	}
	return no
}
