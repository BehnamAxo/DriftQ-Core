package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestRunnerEmitsCoreObservabilitySignals(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	traceProvider := sdktrace.NewTracerProvider()
	traceProvider.RegisterSpanProcessor(recorder)

	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))

	runner := NewRunner(NewMemoryStore(), WithTelemetryProviders(traceProvider, meterProvider))
	runner.SetArtifactStore(NewMemoryArtifactStore())
	runner.SetArtifactInlineLimit(1)
	ctx := WithTenantID(context.Background(), "tenant-a")
	ctx = WithPrincipal(ctx, Principal{
		ID:           "user-1",
		TenantID:     "tenant-a",
		TenantScopes: []string{"tenant-a"},
	})

	workflow := Workflow{
		WorkflowID: "wf-core-otel",
		Nodes: []NodeDef{
			{
				NodeID: "node-1",
				Topic:  "noop",
				Run: func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
					time.Sleep(5 * time.Millisecond)
					return json.RawMessage(`{"ok":true,"payload":"artifact-worthy"}`), nil
				},
			},
		},
	}

	if err := runner.RunWorkflow(ctx, "run-core-otel", workflow, json.RawMessage(`{"hello":"world"}`)); err != nil {
		t.Fatalf("RunWorkflow failed: %v", err)
	}

	if _, _, err := runner.PutArtifact(ctx, []byte(`artifact-payload`), ArtifactMeta{
		ContentType: "application/json",
		WorkflowID:  "wf-core-otel",
		NodeID:      "node-1",
	}); err != nil {
		t.Fatalf("PutArtifact failed: %v", err)
	}

	spans := recorder.Ended()
	assertSpanNames(t, spans,
		"driftq.workflow.run",
		"driftq.authz.evaluate",
		"driftq.risk.evaluate",
		"driftq.governance.check",
		"driftq.node.execute",
		"driftq.tool.execute",
		"driftq.artifact.put",
	)

	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &metrics); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}

	assertMetricNames(t, metrics,
		"driftq.engine.runs",
		"driftq.engine.run.duration_ms",
		"driftq.engine.nodes",
		"driftq.engine.node.duration_ms",
		"driftq.engine.tools",
		"driftq.engine.tool.duration_ms",
		"driftq.engine.artifacts",
		"driftq.engine.artifact.duration_ms",
		"driftq.engine.authz.checks",
		"driftq.engine.risk.checks",
		"driftq.engine.governance.checks",
	)
}

func TestHumanAndReplayObservabilitySignals(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	traceProvider := sdktrace.NewTracerProvider()
	traceProvider.RegisterSpanProcessor(recorder)

	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))

	runner := NewRunner(NewMemoryStore(), WithTelemetryProviders(traceProvider, meterProvider))
	ctx := WithTenantID(context.Background(), "tenant-a")
	ctx = WithPrincipal(ctx, Principal{
		ID:           "reviewer-1",
		TenantID:     "tenant-a",
		TenantScopes: []string{"tenant-a"},
	})

	graph := WorkflowGraph{
		ID: "wf-human-otel",
		Nodes: []NodeDef{
			{
				NodeID: "review",
				Human: &HumanStepSpec{
					Mode:      HumanStepModeApproval,
					Prompt:    "approve this run",
					OnTimeout: HumanTimeoutReject,
				},
			},
		},
	}

	if err := runner.RunDAG(ctx, "run-human-otel", graph, json.RawMessage(`{"review":true}`)); err != nil {
		t.Fatalf("RunDAG failed: %v", err)
	}

	tasks, err := runner.ListHumanTasks("run-human-otel", "", 10)
	if err != nil {
		t.Fatalf("ListHumanTasks failed: %v", err)
	}

	if len(tasks) != 1 {
		t.Fatalf("expected 1 human task, got %d", len(tasks))
	}

	if _, err := runner.ResolveHumanTask(ctx, tasks[0].ID, HumanDecisionApprove, nil, "approved", false); err != nil {
		t.Fatalf("ResolveHumanTask failed: %v", err)
	}

	replayWorkflow := WorkflowGraph{
		ID: "wf-replay-otel",
		Nodes: []NodeDef{
			{
				NodeID: "work",
				Topic:  "noop",
				Run: func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
					return json.RawMessage(`{"done":true}`), nil
				},
			},
		},
	}

	if err := runner.RunDAG(ctx, "run-replay-otel", replayWorkflow, json.RawMessage(`{"start":true}`)); err != nil {
		t.Fatalf("initial replay run failed: %v", err)
	}

	if err := runner.Replay(ctx, "run-replay-otel", ReplayLive); err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	spans := recorder.Ended()
	assertSpanNames(t, spans, "driftq.human.wait", "driftq.human.resolve", "driftq.replay.run")

	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &metrics); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	assertMetricNames(t, metrics, "driftq.engine.human.tasks", "driftq.engine.replays")
}

func assertSpanNames(t *testing.T, spans []sdktrace.ReadOnlySpan, want ...string) {
	t.Helper()
	got := map[string]bool{}
	for _, span := range spans {
		got[span.Name()] = true
	}

	for _, name := range want {
		if !got[name] {
			t.Fatalf("missing span %q; got spans=%v", name, spanNames(spans))
		}
	}
}

func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	out := make([]string, 0, len(spans))
	for _, span := range spans {
		out = append(out, span.Name())
	}

	return out
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
