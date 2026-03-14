package observability

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestMiddlewarePropagatesParentTrace(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	traceProvider := sdktrace.NewTracerProvider()
	traceProvider.RegisterSpanProcessor(recorder)

	prevProvider := otel.GetTracerProvider()
	prevPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(traceProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer func() {
		otel.SetTracerProvider(prevProvider)
		otel.SetTextMapPropagator(prevPropagator)
	}()

	parentTracer := traceProvider.Tracer("test-parent")
	parentCtx, parentSpan := parentTracer.Start(context.Background(), "parent")
	req := httptest.NewRequest(http.MethodGet, "/v1/healthz", nil)
	otel.GetTextMapPropagator().Inject(parentCtx, propagation.HeaderCarrier(req.Header))

	rec := httptest.NewRecorder()
	middleware := Middleware("driftqd", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	middleware.ServeHTTP(rec, req)
	parentSpan.End()

	if traceID := rec.Header().Get("X-Trace-Id"); traceID == "" {
		t.Fatal("expected X-Trace-Id header")
	}

	var childFound bool
	parentTraceID := parentSpan.SpanContext().TraceID()
	for _, span := range recorder.Ended() {
		if span.Name() != "GET /v1/healthz" {
			continue
		}
		childFound = true
		if got := span.SpanContext().TraceID(); got != parentTraceID {
			t.Fatalf("expected child trace id %s, got %s", parentTraceID, got)
		}
	}
	if !childFound {
		t.Fatal("expected middleware server span")
	}
}
