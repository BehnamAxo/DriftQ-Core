package observability

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/engine"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	otrace "go.opentelemetry.io/otel/trace"
)

type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusRecorder) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *statusRecorder) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusRecorder) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += n
	return n, err
}

func Middleware(serviceName string, next http.Handler) http.Handler {
	tracer := otel.GetTracerProvider().Tracer("github.com/driftq-org/DriftQ-Core/http")
	propagator := otel.GetTextMapPropagator()
	if propagator == nil {
		propagator = propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		spanName := r.Method + " " + r.URL.Path
		ctx, span := tracer.Start(ctx, spanName, otrace.WithSpanKind(otrace.SpanKindServer))
		defer span.End()

		rec := &statusRecorder{ResponseWriter: w}
		traceID := span.SpanContext().TraceID().String()
		if strings.TrimSpace(traceID) == "" || traceID == "00000000000000000000000000000000" {
			traceID = strings.TrimSpace(r.Header.Get("X-Trace-Id"))
		}
		if traceID == "" {
			traceID = engine.NewTraceID()
		}

		w.Header().Set("X-Trace-Id", traceID)
		ctx = engine.WithTraceID(ctx, traceID)
		ctx = contextWithServiceName(ctx, serviceName)

		start := time.Now()
		next.ServeHTTP(rec, r.WithContext(ctx))

		duration := time.Since(start)
		statusCode := rec.status
		if statusCode == 0 {
			statusCode = http.StatusOK
		}

		span.SetAttributes(
			attribute.String("http.method", r.Method),
			attribute.String("http.route", r.URL.Path),
			attribute.String("http.target", r.URL.RequestURI()),
			attribute.Int("http.status_code", statusCode),
			attribute.Int("http.response_size", rec.bytes),
			attribute.String("driftq.trace_id", traceID),
			attribute.Int64("driftq.http.duration_ms", duration.Milliseconds()),
		)
		if serviceName != "" {
			span.SetAttributes(attribute.String("service.name", serviceName))
		}
		if statusCode >= 500 {
			span.SetStatus(codes.Error, http.StatusText(statusCode))
		} else {
			span.SetStatus(codes.Ok, http.StatusText(statusCode))
		}

		w.Header().Set("X-Trace-Id", traceID)
		w.Header().Set("X-Trace-Flags", strconv.FormatUint(uint64(span.SpanContext().TraceFlags()), 10))
	})
}

type serviceNameKey struct{}

func contextWithServiceName(ctx context.Context, serviceName string) context.Context {
	if strings.TrimSpace(serviceName) == "" {
		return ctx
	}
	return context.WithValue(ctx, serviceNameKey{}, serviceName)
}
