package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/broker"
	"github.com/driftq-org/DriftQ-Core/internal/debugtypes"
	"github.com/driftq-org/DriftQ-Core/internal/engine"
	v1 "github.com/driftq-org/DriftQ-Core/internal/httpapi/v1"
	"github.com/driftq-org/DriftQ-Core/internal/multiagent"
	"github.com/driftq-org/DriftQ-Core/internal/observability"
	"github.com/driftq-org/DriftQ-Core/internal/storage"
	ui "github.com/driftq-org/DriftQ-Core/ui"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	buildVersion = "dev"
	buildCommit  = "unknown"
)

type server struct {
	broker broker.Broker
	config v1.ConfigResponse
}

type topicDebugAdapter struct {
	b broker.Broker
}

type TestRouter struct{}

type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

type promSink struct {
	produceRejected *prometheus.CounterVec
	dlqTotal        *prometheus.CounterVec
	topicsCreated   *prometheus.CounterVec
	acksTotal       *prometheus.CounterVec
	nacksTotal      *prometheus.CounterVec
	leaseTimeouts   *prometheus.CounterVec
	redeliveries    *prometheus.CounterVec
	walAppend       *prometheus.HistogramVec
	dispatch        prometheus.Histogram
	dispatchStaged  prometheus.Counter
}

type multiMetricsSink struct {
	sinks []broker.MetricsSink
}

func (m *multiMetricsSink) IncProduceRejected(reason string) {
	for _, sink := range m.sinks {
		sink.IncProduceRejected(reason)
	}
}

func (m *multiMetricsSink) IncDLQ(topic, reason string) {
	for _, sink := range m.sinks {
		sink.IncDLQ(topic, reason)
	}
}

func (m *multiMetricsSink) IncTopicCreated(topic string) {
	for _, sink := range m.sinks {
		sink.IncTopicCreated(topic)
	}
}

func (m *multiMetricsSink) IncAck(topic, group string) {
	for _, sink := range m.sinks {
		sink.IncAck(topic, group)
	}
}

func (m *multiMetricsSink) IncNack(topic, group, reason string) {
	for _, sink := range m.sinks {
		sink.IncNack(topic, group, reason)
	}
}

func (m *multiMetricsSink) IncLeaseTimeout(topic, group string) {
	for _, sink := range m.sinks {
		sink.IncLeaseTimeout(topic, group)
	}
}

func (m *multiMetricsSink) IncRedelivery(topic, group, cause string) {
	for _, sink := range m.sinks {
		sink.IncRedelivery(topic, group, cause)
	}
}

func (m *multiMetricsSink) ObserveWALAppend(kind string, d time.Duration) {
	for _, sink := range m.sinks {
		if timing, ok := sink.(broker.TimingMetricsSink); ok {
			timing.ObserveWALAppend(kind, d)
		}
	}
}

func (m *multiMetricsSink) ObserveDispatch(d time.Duration, staged int) {
	for _, sink := range m.sinks {
		if timing, ok := sink.(broker.TimingMetricsSink); ok {
			timing.ObserveDispatch(d, staged)
		}
	}
}

func (a topicDebugAdapter) ListTopics() ([]string, error) {
	return a.b.ListTopics(context.Background())
}

func (a topicDebugAdapter) ConsumerLag(ctx context.Context, group string, topic string) ([]debugtypes.ConsumerLagRow, error) {
	type consumerLagInspector interface {
		ConsumerLag(ctx context.Context, group string, topic string) ([]debugtypes.ConsumerLagRow, error)
	}

	li, ok := a.b.(consumerLagInspector)
	if !ok {
		return nil, errors.New("lag not supported by broker")
	}

	return li.ConsumerLag(ctx, group, topic)
}

func (a topicDebugAdapter) MessageStates(ctx context.Context, group, topic, status, owner string, limit int) ([]debugtypes.MessageStateRow, error) {
	type messageStateInspector interface {
		MessageStates(ctx context.Context, group, topic, status, owner string, limit int) ([]debugtypes.MessageStateRow, error)
	}

	mi, ok := a.b.(messageStateInspector)
	if !ok {
		return nil, errors.New("message state not supported by broker")
	}

	return mi.MessageStates(ctx, group, topic, status, owner, limit)
}

func (a topicDebugAdapter) TopicCount(ctx context.Context, topic string) (int64, error) {
	type topicCounter interface {
		TopicCount(ctx context.Context, topic string) (int64, error)
	}

	tc, ok := a.b.(topicCounter)
	if !ok {
		return 0, errors.New("topic count not supported by broker")
	}

	return tc.TopicCount(ctx, topic)
}

func (a topicDebugAdapter) Peek(topic string, limit int) ([]any, error) {
	type topicPeeker interface {
		Peek(topic string, limit int) ([]any, error)
	}

	pk, ok := a.b.(topicPeeker)
	if !ok {
		return nil, errors.New("peek not supported by broker")
	}

	return pk.Peek(topic, limit)
}

func (p *promSink) IncProduceRejected(reason string) {
	p.produceRejected.WithLabelValues(reason).Inc()
}

func (p *promSink) IncDLQ(topic, reason string) {
	p.dlqTotal.WithLabelValues(topic, reason).Inc()
}

func (p *promSink) IncTopicCreated(topic string) {
	if p.topicsCreated != nil {
		p.topicsCreated.WithLabelValues(topic).Inc()
	}
}

func (p *promSink) IncAck(topic, group string) {
	if p.acksTotal != nil {
		p.acksTotal.WithLabelValues(topic, group).Inc()
	}
}

func (p *promSink) IncNack(topic, group, reason string) {
	if p.nacksTotal != nil {
		p.nacksTotal.WithLabelValues(topic, group, reason).Inc()
	}
}

func (p *promSink) IncLeaseTimeout(topic, group string) {
	if p.leaseTimeouts != nil {
		p.leaseTimeouts.WithLabelValues(topic, group).Inc()
	}
}

func (p *promSink) IncRedelivery(topic, group, cause string) {
	if p.redeliveries != nil {
		p.redeliveries.WithLabelValues(topic, group, cause).Inc()
	}
}

func (p *promSink) ObserveWALAppend(kind string, d time.Duration) {
	if p.walAppend != nil {
		p.walAppend.WithLabelValues(kind).Observe(d.Seconds())
	}
}

func (p *promSink) ObserveDispatch(d time.Duration, staged int) {
	if p.dispatch != nil {
		p.dispatch.Observe(d.Seconds())
	}
	if p.dispatchStaged != nil && staged > 0 {
		p.dispatchStaged.Add(float64(staged))
	}
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
	// If WriteHeader wasn't called, net/http assumes 200.
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += n
	return n, err
}

func newRequestID() string {
	// 12 bytes => 24 hex chars (plenty for logs)
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		// extremely unlikely; fall back to timestamp-ish
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func remoteIP(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err == nil {
		return host
	}
	return addr
}

func withRequestLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// request id (existing)
		reqID := strings.TrimSpace(r.Header.Get("X-Request-Id"))
		if reqID == "" {
			reqID = newRequestID()
		}
		w.Header().Set("X-Request-Id", reqID)

		traceID := strings.TrimSpace(engine.TraceIDFrom(r.Context()))
		if traceID == "" {
			traceID = strings.TrimSpace(r.Header.Get("X-Trace-Id"))
		}
		if traceID == "" {
			traceID = engine.NewTraceID()
		}
		w.Header().Set("X-Trace-Id", traceID)

		rec := &statusRecorder{ResponseWriter: w}

		defer func() {
			dur := time.Since(start)

			logFn := slog.Info
			if rec.status >= 500 {
				logFn = slog.Error
			}

			logFn("http",
				"trace_id", traceID,
				"req_id", reqID,
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"bytes", rec.bytes,
				"duration_ms", dur.Milliseconds(),
				"remote_ip", remoteIP(r.RemoteAddr),
			)
		}()

		next.ServeHTTP(rec, r)
	})
}

func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	case "info", "":
		fallthrough
	default:
		return slog.LevelInfo
	}
}

func normalizeOr(s, def string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return def
	}
	return s
}

func configureLogger(levelStr, formatStr string) *slog.Logger {
	level := parseLogLevel(levelStr)
	format := strings.ToLower(strings.TrimSpace(formatStr))
	if format == "" {
		format = "text"
	}

	opts := &slog.HandlerOptions{Level: level}

	var h slog.Handler
	switch format {
	case "json":
		h = slog.NewJSONHandler(os.Stderr, opts)
	case "text":
		fallthrough
	default:
		h = slog.NewTextHandler(os.Stderr, opts)
	}

	version := normalizeOr(buildVersion, "dev")
	commit := normalizeOr(buildCommit, "unknown")

	l := slog.New(h).With(
		"service", "driftqd",
		"version", version,
		"commit", commit,
	)

	slog.SetDefault(l)
	return l
}

func fatal(msg string, err error) {
	if err != nil {
		slog.Error(msg, "err", err)
	} else {
		slog.Error(msg)
	}
	os.Exit(1)
}

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	walPath := flag.String("wal", "driftq.wal", "path to WAL file")
	resetWAL := flag.Bool("reset-wal", false, "reset WAL by moving existing file aside (creates a .bak.<ts> file)")
	walSyncInterval := flag.Duration("wal-sync-interval", 0, "broker WAL fsync interval (0 = fsync every append, higher = lower latency with larger crash window)")
	walBufferBytes := flag.Int("wal-buffer-bytes", 256*1024, "broker WAL write buffer size in bytes")
	accessLog := flag.Bool("access-log", true, "enable per-request access logging")

	engineStore := flag.String("engine-store", "memory", "engine store: memory|file")
	engineWAL := flag.String("engine-wal", "driftq.engine.wal", "path to engine run/event WAL file (engine-store=file)")
	artifactsDir := flag.String("artifacts-dir", "driftq.artifacts", "artifact store dir (empty = in-memory)")

	logLevel := flag.String("log-level", "info", "log level: debug|info|warn|error")
	logFormat := flag.String("log-format", "text", "log format: text|json")
	otelEnabled := flag.Bool("otel-enabled", false, "enable OTLP trace and metric export")
	otelServiceName := flag.String("otel-service-name", "driftqd", "OpenTelemetry service name")
	otelEndpoint := flag.String("otel-endpoint", "localhost:4318", "OTLP/HTTP collector endpoint host:port")
	otelInsecure := flag.Bool("otel-insecure", true, "use insecure OTLP/HTTP transport")
	otelMetricsInterval := flag.Duration("otel-metrics-interval", 10*time.Second, "OpenTelemetry metrics export interval")

	maxPartitionBytes := flag.Int("max-partition-bytes", 0, "Max bytes buffered per partition (0 = broker default)")
	maxPartitionMsgs := flag.Int("max-partition-msgs", 0, "Max messages buffered per partition (0 = broker default)")
	maxInFlight := flag.Int("max-inflight", 0, "Max in-flight per (topic,group,partition) (0 = broker default)")

	multiagentConfigPath := flag.String("multiagent-config", "", "path to v3.1 multi-agent config JSON (optional)")
	bootstrapMultiagentTopics := flag.Bool("bootstrap-multiagent-topics", false, "create configured v3.1 agent/team topics on startup (safe to rerun)")

	flag.Parse()

	logger := configureLogger(*logLevel, *logFormat)

	otelShutdown, err := observability.Setup(context.Background(), observability.Config{
		Enabled:         *otelEnabled,
		ServiceName:     *otelServiceName,
		ServiceVersion:  normalizeOr(buildVersion, "dev"),
		Endpoint:        strings.TrimSpace(*otelEndpoint),
		Insecure:        *otelInsecure,
		MetricsInterval: *otelMetricsInterval,
	})
	if err != nil {
		fatal("failed to initialize observability", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := otelShutdown(ctx); err != nil {
			logger.Error("observability shutdown failed", "err", err)
		}
	}()

	// Optional safe reset: move existing WAL aside so we start fresh
	if *resetWAL {
		if _, err := os.Stat(*walPath); err == nil {
			bak := fmt.Sprintf("%s.bak.%d", *walPath, time.Now().Unix())
			if err := os.Rename(*walPath, bak); err != nil {
				fatal("failed to reset WAL (rename)", err)
			}
			slog.Info("WAL reset", "from", *walPath, "to", bak)
		} else if !errors.Is(err, os.ErrNotExist) {
			fatal("failed to stat WAL", err)
		}
	}

	wal, err := storage.OpenFileWALWithOptions(*walPath, storage.FileWALOptions{
		SyncInterval: *walSyncInterval,
		BufferBytes:  *walBufferBytes,
	})
	if err != nil {
		fatal("failed to open WAL", err)
	}
	defer wal.Close()

	var brokerOpts []broker.BrokerOption
	if *maxPartitionBytes > 0 {
		brokerOpts = append(brokerOpts, broker.WithMaxPartitionBytes(*maxPartitionBytes))
	}

	if *maxPartitionMsgs > 0 {
		brokerOpts = append(brokerOpts, broker.WithMaxPartitionMsgs(*maxPartitionMsgs))
	}

	if *maxInFlight > 0 {
		brokerOpts = append(brokerOpts, broker.WithMaxInFlight(*maxInFlight))
	}

	b, err := broker.NewInMemoryBrokerFromWAL(wal, brokerOpts...)
	if err != nil {
		fatal("failed to replay WAL", err)
	}

	slog.Info("broker config",
		"max_partition_bytes", b.MaxPartitionBytes(),
		"max_partition_msgs", b.MaxPartitionMsgs(),
		"max_inflight", b.MaxInFlight(),
		"wal_sync_interval", walSyncInterval.String(),
		"wal_buffer_bytes", *walBufferBytes,
		"access_log", *accessLog,
		"otel_enabled", *otelEnabled,
		"otel_endpoint", strings.TrimSpace(*otelEndpoint),
	)

	produceRejected := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "produce_rejected_total",
			Help: "Total number of produce calls rejected (backpressure/overload).",
		},
		[]string{"reason"},
	)

	dlqTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dlq_messages_total",
			Help: "Total number of messages routed to DLQ.",
		},
		[]string{"topic", "reason"},
	)

	topicsCreated := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "topic_created_total",
			Help: "Total number of topics created.",
		},
		[]string{"topic"},
	)

	acksTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "message_acks_total",
			Help: "Total number of successful message acknowledgements.",
		},
		[]string{"topic", "group"},
	)

	nacksTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "message_nacks_total",
			Help: "Total number of explicit message nacks.",
		},
		[]string{"topic", "group", "reason"},
	)

	leaseTimeouts := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "message_lease_timeouts_total",
			Help: "Total number of message lease timeouts detected by redelivery.",
		},
		[]string{"topic", "group"},
	)

	redeliveries := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "message_redeliveries_total",
			Help: "Total number of broker redeliveries.",
		},
		[]string{"topic", "group", "cause"},
	)

	walAppend := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "broker_wal_append_duration_seconds",
			Help:    "Latency of broker WAL appends by record type.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"kind"},
	)

	dispatch := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "broker_dispatch_duration_seconds",
			Help:    "Latency of broker dispatch passes.",
			Buckets: prometheus.DefBuckets,
		},
	)

	dispatchStaged := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "broker_dispatch_staged_messages_total",
			Help: "Total number of messages staged onto consumer queues by dispatch.",
		},
	)

	prometheus.MustRegister(
		produceRejected,
		dlqTotal,
		topicsCreated,
		acksTotal,
		nacksTotal,
		leaseTimeouts,
		redeliveries,
		walAppend,
		dispatch,
		dispatchStaged,
		NewBrokerCollector(b),
	)

	promMetricsSink := &promSink{
		produceRejected: produceRejected,
		dlqTotal:        dlqTotal,
		topicsCreated:   topicsCreated,
		acksTotal:       acksTotal,
		nacksTotal:      nacksTotal,
		leaseTimeouts:   leaseTimeouts,
		redeliveries:    redeliveries,
		walAppend:       walAppend,
		dispatch:        dispatch,
		dispatchStaged:  dispatchStaged,
	}
	b.SetMetricsSink(&multiMetricsSink{
		sinks: []broker.MetricsSink{
			promMetricsSink,
			observability.NewBrokerMetricsSink(nil),
		},
	})

	appCtx, appCancel := context.WithCancel(context.Background())
	defer appCancel()

	b.StartRedeliveryLoop(appCtx)

	if cfgPath := strings.TrimSpace(*multiagentConfigPath); cfgPath != "" {
		maCfg, err := multiagent.LoadStartupConfig(cfgPath)
		if err != nil {
			fatal("failed to load multiagent config", err)
		}

		if *bootstrapMultiagentTopics {
			summary, err := multiagent.BootstrapTopics(appCtx, b, maCfg)
			if err != nil {
				fatal("failed to bootstrap multiagent topics", err)
			}

			slog.Info("multiagent topics bootstrapped",
				"created", len(summary.Created),
				"skipped", len(summary.Skipped),
				"partitions", maCfg.TopicPartitions,
			)
		}

		reg, err := maCfg.BuildRegistry()
		if err != nil {
			fatal("failed to build multiagent capability registry", err)
		}

		mrouter := multiagent.NewRouter(maCfg.RouterConfig(reg))
		b.SetRouter(observability.WrapRouter(mrouter, nil))

		slog.Info("multiagent router enabled",
			"config", cfgPath,
			"agents", len(maCfg.AllAgentIDs()),
			"teams", len(maCfg.Teams),
			"capabilities", len(maCfg.Capabilities),
			"strict", maCfg.RouterStrict,
			"source_topics", len(maCfg.SourceTopics),
		)
	}

	wrappedBroker := observability.WrapBroker(b, nil, nil)

	s := &server{
		broker: wrappedBroker,
		config: v1.ConfigResponse{
			Addr:              *addr,
			WalPath:           *walPath,
			AccessLog:         *accessLog,
			EngineStore:       strings.ToLower(strings.TrimSpace(*engineStore)),
			EngineWAL:         *engineWAL,
			ArtifactsDir:      *artifactsDir,
			LogLevel:          strings.ToLower(strings.TrimSpace(*logLevel)),
			LogFormat:         strings.ToLower(strings.TrimSpace(*logFormat)),
			MaxPartitionBytes: b.MaxPartitionBytes(),
			MaxPartitionMsgs:  b.MaxPartitionMsgs(),
			MaxInFlight:       b.MaxInFlight(),
			WALSyncInterval:   walSyncInterval.String(),
			WALBufferBytes:    *walBufferBytes,
		},
	}

	// v2 runner store (memory or durable file WAL)
	var runStore engine.Store
	var closeRunStore func() error
	switch strings.ToLower(strings.TrimSpace(*engineStore)) {
	case "file":
		fs, err := engine.OpenFileStore(*engineWAL)
		if err != nil {
			fatal("failed to open engine store", err)
		}

		runStore = fs
		closeRunStore = fs.Close
	default:
		runStore = engine.NewMemoryStore()
	}

	if closeRunStore != nil {
		defer func() { _ = closeRunStore() }()
	}

	runner := engine.NewRunner(runStore)

	// Artifact store: filesystem by default so demo outputs survive restarts.
	if strings.TrimSpace(*artifactsDir) != "" {
		as, err := engine.NewLocalArtifactStore(*artifactsDir)
		if err != nil {
			fatal("failed to init artifact store", err)
		}
		runner.SetArtifactStore(as)
	} else {
		runner.SetArtifactStore(engine.NewMemoryArtifactStore())
	}
	runner.SetLogger(logger)

	// fire due timers in the background (durable delay primitive)
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-appCtx.Done():
				return
			case t := <-ticker.C:
				fired, resumed, err := runner.FireDueTimersAndResume(appCtx, t.UTC())
				if err != nil {
					logger.Error("timers: fire due timers failed", "err", err)
					continue
				}

				if fired > 0 || resumed > 0 {
					logger.Info("timers: fired/resumed", "fired", fired, "resumed", resumed)
				}
			}
		}
	}()

	// global registry used by /debug/run-spec (and replay) :)
	reg := engine.NewHandlerRegistry()

	reg.Register("noop", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return input, nil
	})

	reg.Register("sleep_500ms", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		time.Sleep(500 * time.Millisecond)
		return input, nil
	})

	// IMPORTANT: stateless fail-on-attempt-1, succeed on attempt>=2 (works across restarts)
	reg.Register("fail_once", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		if engine.AttemptFrom(ctx) <= 1 {
			return nil, errors.New("boom")
		}
		return json.RawMessage(`{"ok":true}`), nil
	})

	reg.Register("delay_once_2s", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		if engine.AttemptFrom(ctx) <= 1 {
			return nil, engine.Delay(2*time.Second, "demo delay once")
		}
		return json.RawMessage(`{"ok":true,"after":"delay"}`), nil
	})

	runner.SetHandlerRegistry(reg)

	rootMux := http.NewServeMux()
	v1Mux := http.NewServeMux()
	engine.AttachDebugRoutes(rootMux, runner)
	engine.AttachTopicDebugRoutes(rootMux, topicDebugAdapter{b: wrappedBroker})

	// v1 routes
	v1Mux.HandleFunc("/healthz", s.requireMethod(http.MethodGet)(s.handleHealthz))
	v1Mux.HandleFunc("/produce", s.requireMethod(http.MethodPost)(s.handleProduce))
	v1Mux.HandleFunc("/consume", s.requireMethod(http.MethodGet)(s.handleConsume))
	v1Mux.HandleFunc("/ack", s.requireMethod(http.MethodPost)(s.handleAck))
	v1Mux.HandleFunc("/ack-cumulative", s.requireMethod(http.MethodPost)(s.handleAckCumulative))
	v1Mux.HandleFunc("/nack", s.requireMethod(http.MethodPost)(s.handleNack))
	v1Mux.HandleFunc("/topics", s.method(s.handleTopicsList, s.handleTopicsCreate))
	v1Mux.HandleFunc("/version", s.requireMethod(http.MethodGet)(s.handleVersion))
	v1Mux.HandleFunc("/config", s.requireMethod(http.MethodGet)(s.handleConfig))

	// mount v1 under /v1/*
	rootMux.Handle("/v1/", http.StripPrefix("/v1", v1Mux))
	// Optional dashboard UI
	rootMux.Handle("/ui", http.RedirectHandler("/ui/", http.StatusPermanentRedirect))
	rootMux.Handle("/ui/", ui.Handler())
	// Prometheus scrape endpoint (not versioned yet)
	rootMux.Handle("/metrics", promhttp.Handler())

	// block unversioned routes
	rootMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		v1.WriteError(w, http.StatusNotFound, "NOT_FOUND", "use /v1/* endpoints")
	})

	handler := http.Handler(rootMux)
	if *accessLog {
		handler = withRequestLogging(handler)
	}
	handler = observability.Middleware(*otelServiceName, handler)

	srv := &http.Server{
		Addr:         *addr,
		Handler:      handler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 0,
		ErrorLog:     slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	slog.Info("broker starting", "addr", *addr)

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fatal("http server error", err)
		}
	}()

	// Shutdown stuff
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	slog.Info("shutting down")
	appCancel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("http shutdown error", "err", err)
	}
	if err := b.Close(); err != nil {
		slog.Error("broker close error", "err", err)
	}

	slog.Info("broker stopped")
}

// requireMethod wraps a handler and rejects non-allowed methods with JSON 405 + Allow
func (s *server) requireMethod(allowed ...string) func(http.HandlerFunc) http.HandlerFunc {
	allowSet := map[string]struct{}{}
	for _, m := range allowed {
		allowSet[m] = struct{}{}
	}
	allowHeader := strings.Join(allowed, ", ")

	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if _, ok := allowSet[r.Method]; !ok {
				if allowHeader != "" {
					w.Header().Set("Allow", allowHeader)
				}

				v1.WriteError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
				return
			}
			next(w, r)
		}
	}
}

// method dispatches GET/POST with proper Allow header and JSON 405
func (s *server) method(get, post http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if get == nil {
				w.Header().Set("Allow", http.MethodPost)
				v1.WriteError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
				return
			}
			get(w, r)
		case http.MethodPost:
			if post == nil {
				w.Header().Set("Allow", http.MethodGet)
				v1.WriteError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
				return
			}
			post(w, r)

		default:
			allow := make([]string, 0, 2)
			if get != nil {
				allow = append(allow, http.MethodGet)
			}

			if post != nil {
				allow = append(allow, http.MethodPost)
			}

			if len(allow) > 0 {
				w.Header().Set("Allow", strings.Join(allow, ", "))
			}
			v1.WriteError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		}
	}
}

func (s *server) handleVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		v1.MethodNotAllowed(w, http.MethodGet)
		return
	}

	version := strings.TrimSpace(buildVersion)
	if version == "" {
		version = "dev"
	}

	commit := strings.TrimSpace(buildCommit)
	if commit == "" {
		commit = "unknown"
	}

	type walEnabled interface{ WALEnabled() bool }
	walOn := false
	if b, ok := any(s.broker).(walEnabled); ok {
		walOn = b.WALEnabled()
	}

	v1.WriteJSON(w, http.StatusOK, v1.VersionResponse{
		Version:    version,
		Commit:     commit,
		WalEnabled: walOn,
	})
}

func (s *server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		v1.MethodNotAllowed(w, http.MethodGet)
		return
	}

	v1.WriteJSON(w, http.StatusOK, s.config)
}

func (s *server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		v1.MethodNotAllowed(w, http.MethodGet)
		return
	}

	v1.WriteJSON(w, http.StatusOK, v1.HealthzResponse{Status: "ok"})
}

func (s *server) handleTopicsList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	topics, err := s.broker.ListTopics(ctx)
	if err != nil {
		v1.WriteError(w, http.StatusInternalServerError, "INTERNAL", err.Error())
		return
	}

	v1.WriteJSON(w, http.StatusOK, v1.TopicsListResponse{Topics: topics})
}

func (s *server) handleTopicsCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req v1.TopicsCreateRequest

	if r.Body != nil && r.ContentLength != 0 {
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json body: "+err.Error())
			return
		}
	} else {
		q := r.URL.Query()
		req.Name = strings.TrimSpace(q.Get("name"))

		if p := strings.TrimSpace(q.Get("partitions")); p != "" {
			n, err := strconv.Atoi(p)
			if err != nil {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid partitions")
				return
			}
			req.Partitions = n
		}
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "name is required")
		return
	}

	partitions := req.Partitions
	if partitions == 0 {
		partitions = 1
	}

	if partitions <= 0 {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid partitions")
		return
	}

	if err := s.broker.CreateTopic(ctx, name, partitions); err != nil {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	v1.WriteJSON(w, http.StatusCreated, v1.TopicsCreateResponse{
		Status:     "created",
		Name:       name,
		Partitions: partitions,
	})
}

func toBrokerEnvelope(env *v1.Envelope) *broker.Envelope {
	if env == nil {
		return nil
	}

	out := &broker.Envelope{
		RunID:             env.RunID,
		StepID:            env.StepID,
		ParentStepID:      env.ParentStepID,
		TenantID:          env.TenantID,
		IdempotencyKey:    env.IdempotencyKey,
		TargetTopic:       env.TargetTopic,
		Deadline:          env.Deadline,
		PartitionOverride: env.PartitionOverride,
	}

	if env.RetryPolicy != nil {
		out.RetryPolicy = &broker.RetryPolicy{
			MaxAttempts:  env.RetryPolicy.MaxAttempts,
			BackoffMs:    env.RetryPolicy.BackoffMs,
			MaxBackoffMs: env.RetryPolicy.MaxBackoffMs,
		}
	}

	return out
}

func parseDeadlineQuery(q url.Values) (*time.Time, error) {
	if v := strings.TrimSpace(q.Get("deadline_rfc3339")); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return nil, fmt.Errorf("invalid deadline_rfc3339: %w", err)
		}
		return &t, nil
	}

	if v := strings.TrimSpace(q.Get("deadline_ms")); v != "" {
		ms, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid deadline_ms: %w", err)
		}
		t := time.Unix(0, ms*int64(time.Millisecond))
		return &t, nil
	}

	return nil, nil
}

func parseOptionalIntQuery(q url.Values, key string) (*int, error) {
	v := strings.TrimSpace(q.Get(key))
	if v == "" {
		return nil, nil
	}

	n, err := strconv.Atoi(v)
	if err != nil {
		return nil, fmt.Errorf("invalid %s", key)
	}
	return &n, nil
}

func parseOptionalInt64Query(q url.Values, key string) (*int64, error) {
	v := strings.TrimSpace(q.Get(key))
	if v == "" {
		return nil, nil
	}

	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid %s", key)
	}
	return &n, nil
}

func hasProduceEnvelopeQueryParams(q url.Values) bool {
	for key := range q {
		switch key {
		case "run_id", "step_id", "parent_step_id",
			"tenant_id", "tenant",
			"idempotency_key", "idem_key",
			"target_topic", "partition_override",
			"deadline_rfc3339", "deadline_ms",
			"retry_max_attempts", "retry_backoff_ms", "retry_max_backoff_ms":
			return true
		}
	}
	return false
}

func (s *server) handleProduce(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req v1.ProduceRequest

	// Prefer JSON body; fall back to query params
	if r.Body != nil && r.ContentLength != 0 {
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json body: "+err.Error())
			return
		}
	} else {
		q := r.URL.Query()

		req.Topic = q.Get("topic")
		req.Key = q.Get("key")
		req.Value = q.Get("value")

		// Hot-path: plain query produce (topic/key/value only).
		if hasProduceEnvelopeQueryParams(q) {
			env := &v1.Envelope{}
			anySet := false

			if v := strings.TrimSpace(q.Get("run_id")); v != "" {
				env.RunID = v
				anySet = true
			}

			if v := strings.TrimSpace(q.Get("step_id")); v != "" {
				env.StepID = v
				anySet = true
			}

			if v := strings.TrimSpace(q.Get("parent_step_id")); v != "" {
				env.ParentStepID = v
				anySet = true
			}

			// tenant_id (primary) + tenant (alias)
			if v := strings.TrimSpace(q.Get("tenant_id")); v != "" {
				env.TenantID = v
				anySet = true
			} else if v := strings.TrimSpace(q.Get("tenant")); v != "" {
				env.TenantID = v
				anySet = true
			}

			// idempotency_key (primary) + idem_key (alias)
			idem := strings.TrimSpace(q.Get("idempotency_key"))
			if idem == "" {
				idem = strings.TrimSpace(q.Get("idem_key"))
			}

			if idem != "" {
				env.IdempotencyKey = idem
				anySet = true
			}

			if v := strings.TrimSpace(q.Get("target_topic")); v != "" {
				env.TargetTopic = v
				anySet = true
			}

			if v := strings.TrimSpace(q.Get("partition_override")); v != "" {
				pi, err := strconv.Atoi(v)
				if err != nil || pi < 0 {
					v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid partition_override")
					return
				}
				env.PartitionOverride = &pi
				anySet = true
			}

			if dl, err := parseDeadlineQuery(q); err != nil {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
				return
			} else if dl != nil {
				env.Deadline = dl
				anySet = true
			}

			// Retry policy (query params)
			maxAttemptsPtr, err := parseOptionalIntQuery(q, "retry_max_attempts")
			if err != nil {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
				return
			}

			backoffPtr, err := parseOptionalInt64Query(q, "retry_backoff_ms")
			if err != nil {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
				return
			}

			maxBackoffPtr, err := parseOptionalInt64Query(q, "retry_max_backoff_ms")
			if err != nil {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
				return
			}

			if maxAttemptsPtr != nil || backoffPtr != nil || maxBackoffPtr != nil {
				rp := &v1.RetryPolicy{}

				if maxAttemptsPtr != nil {
					if *maxAttemptsPtr < 0 {
						v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid retry_max_attempts")
						return
					}
					rp.MaxAttempts = *maxAttemptsPtr
				}

				if backoffPtr != nil {
					if *backoffPtr < 0 {
						v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid retry_backoff_ms")
						return
					}
					rp.BackoffMs = *backoffPtr
				}

				if maxBackoffPtr != nil {
					if *maxBackoffPtr < 0 {
						v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid retry_max_backoff_ms")
						return
					}
					rp.MaxBackoffMs = *maxBackoffPtr
				}

				if (backoffPtr != nil || maxBackoffPtr != nil) && rp.MaxAttempts <= 0 {
					v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "retry_max_attempts must be > 0 when using retry backoff params")
					return
				}

				if rp.MaxAttempts != 0 || rp.BackoffMs != 0 || rp.MaxBackoffMs != 0 {
					env.RetryPolicy = rp
					anySet = true
				}
			}

			if anySet {
				req.Envelope = env
			}
		}
	}

	if strings.TrimSpace(req.Topic) == "" || req.Value == "" {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "topic and value are required")
		return
	}

	msg := broker.Message{
		Key:      []byte(req.Key),
		Value:    []byte(req.Value),
		Envelope: toBrokerEnvelope(req.Envelope),
	}

	if err := s.broker.Produce(ctx, req.Topic, msg); err != nil {
		if errors.Is(err, broker.ErrProducerOverloaded) {
			retryAfter := 1 * time.Second
			reason := "overloaded"

			var oe *broker.ProducerOverloadError
			if errors.As(err, &oe) && oe != nil {
				if oe.RetryAfter > 0 {
					retryAfter = oe.RetryAfter
				}
				if oe.Reason != "" {
					reason = oe.Reason
				}
			}

			secs := int((retryAfter + time.Second - 1) / time.Second)
			w.Header().Set("Retry-After", strconv.Itoa(secs))

			v1.WriteJSON(w, http.StatusTooManyRequests, v1.ResourceExhaustedResponse{
				Error:        "RESOURCE_EXHAUSTED",
				Message:      err.Error(),
				Reason:       reason,
				RetryAfterMs: int(retryAfter / time.Millisecond),
			})
			return
		}

		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	v1.WriteJSON(w, http.StatusOK, v1.ProduceResponse{
		Status: "produced",
		Topic:  req.Topic,
	})
}

func (s *server) handleConsume(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	lease := 2 * time.Second
	var req v1.ConsumeRequest

	if r.Body != nil && r.ContentLength != 0 {
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json body: "+err.Error())
			return
		}
	} else {
		q := r.URL.Query()
		req.Topic = q.Get("topic")
		req.Group = q.Get("group")
		req.Owner = q.Get("owner")

		if v := strings.TrimSpace(q.Get("lease_ms")); v != "" {
			ms, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid lease_ms")
				return
			}
			req.LeaseMs = ms
		}
	}

	topic := strings.TrimSpace(req.Topic)
	group := strings.TrimSpace(req.Group)
	owner := strings.TrimSpace(req.Owner)

	if topic == "" || group == "" || owner == "" {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "topic, group, and owner are required")
		return
	}

	if req.LeaseMs < 0 {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid lease_ms")
		return
	}

	if req.LeaseMs > 0 {
		lease = time.Duration(req.LeaseMs) * time.Millisecond
	}

	// Use the new broker method (no type assertions now)
	ch, err := s.broker.ConsumeWithLease(ctx, topic, group, owner, lease)
	if err != nil {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	// stream NDJSON
	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")

	flusher, ok := w.(http.Flusher)
	if !ok {
		v1.WriteError(w, http.StatusInternalServerError, "INTERNAL", "streaming not supported")
		return
	}

	enc := json.NewEncoder(w)

	for {
		select {
		case <-ctx.Done():
			return

		case m, ok := <-ch:
			if !ok {
				return
			}

			item := v1.ConsumeItem{
				Partition: m.Partition,
				Offset:    m.Offset,
				Attempts:  m.Attempts,
				Key:       string(m.Key),
				Value:     string(m.Value),
				LastError: m.LastError,
			}

			if m.Routing != nil {
				item.Routing = &v1.Routing{
					Label: m.Routing.Label,
					Meta:  m.Routing.Meta,
				}
			}

			if m.Envelope != nil {
				item.Envelope = &v1.Envelope{
					RunID:             m.Envelope.RunID,
					StepID:            m.Envelope.StepID,
					ParentStepID:      m.Envelope.ParentStepID,
					TenantID:          m.Envelope.TenantID,
					IdempotencyKey:    m.Envelope.IdempotencyKey,
					TargetTopic:       m.Envelope.TargetTopic,
					Deadline:          m.Envelope.Deadline,
					PartitionOverride: m.Envelope.PartitionOverride,
					RetryPolicy:       nil,
				}

				if m.Envelope.RetryPolicy != nil {
					item.Envelope.RetryPolicy = &v1.RetryPolicy{
						MaxAttempts:  m.Envelope.RetryPolicy.MaxAttempts,
						BackoffMs:    m.Envelope.RetryPolicy.BackoffMs,
						MaxBackoffMs: m.Envelope.RetryPolicy.MaxBackoffMs,
					}
				}
			}

			if err := enc.Encode(item); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func parseAckRequest(w http.ResponseWriter, r *http.Request) (topic, group, owner string, part int, off int64, ok bool) {
	var (
		parseOK bool
	)

	if r.Body != nil && r.ContentLength != 0 {
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "failed to read body")
			return "", "", "", 0, 0, false
		}

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(bodyBytes, &raw); err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json body: "+err.Error())
			return "", "", "", 0, 0, false
		}

		allowed := map[string]struct{}{
			"topic":     {},
			"group":     {},
			"owner":     {},
			"partition": {},
			"offset":    {},
		}

		for k := range raw {
			if _, ok := allowed[k]; !ok {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "unknown field: "+k)
				return "", "", "", 0, 0, false
			}
		}

		if v, ok := raw["topic"]; ok {
			_ = json.Unmarshal(v, &topic)
		}

		if v, ok := raw["group"]; ok {
			_ = json.Unmarshal(v, &group)
		}

		if v, ok := raw["owner"]; ok {
			_ = json.Unmarshal(v, &owner)
		}

		if v, ok := raw["partition"]; ok {
			if err := json.Unmarshal(v, &part); err != nil {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid partition")
				return "", "", "", 0, 0, false
			}
		}
		if v, ok := raw["offset"]; ok {
			if err := json.Unmarshal(v, &off); err != nil {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid offset")
				return "", "", "", 0, 0, false
			}
		}
		parseOK = true
	} else {
		q := r.URL.Query()
		topic = q.Get("topic")
		group = q.Get("group")
		owner = q.Get("owner")

		pStr := strings.TrimSpace(q.Get("partition"))
		oStr := strings.TrimSpace(q.Get("offset"))
		if pStr == "" || oStr == "" {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "partition and offset are required")
			return "", "", "", 0, 0, false
		}

		p64, err := strconv.ParseInt(pStr, 10, 32)
		if err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid partition")
			return "", "", "", 0, 0, false
		}
		part = int(p64)

		off, err = strconv.ParseInt(oStr, 10, 64)
		if err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid offset")
			return "", "", "", 0, 0, false
		}
		parseOK = true
	}

	topic = strings.TrimSpace(topic)
	group = strings.TrimSpace(group)
	owner = strings.TrimSpace(owner)

	if topic == "" || group == "" || owner == "" {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "topic, group, and owner are required")
		return "", "", "", 0, 0, false
	}

	return topic, group, owner, part, off, parseOK
}

func (s *server) handleAck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	topic, group, owner, part, off, ok := parseAckRequest(w, r)
	if !ok {
		return
	}

	if err := s.broker.AckIfOwner(ctx, topic, group, part, off, owner); err != nil {
		if errors.Is(err, broker.ErrNotOwner) {
			v1.WriteError(w, http.StatusConflict, "FAILED_PRECONDITION", "not owner")
			return
		}
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleAckCumulative(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	topic, group, owner, part, off, ok := parseAckRequest(w, r)
	if !ok {
		return
	}

	if err := s.broker.AckCumulativeIfOwner(ctx, topic, group, part, off, owner); err != nil {
		if errors.Is(err, broker.ErrNotOwner) {
			v1.WriteError(w, http.StatusConflict, "FAILED_PRECONDITION", "not owner")
			return
		}
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleNack(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var (
		topic  string
		group  string
		owner  string
		reason string
		part   int
		off    int64
	)

	if r.Body != nil && r.ContentLength != 0 {
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "failed to read body")
			return
		}

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(bodyBytes, &raw); err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid json body: "+err.Error())
			return
		}

		allowed := map[string]struct{}{
			"topic":     {},
			"group":     {},
			"owner":     {},
			"partition": {},
			"offset":    {},
			"reason":    {},
		}

		for k := range raw {
			if _, ok := allowed[k]; !ok {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "unknown field: "+k)
				return
			}
		}

		if v, ok := raw["topic"]; ok {
			_ = json.Unmarshal(v, &topic)
		}

		if v, ok := raw["group"]; ok {
			_ = json.Unmarshal(v, &group)
		}

		if v, ok := raw["owner"]; ok {
			_ = json.Unmarshal(v, &owner)
		}

		if v, ok := raw["reason"]; ok {
			_ = json.Unmarshal(v, &reason)
		}

		if v, ok := raw["partition"]; ok {
			if err := json.Unmarshal(v, &part); err != nil {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid partition")
				return
			}
		}
		if v, ok := raw["offset"]; ok {
			if err := json.Unmarshal(v, &off); err != nil {
				v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid offset")
				return
			}
		}
	} else {
		q := r.URL.Query()
		topic = q.Get("topic")
		group = q.Get("group")
		owner = q.Get("owner")
		reason = q.Get("reason")

		pStr := strings.TrimSpace(q.Get("partition"))
		oStr := strings.TrimSpace(q.Get("offset"))

		if pStr == "" || oStr == "" {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "partition and offset are required")
			return
		}

		p64, err := strconv.ParseInt(pStr, 10, 32)
		if err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid partition")
			return
		}
		part = int(p64)

		off, err = strconv.ParseInt(oStr, 10, 64)
		if err != nil {
			v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid offset")
			return
		}
	}

	topic = strings.TrimSpace(topic)
	group = strings.TrimSpace(group)
	owner = strings.TrimSpace(owner)
	reason = strings.TrimSpace(reason)

	if topic == "" || group == "" || owner == "" {
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "topic, group, and owner are required")
		return
	}

	if err := s.broker.Nack(ctx, topic, group, part, off, owner, reason); err != nil {
		if errors.Is(err, broker.ErrNotOwner) {
			v1.WriteError(w, http.StatusConflict, "FAILED_PRECONDITION", "not owner")
			return
		}
		v1.WriteError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (TestRouter) Route(_ context.Context, topic string, msg broker.Message) (broker.RoutingDecision, error) {
	return broker.RoutingDecision{
		Label: "test-label",
		Meta:  map[string]string{"source": "router"},
	}, nil
}
