package engine

import (
	"sync"
	"time"
)

type MetricCounter struct {
	mu    sync.Mutex
	count int64
}

func (m *MetricCounter) Inc() {
	m.mu.Lock()
	m.count++
	m.mu.Unlock()
}

func (m *MetricCounter) Add(n int64) {
	m.mu.Lock()
	m.count += n
	m.mu.Unlock()
}

func (m *MetricCounter) Get() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.count
}

// MetricTimer tracks durations (for latency histograms).
type MetricTimer struct {
	mu        sync.Mutex
	durations []time.Duration
}

func (t *MetricTimer) Observe(d time.Duration) {
	t.mu.Lock()
	t.durations = append(t.durations, d)
	t.mu.Unlock()
}

func (t *MetricTimer) Summary() (count int, avg, p50, p95, p99 time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.durations) == 0 {
		return 0, 0, 0, 0, 0
	}

	count = len(t.durations)
	sum := time.Duration(0)
	for _, d := range t.durations {
		sum += d
	}
	avg = sum / time.Duration(len(t.durations))

	sorted := make([]time.Duration, len(t.durations))
	copy(sorted, t.durations)
	sortDurations(sorted)

	p50 = percentile(sorted, 0.50)
	p95 = percentile(sorted, 0.95)
	p99 = percentile(sorted, 0.99)
	return
}

func sortDurations(d []time.Duration) {
	for i := 1; i < len(d); i++ {
		j := i
		for j > 0 && d[j-1] > d[j] {
			d[j-1], d[j] = d[j], d[j-1]
			j--
		}
	}
}

func percentile(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	idx := int(float64(len(d)-1) * p)
	return d[idx]
}

// EngineMetrics holds counters and timers scoped per workflow/node.
type EngineMetrics struct {
	mu sync.Mutex

	RunStarted    MetricCounter
	RunSucceeded  MetricCounter
	RunFailed     MetricCounter
	RunCanceled   MetricCounter
	RunDurations  MetricTimer
	NodeDurations map[string]*MetricTimer // keyed by node_id
	NodeSucceeded map[string]*MetricCounter
	NodeFailed    map[string]*MetricCounter
}

func NewEngineMetrics() *EngineMetrics {
	return &EngineMetrics{
		NodeDurations: make(map[string]*MetricTimer),
		NodeSucceeded: make(map[string]*MetricCounter),
		NodeFailed:    make(map[string]*MetricCounter),
	}
}

func (m *EngineMetrics) ObserveRun(status RunStatus, dur time.Duration) {
	switch status {
	case RunStatusSucceeded:
		m.RunSucceeded.Inc()
	case RunStatusFailed:
		m.RunFailed.Inc()
	case RunStatusCanceled:
		m.RunCanceled.Inc()
	default:
		m.RunStarted.Inc()
	}
	if dur > 0 {
		m.RunDurations.Observe(dur)
	}
}

func (m *EngineMetrics) ObserveNode(nodeID string, succeeded bool, dur time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.NodeDurations[nodeID]; !ok {
		m.NodeDurations[nodeID] = &MetricTimer{}
		m.NodeSucceeded[nodeID] = &MetricCounter{}
		m.NodeFailed[nodeID] = &MetricCounter{}
	}

	m.NodeDurations[nodeID].Observe(dur)
	if succeeded {
		m.NodeSucceeded[nodeID].Inc()
	} else {
		m.NodeFailed[nodeID].Inc()
	}
}
