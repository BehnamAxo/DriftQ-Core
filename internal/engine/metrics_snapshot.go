package engine

import "time"

type NodeMetricSnapshot struct {
	Count int           `json:"count"`
	Avg   time.Duration `json:"avg"`
	P50   time.Duration `json:"p50"`
	P95   time.Duration `json:"p95"`
	P99   time.Duration `json:"p99"`

	Succeeded int64 `json:"succeeded"`
	Failed    int64 `json:"failed"`
}

type MetricsSnapshot struct {
	Runs struct {
		Started   int64 `json:"started"`
		Succeeded int64 `json:"succeeded"`
		Failed    int64 `json:"failed"`
		Canceled  int64 `json:"canceled"`

		Latency struct {
			Count int           `json:"count"`
			Avg   time.Duration `json:"avg"`
			P50   time.Duration `json:"p50"`
			P95   time.Duration `json:"p95"`
			P99   time.Duration `json:"p99"`
		} `json:"latency"`
	} `json:"runs"`

	Nodes map[string]NodeMetricSnapshot `json:"nodes"`
}

func (m *EngineMetrics) Snapshot() MetricsSnapshot {
	var snap MetricsSnapshot

	snap.Runs.Started = m.RunStarted.Get()
	snap.Runs.Succeeded = m.RunSucceeded.Get()
	snap.Runs.Failed = m.RunFailed.Get()
	snap.Runs.Canceled = m.RunCanceled.Get()

	c, avg, p50, p95, p99 := m.RunDurations.Summary()
	snap.Runs.Latency.Count = c
	snap.Runs.Latency.Avg = avg
	snap.Runs.Latency.P50 = p50
	snap.Runs.Latency.P95 = p95
	snap.Runs.Latency.P99 = p99

	snap.Nodes = make(map[string]NodeMetricSnapshot)

	// Node maps are guarded by m.mu :)
	m.mu.Lock()
	defer m.mu.Unlock()

	for nodeID, timer := range m.NodeDurations {
		c2, avg2, p502, p952, p992 := timer.Summary()

		var succ, fail int64
		if m.NodeSucceeded[nodeID] != nil {
			succ = m.NodeSucceeded[nodeID].Get()
		}
		if m.NodeFailed[nodeID] != nil {
			fail = m.NodeFailed[nodeID].Get()
		}

		snap.Nodes[nodeID] = NodeMetricSnapshot{
			Count:     c2,
			Avg:       avg2,
			P50:       p502,
			P95:       p952,
			P99:       p992,
			Succeeded: succ,
			Failed:    fail,
		}
	}

	return snap
}
