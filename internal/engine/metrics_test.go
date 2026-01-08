package engine

import (
	"testing"
	"time"
)

func TestEngineMetrics_Basic(t *testing.T) {
	m := NewEngineMetrics()
	m.ObserveRun(RunStatusSucceeded, 120*time.Millisecond)
	m.ObserveRun(RunStatusFailed, 200*time.Millisecond)

	m.ObserveNode("A", true, 10*time.Millisecond)
	m.ObserveNode("A", false, 30*time.Millisecond)
	m.ObserveNode("B", true, 40*time.Millisecond)

	if m.RunSucceeded.Get() != 1 || m.RunFailed.Get() != 1 {
		t.Fatalf("unexpected run counters: %+v", m)
	}

	count, avg, p50, p95, _ := m.RunDurations.Summary()
	if count != 2 || avg == 0 || p50 == 0 || p95 == 0 {
		t.Fatalf("unexpected summary: %v %v %v %v", count, avg, p50, p95)
	}
}
