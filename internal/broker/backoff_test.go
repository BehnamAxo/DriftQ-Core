package broker

import (
	"testing"
	"time"
)

func TestComputeBackoff(t *testing.T) {
	// nil or invalid inputs
	if got := computeBackoff(nil, 1); got != 0 {
		t.Fatalf("expected 0, got %v", got)
	}

	if got := computeBackoff(&RetryPolicy{BackoffMs: 100}, 0); got != 0 {
		t.Fatalf("expected 0 for retryNumber=0, got %v", got)
	}

	if got := computeBackoff(&RetryPolicy{BackoffMs: 0}, 2); got != 0 {
		t.Fatalf("expected 0 for BackoffMs=0, got %v", got)
	}

	p := &RetryPolicy{BackoffMs: 100}
	if got := computeBackoff(p, 1); got != 100*time.Millisecond {
		t.Fatalf("expected 100ms, got %v", got)
	}

	if got := computeBackoff(p, 2); got != 200*time.Millisecond {
		t.Fatalf("expected 200ms, got %v", got)
	}

	if got := computeBackoff(p, 3); got != 400*time.Millisecond {
		t.Fatalf("expected 400ms, got %v", got)
	}

	// clamp
	p2 := &RetryPolicy{BackoffMs: 200, MaxBackoffMs: 150}
	if got := computeBackoff(p2, 1); got != 150*time.Millisecond {
		t.Fatalf("expected clamp on first retry, got %v", got)
	}

	p3 := &RetryPolicy{BackoffMs: 100, MaxBackoffMs: 150}
	if got := computeBackoff(p3, 1); got != 100*time.Millisecond {
		t.Fatalf("expected 100ms, got %v", got)
	}

	if got := computeBackoff(p3, 2); got != 150*time.Millisecond {
		t.Fatalf("expected clamp at 150ms, got %v", got)
	}

	if got := computeBackoff(p3, 10); got != 150*time.Millisecond {
		t.Fatalf("expected clamp at 150ms even for large retryNumber, got %v", got)
	}
}
