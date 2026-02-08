package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func Test_ReplayTimeTravel_DoesNotRerunSucceededNodes_AndArtifactsExist(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	store := NewMemoryStore()
	r := NewRunner(store)

	// Force artifacts for *everything* (limit==0 => never inline)
	r.artifactInlineLimit = 0

	root := t.TempDir()
	as, err := NewLocalArtifactStore(root)
	if err != nil {
		t.Fatalf("NewLocalArtifactStore: %v", err)
	}

	r.artifacts = as
	var callsA int32
	var callsB int32

	// deterministic, big, stable JSON bytes (no map ordering issues)
	aOut := []byte(`{"blob":"` + strings.Repeat("a", 2048) + `"}`)

	// A succeeds, B fails once then succeeds on replay
	g := WorkflowGraph{
		ID: "wf_replay_time_travel",
		Nodes: []NodeDef{
			{
				NodeID: "A",
				Run: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					atomic.AddInt32(&callsA, 1)
					return json.RawMessage(aOut), nil
				},
			},
			{
				NodeID: "B",
				Run: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					n := atomic.AddInt32(&callsB, 1)
					if n == 1 {
						return nil, errors.New("boom")
					}
					return json.RawMessage(`{"ok":true}`), nil
				},
			},
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
		},
	}

	runID := "run_failed_then_replay"
	err = r.RunDAG(ctx, runID, g, json.RawMessage(`{}`))
	if err == nil {
		t.Fatalf("expected first run to fail, got nil")
	}

	if got := atomic.LoadInt32(&callsA); got != 1 {
		t.Fatalf("expected A called once on failed run, got %d", got)
	}

	if got := atomic.LoadInt32(&callsB); got != 1 {
		t.Fatalf("expected B called once on failed run, got %d", got)
	}

	// Artifact for A output must exist (because artifactInlineLimit==0 => always store)
	wantID := sha256Hex(aOut)

	gotBytes, gotMeta, err := r.GetArtifact(ctx, wantID)
	if err != nil {
		t.Fatalf("GetArtifact(%s): %v", wantID, err)
	}

	if string(gotBytes) != string(aOut) {
		t.Fatalf("artifact bytes mismatch")
	}

	if gotMeta.ArtifactID != wantID || gotMeta.Sha256 != wantID {
		t.Fatalf("artifact meta mismatch: %+v", gotMeta)
	}

	if gotMeta.Size != int64(len(aOut)) {
		t.Fatalf("artifact size mismatch: got %d want %d", gotMeta.Size, len(aOut))
	}

	// Now time-travel replay should re-run ONLY the failed node(s), not A.
	err = r.Replay(ctx, runID, ReplayTimeTravel)
	if err != nil {
		t.Fatalf("Replay(time_travel) failed: %v", err)
	}

	if got := atomic.LoadInt32(&callsA); got != 1 {
		t.Fatalf("expected A NOT re-run during replay, callsA=%d", got)
	}

	if got := atomic.LoadInt32(&callsB); got != 2 {
		t.Fatalf("expected B re-run once during replay, callsB=%d", got)
	}

	run, ok := store.GetRun(runID)
	if !ok {
		t.Fatalf("run missing after replay")
	}

	if run.Status != RunStatusSucceeded {
		t.Fatalf("expected run succeeded after replay, got %q", run.Status)
	}
}

func Test_ReplayCache_ShortCircuitsOnFreshRun_AndDownstreamStillWorks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	store := NewMemoryStore()
	r := NewRunner(store)

	// Force artifacts for cached node finished payload too (even if small)
	r.artifactInlineLimit = 0

	root := t.TempDir()
	as, err := NewLocalArtifactStore(root)
	if err != nil {
		t.Fatalf("NewLocalArtifactStore: %v", err)
	}

	r.artifacts = as
	var callsA int32
	var callsB int32

	aOut := []byte(`{"value":123,"blob":"` + strings.Repeat("z", 1024) + `"}`)

	g := WorkflowGraph{
		ID: "wf_cache_short_circuit",
		Nodes: []NodeDef{
			{
				NodeID: "A",
				Run: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					atomic.AddInt32(&callsA, 1)
					return json.RawMessage(aOut), nil
				},
			},
			{
				NodeID: "B",
				Run: func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
					atomic.AddInt32(&callsB, 1)

					// B must receive aggregated input that includes A output.
					var agg map[string]json.RawMessage
					if err := json.Unmarshal(in, &agg); err != nil {
						t.Fatalf("B got non-agg input: %s err=%v", string(in), err)
					}
					gotA, ok := agg["A"]
					if !ok {
						t.Fatalf("B input missing key A: %s", string(in))
					}
					if string(gotA) != string(aOut) {
						t.Fatalf("B saw wrong A output.\n got: %s\nwant: %s", string(gotA), string(aOut))
					}

					return json.RawMessage(`{"b":true}`), nil
				},
			},
		},
		Edges: []NodeEdge{
			{From: "A", To: "B"},
		},
	}

	// 1) Source run produces outputs we want to cache
	srcRunID := "run_src"
	if err := r.RunDAG(ctx, srcRunID, g, json.RawMessage(`{}`)); err != nil {
		t.Fatalf("src RunDAG failed: %v", err)
	}

	if got := atomic.LoadInt32(&callsA); got != 1 {
		t.Fatalf("expected A called once in src run, got %d", got)
	}

	if got := atomic.LoadInt32(&callsB); got != 1 {
		t.Fatalf("expected B called once in src run, got %d", got)
	}

	// Build replay cache from src run, but ONLY keep A (so B still executes and proves downstream wiring)
	cache := r.buildReplayCacheFromRun(srcRunID)
	delete(cache, "B")

	// 2) Fresh run uses replay cache: A must NOT execute; B must execute and see A output
	dstRunID := "run_dst_cached"
	if err := r.runDAGWithReplayCache(ctx, dstRunID, g, json.RawMessage(`{}`), nil, cache); err != nil {
		t.Fatalf("dst runDAGWithReplayCache failed: %v", err)
	}

	if got := atomic.LoadInt32(&callsA); got != 1 {
		t.Fatalf("expected A NOT called again on cached run, callsA=%d", got)
	}
	if got := atomic.LoadInt32(&callsB); got != 2 {
		t.Fatalf("expected B called again on cached run, callsB=%d", got)
	}

// Proof: cached run must emit used_cached_output=true for node A finished event
events := store.ListEvents(dstRunID)
found := false
for _, e := range events {
	if e.Type == EventNodeFinished && e.NodeID == "A" {
		var p NodeFinishedPayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			t.Fatalf("unmarshal node finished payload: %v", err)
		}
		if !p.UsedCachedOutput {
			t.Fatalf("expected used_cached_output=true on cached node A")
		}
		if p.CachedAttempt != 1 {
			t.Fatalf("expected cached_attempt=1 on cached node A, got=%d", p.CachedAttempt)
		}
		if p.StartedAt == nil || p.EndedAt == nil {
			t.Fatalf("expected started_at/ended_at on cached node A payload")
		}
		found = true
		break
	}
}
if !found {
	t.Fatalf("expected EventNodeFinished for node A in cached run")
}

	// Also prove the cached output was artifact-stored (content-addressed)
	wantID := sha256Hex(aOut)

	// Give filesystem a tiny moment on slow Windows AV setups (atomic write usually makes this unnecessary,
	// but this avoids flaky failures on some dev machines).
	deadline := time.Now().Add(2 * time.Second)
	for {
		gotBytes, _, err := r.GetArtifact(ctx, wantID)
		if err == nil {
			if string(gotBytes) != string(aOut) {
				t.Fatalf("artifact bytes mismatch in cached run")
			}
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("expected artifact %s to exist, last err=%v", wantID, err)
		}

		time.Sleep(25 * time.Millisecond)
	}
}
