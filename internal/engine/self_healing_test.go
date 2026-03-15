package engine

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSelfHealing_AutomaticArtifactOnFailedRun(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)

	reg.Register("fail.step", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New("boom")
	})

	spec := []byte(`{"id":"wf_self_heal","nodes":[{"id":"explode","topic":"fail.step"}]}`)
	err := runner.RunSpecJSON(context.Background(), "run-self-heal", spec, reg, json.RawMessage(`{"task":"test"}`))
	if err == nil {
		t.Fatal("expected workflow failure")
	}

	run, ok := runner.store.GetRun("run-self-heal")
	if !ok || run.Status != RunStatusFailed {
		t.Fatalf("run=%+v ok=%v want failed run", run, ok)
	}

	if run.TerminalReason != "node_failed" {
		t.Fatalf("terminal_reason=%q want %q", run.TerminalReason, "node_failed")
	}

	artifact, ok, err := runner.GetSelfHealingArtifactByRun(context.Background(), "run-self-heal")
	if err != nil {
		t.Fatalf("GetSelfHealingArtifactByRun: %v", err)
	}

	if !ok {
		t.Fatal("expected self-healing artifact")
	}

	if artifact.FailureNodeID != "explode" {
		t.Fatalf("failure_node_id=%q want %q", artifact.FailureNodeID, "explode")
	}

	if artifact.EvalDatasetID == "" || artifact.EvalCaseID == "" {
		t.Fatalf("expected eval capture ids, got %+v", artifact)
	}

	if artifact.SaferRerun.Mode != ReplayTimeTravel {
		t.Fatalf("safer rerun mode=%q want %q", artifact.SaferRerun.Mode, ReplayTimeTravel)
	}

	if artifact.SaferRerun.SideEffectMode != SideEffectModeDryRun {
		t.Fatalf("safer side-effect mode=%q want %q", artifact.SaferRerun.SideEffectMode, SideEffectModeDryRun)
	}

	if len(artifact.ReplaySuggestions) < 2 {
		t.Fatalf("expected replay suggestions, got %+v", artifact.ReplaySuggestions)
	}

	dataset, ok, err := runner.GetEvalDataset(artifact.EvalDatasetID)
	if err != nil {
		t.Fatalf("GetEvalDataset: %v", err)
	}

	if !ok {
		t.Fatalf("expected eval dataset %q", artifact.EvalDatasetID)
	}

	found := false
	for _, evalCase := range dataset.Cases {
		if evalCase.ID != artifact.EvalCaseID {
			continue
		}

		found = true
		if evalCase.Labels["self_heal"] != "true" {
			t.Fatalf("expected self_heal label on eval case, got %+v", evalCase.Labels)
		}
	}

	if !found {
		t.Fatalf("expected eval case %q in dataset %+v", artifact.EvalCaseID, dataset.Cases)
	}
}

func TestSelfHealing_HTTPReplayUsesDryRunRecoveryPath(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)

	var seenModes []SideEffectMode
	reg.Register("side.effect", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		runtime, ok := SideEffectRuntimeFrom(ctx)

		if !ok {
			t.Fatal("expected side effect runtime")
		}

		seenModes = append(seenModes, runtime.Mode)
		if runtime.Mode != SideEffectModeDryRun {
			return nil, errors.New("commit path intentionally blocked")
		}

		return json.RawMessage(`{"preview":"ok"}`), nil
	})

	if err := runner.SaveToolGatewayBundle(ToolGatewayBundle{
		Tools: []ToolPolicy{{
			ID:       "side-effect",
			Tool:     "side.effect",
			Approved: true,
			SideEffect: &SideEffectPolicy{
				Enabled:         true,
				DryRunSupported: true,
			},
		}},
	}); err != nil {
		t.Fatalf("SaveToolGatewayBundle: %v", err)
	}

	spec := []byte(`{"id":"wf_side_effect_recover","nodes":[{"id":"act","topic":"side.effect"}]}`)
	err := runner.RunSpecJSON(context.Background(), "run-self-heal-replay", spec, reg, json.RawMessage(`{"task":"recover"}`))
	if err == nil {
		t.Fatal("expected initial failure")
	}

	mux := http.NewServeMux()
	AttachDebugRoutes(mux, runner)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	artifactBody := mustPostJSONStatus(t, srv.URL+"/debug/self-heal/artifact", `{"run_id":"run-self-heal-replay"}`, http.StatusOK)
	if !strings.Contains(string(artifactBody), `"run_id":"run-self-heal-replay"`) {
		t.Fatalf("unexpected artifact response: %s", string(artifactBody))
	}

	replayBody := mustPostJSONStatus(t, srv.URL+"/debug/self-heal/replay", `{"run_id":"run-self-heal-replay"}`, http.StatusOK)
	if !strings.Contains(string(replayBody), `"ok":true`) {
		t.Fatalf("unexpected replay response: %s", string(replayBody))
	}

	run, ok := runner.store.GetRun("run-self-heal-replay")
	if !ok || run.Status != RunStatusSucceeded {
		t.Fatalf("expected recovered succeeded run, got ok=%v run=%+v", ok, run)
	}

	if len(seenModes) < 2 {
		t.Fatalf("expected commit failure then dry-run replay, got modes=%v", seenModes)
	}

	if seenModes[0] == SideEffectModeDryRun {
		t.Fatalf("expected initial attempt to be non-dry-run, got %v", seenModes)
	}

	lastMode := seenModes[len(seenModes)-1]
	if lastMode != SideEffectModeDryRun {
		t.Fatalf("expected replay to use dry-run mode, got %v", seenModes)
	}
}
