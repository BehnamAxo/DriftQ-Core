package multiagent

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/driftq-org/DriftQ-Core/internal/broker"
)

func TestParseStartupConfig_NormalizesAndValidates(t *testing.T) {
	raw := map[string]any{
		"agents": []string{"planner", " coder ", "planner", ""},
		"teams":  []string{"core", "core", "ops"},
		"capabilities": map[string][]string{
			"Coding": {"coder", "coder", ""},
			"review": {"reviewer"},
		},
		"topic_partitions": 2,
		"router_strict":    true,
		"source_topics":    []string{"agent-ingress", "agent-ingress", " jobs "},
	}
	b, _ := json.Marshal(raw)

	cfg, err := ParseStartupConfig(b)
	if err != nil {
		t.Fatalf("ParseStartupConfig err: %v", err)
	}

	if cfg.TopicPartitions != 2 {
		t.Fatalf("TopicPartitions=%d want 2", cfg.TopicPartitions)
	}

	if !cfg.RouterStrict {
		t.Fatalf("RouterStrict=false want true")
	}

	if got, want := cfg.Agents, []string{"planner", "coder"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Agents mismatch\n got=%v\nwant=%v", got, want)
	}

	if got, want := cfg.Teams, []string{"core", "ops"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Teams mismatch\n got=%v\nwant=%v", got, want)
	}

	if got, want := cfg.SourceTopics, []string{"agent-ingress", "jobs"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("SourceTopics mismatch\n got=%v\nwant=%v", got, want)
	}

	if _, ok := cfg.Capabilities["coding"]; !ok {
		t.Fatalf("expected normalized capability key 'coding' in map: %v", cfg.Capabilities)
	}

	if got, want := cfg.Capabilities["coding"], []string{"coder"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("capabilities[coding] mismatch got=%v want=%v", got, want)
	}
}

func TestParseStartupConfig_DefaultsTopicPartitionsToOne(t *testing.T) {
	cfg, err := ParseStartupConfig([]byte(`{"agents":["planner"]}`))
	if err != nil {
		t.Fatalf("ParseStartupConfig err: %v", err)
	}

	if cfg.TopicPartitions != 1 {
		t.Fatalf("TopicPartitions=%d want 1", cfg.TopicPartitions)
	}
}

func TestStartupConfig_DesiredTopics_UsesAgentUnionFromCapabilities(t *testing.T) {
	cfg := StartupConfig{
		Agents:          []string{"planner"},
		Teams:           []string{"core"},
		Capabilities:    map[string][]string{"coding": {"coder"}},
		TopicPartitions: 1,
	}

	if err := cfg.NormalizeAndValidate(); err != nil {
		t.Fatalf("NormalizeAndValidate: %v", err)
	}

	topics, err := cfg.DesiredTopics()
	if err != nil {
		t.Fatalf("DesiredTopics: %v", err)
	}

	want := []string{
		"agent.coder.inbox",
		"agent.coder.outbox",
		"agent.planner.inbox",
		"agent.planner.outbox",
		"team.core.broadcast",
	}

	if !reflect.DeepEqual(topics, want) {
		t.Fatalf("topics mismatch\n got=%v\nwant=%v", topics, want)
	}
}

func TestBootstrapTopics_CreatesAndSkipsExisting(t *testing.T) {
	b := broker.NewInMemoryBroker()
	ctx := context.Background()
	if err := b.CreateTopic(ctx, "agent.planner.inbox", 1); err != nil {
		t.Fatalf("seed CreateTopic: %v", err)
	}

	cfg := StartupConfig{
		Agents:          []string{"planner", "coder"},
		Teams:           []string{"core"},
		TopicPartitions: 1,
	}

	summary, err := BootstrapTopics(ctx, b, cfg)
	if err != nil {
		t.Fatalf("BootstrapTopics err: %v", err)
	}

	if len(summary.Skipped) != 1 || summary.Skipped[0] != "agent.planner.inbox" {
		t.Fatalf("unexpected skipped topics: %v", summary.Skipped)
	}

	if len(summary.Created) != 4 {
		t.Fatalf("created count=%d want 4 (%v)", len(summary.Created), summary.Created)
	}

	gotTopics, err := b.ListTopics(ctx)
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}

	wantAll := []string{
		"agent.coder.inbox",
		"agent.coder.outbox",
		"agent.planner.inbox",
		"agent.planner.outbox",
		"team.core.broadcast",
	}

	if !reflect.DeepEqual(gotTopics, wantAll) {
		t.Fatalf("ListTopics mismatch\n got=%v\nwant=%v", gotTopics, wantAll)
	}
}
