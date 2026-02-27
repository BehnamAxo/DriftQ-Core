package multiagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/driftq-org/DriftQ-Core/internal/broker"
)

// StartupConfig configures v3.1 multi-agent routing + optional topic bootstrap in driftqd.
// It is intentionally small for the first usable cut.
type StartupConfig struct {
	Agents []string `json:"agents,omitempty"`
	Teams  []string `json:"teams,omitempty"`

	// capability -> []agent_id
	Capabilities map[string][]string `json:"capabilities,omitempty"`

	// Optional. If 0, defaults to 1
	TopicPartitions int `json:"topic_partitions,omitempty"`

	// Optional router behavior flags
	RouterStrict bool     `json:"router_strict,omitempty"`
	SourceTopics []string `json:"source_topics,omitempty"`
}

type BootstrapSummary struct {
	Created []string
	Skipped []string // => already existed
}

func LoadStartupConfig(path string) (StartupConfig, error) {
	var cfg StartupConfig
	p := strings.TrimSpace(path)
	if p == "" {
		return cfg, fmt.Errorf("multiagent config path is required")
	}

	b, err := os.ReadFile(p)
	if err != nil {
		return cfg, err
	}
	return ParseStartupConfig(b)
}

func ParseStartupConfig(b []byte) (StartupConfig, error) {
	var cfg StartupConfig
	if len(strings.TrimSpace(string(b))) == 0 {
		return cfg, fmt.Errorf("multiagent config is empty")
	}

	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()

	if err := dec.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("parse multiagent config: %w", err)
	}

	if err := cfg.NormalizeAndValidate(); err != nil {
		return cfg, err
	}

	return cfg, nil
}

func (c *StartupConfig) NormalizeAndValidate() error {
	if c == nil {
		return fmt.Errorf("multiagent config is nil")
	}

	c.Agents = dedupeNormalizedIDs(c.Agents)
	c.Teams = dedupeNormalizedIDs(c.Teams)
	c.SourceTopics = dedupeTrimmed(c.SourceTopics)

	for _, id := range c.Agents {
		if err := validateTopicIDComponent("agent id", id); err != nil {
			return err
		}
	}

	for _, id := range c.Teams {
		if err := validateTopicIDComponent("team id", id); err != nil {
			return err
		}
	}

	if c.TopicPartitions < 0 {
		return fmt.Errorf("topic_partitions must be >= 0")
	}

	if c.TopicPartitions == 0 {
		c.TopicPartitions = 1
	}

	if c.Capabilities == nil {
		c.Capabilities = map[string][]string{}
	}

	normCaps := make(map[string][]string, len(c.Capabilities))
	for rawCap, rawAgents := range c.Capabilities {
		cap := strings.ToLower(strings.TrimSpace(rawCap))
		if cap == "" {
			return fmt.Errorf("capabilities contains empty key")
		}

		if err := validateTopicIDComponent("capability", cap); err != nil {
			return err
		}

		agents := dedupeNormalizedIDs(rawAgents)
		for _, agentID := range agents {
			if err := validateTopicIDComponent("agent id", agentID); err != nil {
				return err
			}
		}
		normCaps[cap] = agents
	}
	c.Capabilities = normCaps

	for _, topic := range c.SourceTopics {
		if strings.TrimSpace(topic) == "" {
			return fmt.Errorf("source_topics cannot contain empty topic")
		}
	}

	return nil
}

func dedupeNormalizedIDs(in []string) []string {
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))

	for _, raw := range in {
		v := strings.TrimSpace(raw)
		if v == "" {
			continue
		}

		if _, ok := seen[v]; ok {
			continue
		}

		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func dedupeTrimmed(in []string) []string {
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))

	for _, raw := range in {
		v := strings.TrimSpace(raw)
		if v == "" {
			continue
		}

		if _, ok := seen[v]; ok {
			continue
		}

		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func (c StartupConfig) AllAgentIDs() []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(c.Agents)+len(c.Capabilities))

	for _, id := range c.Agents {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}

		if _, ok := seen[id]; ok {
			continue
		}

		seen[id] = struct{}{}
		out = append(out, id)
	}

	for _, ids := range c.Capabilities {
		for _, id := range ids {
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}

			if _, ok := seen[id]; ok {
				continue
			}

			seen[id] = struct{}{}
			out = append(out, id)
		}
	}

	sort.Strings(out)
	return out
}

func (c StartupConfig) DesiredTopics() ([]string, error) {
	partitions := c.TopicPartitions
	if partitions <= 0 {
		partitions = 1
	}

	_ = partitions // kept explicit for future per-topic partition config

	set := make(map[string]struct{})
	for _, agentID := range c.AllAgentIDs() {
		inbox, err := AgentInboxTopic(agentID)
		if err != nil {
			return nil, err
		}

		outbox, err := AgentOutboxTopic(agentID)
		if err != nil {
			return nil, err
		}

		set[inbox] = struct{}{}
		set[outbox] = struct{}{}
	}

	for _, teamID := range c.Teams {
		topic, err := TeamBroadcastTopic(teamID)
		if err != nil {
			return nil, err
		}
		set[topic] = struct{}{}
	}

	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}

	sort.Strings(out)
	return out, nil
}

func (c StartupConfig) BuildRegistry() (*CapabilityRegistry, error) {
	reg := NewCapabilityRegistry()
	for cap, agents := range c.Capabilities {
		if err := reg.Replace(cap, agents); err != nil {
			return nil, err
		}
	}

	return reg, nil
}

func (c StartupConfig) RouterConfig(reg *CapabilityRegistry) RouterConfig {
	return RouterConfig{
		Registry:     reg,
		Strict:       c.RouterStrict,
		SourceTopics: SourceTopicSet(c.SourceTopics...),
	}
}

func BootstrapTopics(ctx context.Context, b broker.Broker, cfg StartupConfig) (BootstrapSummary, error) {
	var out BootstrapSummary
	if b == nil {
		return out, fmt.Errorf("broker is nil")
	}

	if err := cfg.NormalizeAndValidate(); err != nil {
		return out, err
	}

	topics, err := cfg.DesiredTopics()
	if err != nil {
		return out, err
	}

	for _, t := range topics {
		err := b.CreateTopic(ctx, t, cfg.TopicPartitions)
		if err == nil {
			out.Created = append(out.Created, t)
			continue
		}

		if errors.Is(err, broker.ErrTopicExists) {
			out.Skipped = append(out.Skipped, t)
			continue
		}

		return out, fmt.Errorf("create topic %s: %w", t, err)
	}
	return out, nil
}
