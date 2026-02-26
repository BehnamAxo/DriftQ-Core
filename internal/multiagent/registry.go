package multiagent

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	ErrNilRegistry           = errors.New("capability registry is nil")
	ErrEmptyCapability       = errors.New("capability is required")
	ErrCapabilityNotFound    = errors.New("capability not found")
	ErrCapabilityHasNoAgents = errors.New("capability has no registered agents")
)

// CapabilityRegistry is the v3.1 foundational in-memory registry used by the
// multi-agent router to resolve capability/role -> agent inbox.
// It intentionally stays simple for v3.1:
//   - in-memory only
//   - deterministic round-robin selection per capability
//   - no persistence / health / weights / policies (those come later)
//
// CapabilityRegistry is basically a small in-memory phonebook for agent skills. Instead of hardcoding "send this to agent X",
// we can say "this needs coding" and the registry tells us which agent should get it. For v3.1 it keeps things simple:
// capability -> list of agent IDs, and each ResolveAgent call picks the next one in round-robin order.
type CapabilityRegistry struct {
	mu sync.Mutex

	capToAgents map[string][]string
	rrCursor    map[string]int
}

func NewCapabilityRegistry() *CapabilityRegistry {
	return &CapabilityRegistry{
		capToAgents: make(map[string][]string),
		rrCursor:    make(map[string]int),
	}
}

func normalizeCapability(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func normalizeAgentID(s string) string {
	return strings.TrimSpace(s)
}

func (r *CapabilityRegistry) Register(capability string, agentIDs ...string) error {
	if r == nil {
		return ErrNilRegistry
	}

	cap := normalizeCapability(capability)
	if cap == "" {
		return ErrEmptyCapability
	}

	if err := validateTopicIDComponent("capability", cap); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	seen := make(map[string]struct{})
	for _, existing := range r.capToAgents[cap] {
		seen[existing] = struct{}{}
	}

	for _, raw := range agentIDs {
		agentID := normalizeAgentID(raw)
		if agentID == "" {
			continue
		}

		if err := validateTopicIDComponent("agent id", agentID); err != nil {
			return err
		}

		if _, ok := seen[agentID]; ok {
			continue
		}
		r.capToAgents[cap] = append(r.capToAgents[cap], agentID)
		seen[agentID] = struct{}{}
	}

	return nil
}

func (r *CapabilityRegistry) Replace(capability string, agentIDs []string) error {
	if r == nil {
		return ErrNilRegistry
	}

	cap := normalizeCapability(capability)
	if cap == "" {
		return ErrEmptyCapability
	}

	if err := validateTopicIDComponent("capability", cap); err != nil {
		return err
	}

	next := make([]string, 0, len(agentIDs))
	seen := make(map[string]struct{}, len(agentIDs))
	for _, raw := range agentIDs {
		agentID := normalizeAgentID(raw)
		if agentID == "" {
			continue
		}

		if err := validateTopicIDComponent("agent id", agentID); err != nil {
			return err
		}

		if _, ok := seen[agentID]; ok {
			continue
		}

		next = append(next, agentID)
		seen[agentID] = struct{}{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.capToAgents[cap] = next
	r.rrCursor[cap] = 0
	return nil
}

func (r *CapabilityRegistry) Snapshot() map[string][]string {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string][]string, len(r.capToAgents))

	for k, v := range r.capToAgents {
		cp := append([]string(nil), v...)
		out[k] = cp
	}

	return out
}

func (r *CapabilityRegistry) ListCapabilities() []string {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	caps := make([]string, 0, len(r.capToAgents))

	for k := range r.capToAgents {
		caps = append(caps, k)
	}

	sort.Strings(caps)
	return caps
}

// ResolveAgent deterministically selects an agent for the capability using
// per-capability round-robin.
func (r *CapabilityRegistry) ResolveAgent(capability string) (string, error) {
	if r == nil {
		return "", ErrNilRegistry
	}

	cap := normalizeCapability(capability)
	if cap == "" {
		return "", ErrEmptyCapability
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	agents, ok := r.capToAgents[cap]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrCapabilityNotFound, cap)
	}

	if len(agents) == 0 {
		return "", fmt.Errorf("%w: %s", ErrCapabilityHasNoAgents, cap)
	}

	idx := r.rrCursor[cap]
	if idx < 0 {
		idx = 0
	}

	selected := agents[idx%len(agents)]
	r.rrCursor[cap] = (idx + 1) % len(agents)
	return selected, nil
}
