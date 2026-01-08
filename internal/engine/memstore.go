package engine

import (
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"
)

var (
	ErrRunAlreadyExists = errors.New("run already exists")
	ErrRunNotFound      = errors.New("run not found")
)

// The minimal persistence layer for v2.0
type Store interface {
	CreateRun(r Run) error
	UpdateRun(r Run) error
	GetRun(runID string) (Run, bool)

	UpsertNodeExecution(n NodeExecution) error
	GetNodeExecution(runID, nodeID string, attempt int) (NodeExecution, bool)
	ListNodeExecutions(runID string) []NodeExecution

	AppendEvent(e RunEvent) (RunEvent, error)
	ListEvents(runID string) []RunEvent
}

type nodeKey struct {
	RunID   string
	NodeID  string
	Attempt int
}

type MemoryStore struct {
	mu      sync.RWMutex
	runs    map[string]Run
	nodes   map[nodeKey]NodeExecution
	events  map[string][]RunEvent
	nextSeq map[string]int64
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		runs:    make(map[string]Run),
		nodes:   make(map[nodeKey]NodeExecution),
		events:  make(map[string][]RunEvent),
		nextSeq: make(map[string]int64),
	}
}

func (s *MemoryStore) CreateRun(r Run) error {
	if err := r.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.runs[r.RunID]; ok {
		return ErrRunAlreadyExists
	}

	s.runs[r.RunID] = cloneRun(r)
	if _, ok := s.nextSeq[r.RunID]; !ok {
		s.nextSeq[r.RunID] = 1
	}

	return nil
}

func (s *MemoryStore) UpdateRun(r Run) error {
	if err := r.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.runs[r.RunID]; !ok {
		return ErrRunNotFound
	}

	s.runs[r.RunID] = cloneRun(r)
	return nil
}

func (s *MemoryStore) GetRun(runID string) (Run, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	r, ok := s.runs[runID]
	if !ok {
		return Run{}, false
	}
	return cloneRun(r), true
}

func (s *MemoryStore) UpsertNodeExecution(n NodeExecution) error {
	if err := n.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.runs[n.RunID]; !ok {
		return ErrRunNotFound
	}

	k := nodeKey{RunID: n.RunID, NodeID: n.NodeID, Attempt: n.Attempt}
	s.nodes[k] = cloneNodeExecution(n)
	return nil
}

func (s *MemoryStore) GetNodeExecution(runID, nodeID string, attempt int) (NodeExecution, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n, ok := s.nodes[nodeKey{RunID: runID, NodeID: nodeID, Attempt: attempt}]
	if !ok {
		return NodeExecution{}, false
	}

	return cloneNodeExecution(n), true
}

func (s *MemoryStore) ListNodeExecutions(runID string) []NodeExecution {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]NodeExecution, 0)
	for k, v := range s.nodes {
		if k.RunID == runID {
			out = append(out, cloneNodeExecution(v))
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].NodeID != out[j].NodeID {
			return out[i].NodeID < out[j].NodeID
		}
		return out[i].Attempt < out[j].Attempt
	})

	return out
}

func (s *MemoryStore) AppendEvent(e RunEvent) (RunEvent, error) {
	if e.At.IsZero() {
		e.At = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.runs[e.RunID]; !ok {
		return RunEvent{}, ErrRunNotFound
	}

	seq := s.nextSeq[e.RunID]
	if seq < 1 {
		seq = 1
	}
	s.nextSeq[e.RunID] = seq + 1

	// Always assign seq here to enforce append-only ordering
	e.Seq = seq

	if err := e.Validate(); err != nil {
		return RunEvent{}, err
	}

	stored := cloneRunEvent(e)
	s.events[e.RunID] = append(s.events[e.RunID], stored)

	return cloneRunEvent(stored), nil
}

func (s *MemoryStore) ListEvents(runID string) []RunEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()

	evs := s.events[runID]
	out := make([]RunEvent, 0, len(evs))

	for _, e := range evs {
		out = append(out, cloneRunEvent(e))
	}

	return out
}

func cloneRun(r Run) Run {
	out := r

	if r.StartedAt != nil {
		t := (*r.StartedAt).UTC()
		out.StartedAt = &t
	}

	if r.EndedAt != nil {
		t := (*r.EndedAt).UTC()
		out.EndedAt = &t
	}

	return out
}

func cloneRaw(b json.RawMessage) json.RawMessage {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func cloneNodeExecution(n NodeExecution) NodeExecution {
	out := n

	if n.StartedAt != nil {
		t := (*n.StartedAt).UTC()
		out.StartedAt = &t
	}

	if n.EndedAt != nil {
		t := (*n.EndedAt).UTC()
		out.EndedAt = &t
	}

	out.Input = cloneRaw(n.Input)
	out.Output = cloneRaw(n.Output)
	return out
}

func cloneRunEvent(e RunEvent) RunEvent {
	out := e
	out.Payload = cloneRaw(e.Payload)
	return out
}
