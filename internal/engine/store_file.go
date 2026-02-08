package engine

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"
)

// FileStore is a minimal durable Store backed by an append-only JSONL WAL.
//
// Goals (v2.9):
//   - persist runs/events/nodes/timers across restarts
//   - enforce append-only event sequencing
//   - keep implementation dependency-free (stdlib only)
//
// This is intentionally simple: it fsyncs on every write
// Compaction/snapshots can be added later
type FileStore struct {
	mu sync.RWMutex

	runs    map[string]Run
	nodes   map[nodeKey]NodeExecution
	events  map[string][]RunEvent
	nextSeq map[string]int64
	timers  map[string]Timer

	wal *engineWAL
}

type walOp string

const (
	opCreateRun   walOp = "create_run"
	opUpdateRun   walOp = "update_run"
	opUpsertNode  walOp = "upsert_node"
	opAppendEvent walOp = "append_event"
	opUpsertTimer walOp = "upsert_timer"
)

type walRecord struct {
	Op    walOp           `json:"op"`
	At    time.Time       `json:"at"`
	Value json.RawMessage `json:"value"`
}

// OpenFileStore opens/creates a WAL file at path and replays it to rebuild state
func OpenFileStore(path string) (*FileStore, error) {
	wal, err := openEngineWAL(path)
	if err != nil {
		return nil, err
	}

	s := &FileStore{
		runs:    make(map[string]Run),
		nodes:   make(map[nodeKey]NodeExecution),
		events:  make(map[string][]RunEvent),
		nextSeq: make(map[string]int64),
		timers:  make(map[string]Timer),
		wal:     wal,
	}

	if err := s.replayLocked(); err != nil {
		_ = wal.Close()
		return nil, err
	}

	return s, nil
}

// Close closes the underlying WAL file
func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal == nil {
		return nil
	}
	return s.wal.Close()
}

func (s *FileStore) replayLocked() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	records, err := s.wal.Replay()
	if err != nil {
		return err
	}

	// Rebuild from scratch
	s.runs = make(map[string]Run)
	s.nodes = make(map[nodeKey]NodeExecution)
	s.events = make(map[string][]RunEvent)
	s.nextSeq = make(map[string]int64)
	s.timers = make(map[string]Timer)

	for _, rec := range records {
		if err := s.applyRecord(rec); err != nil {
			return err
		}
	}

	// Ensure every run has a nextSeq initialized
	for runID := range s.runs {
		if _, ok := s.nextSeq[runID]; !ok {
			s.nextSeq[runID] = 1
		}
	}

	return nil
}

func (s *FileStore) applyRecord(rec walRecord) error {
	switch rec.Op {
	case opCreateRun:
		var r Run
		if err := json.Unmarshal(rec.Value, &r); err != nil {
			return fmt.Errorf("replay create_run: %w", err)
		}

		if err := r.Validate(); err != nil {
			return fmt.Errorf("replay create_run validate: %w", err)
		}

		s.runs[r.RunID] = cloneRun(r)
		if _, ok := s.nextSeq[r.RunID]; !ok {
			s.nextSeq[r.RunID] = 1
		}

		return nil

	case opUpdateRun:
		var r Run
		if err := json.Unmarshal(rec.Value, &r); err != nil {
			return fmt.Errorf("replay update_run: %w", err)
		}

		if err := r.Validate(); err != nil {
			return fmt.Errorf("replay update_run validate: %w", err)
		}

		// Update is idempotent during replay
		s.runs[r.RunID] = cloneRun(r)
		if _, ok := s.nextSeq[r.RunID]; !ok {
			s.nextSeq[r.RunID] = 1
		}
		return nil

	case opUpsertNode:
		var n NodeExecution
		if err := json.Unmarshal(rec.Value, &n); err != nil {
			return fmt.Errorf("replay upsert_node: %w", err)
		}

		if err := n.Validate(); err != nil {
			return fmt.Errorf("replay upsert_node validate: %w", err)
		}

		s.nodes[nodeKey{RunID: n.RunID, NodeID: n.NodeID, Attempt: n.Attempt}] = cloneNodeExecution(n)
		return nil

	case opAppendEvent:
		var e RunEvent
		if err := json.Unmarshal(rec.Value, &e); err != nil {
			return fmt.Errorf("replay append_event: %w", err)
		}
		if err := e.Validate(); err != nil {
			return fmt.Errorf("replay append_event validate: %w", err)
		}

		s.events[e.RunID] = append(s.events[e.RunID], cloneRunEvent(e))

		// Advance nextSeq to max(seq)+1
		next := e.Seq + 1
		if cur, ok := s.nextSeq[e.RunID]; !ok || next > cur {
			s.nextSeq[e.RunID] = next
		}
		return nil

	case opUpsertTimer:
		var t Timer
		if err := json.Unmarshal(rec.Value, &t); err != nil {
			return fmt.Errorf("replay upsert_timer: %w", err)
		}

		// Timer.Validate doesn't exist; keep minimal invariants
		if t.RunID == "" || t.NodeID == "" || t.Attempt < 1 {
			return fmt.Errorf("replay upsert_timer: invalid timer fields")
		}

		s.timers[timerKey(t.RunID, t.NodeID, t.Attempt)] = cloneTimer(t)
		return nil

	default:
		return fmt.Errorf("unknown wal op: %q", rec.Op)
	}
}

func (s *FileStore) CreateRun(r Run) error {
	if err := r.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.runs[r.RunID]; ok {
		return ErrRunAlreadyExists
	}

	// Persist first, then apply.
	if err := s.wal.Append(opCreateRun, r); err != nil {
		return err
	}

	s.runs[r.RunID] = cloneRun(r)
	if _, ok := s.nextSeq[r.RunID]; !ok {
		s.nextSeq[r.RunID] = 1
	}
	return nil
}

func (s *FileStore) UpdateRun(r Run) error {
	if err := r.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.runs[r.RunID]; !ok {
		return ErrRunNotFound
	}

	if err := s.wal.Append(opUpdateRun, r); err != nil {
		return err
	}

	s.runs[r.RunID] = cloneRun(r)
	return nil
}

func (s *FileStore) GetRun(runID string) (Run, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	r, ok := s.runs[runID]
	if !ok {
		return Run{}, false
	}

	return cloneRun(r), true
}

func (s *FileStore) ListRuns() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]string, 0, len(s.runs))
	for id := range s.runs {
		ids = append(ids, id)
	}

	// Same ordering as MemoryStore
	sort.Slice(ids, func(i, j int) bool {
		ri := s.runs[ids[i]]
		rj := s.runs[ids[j]]

		ti := time.Time{}
		if ri.EndedAt != nil {
			ti = (*ri.EndedAt).UTC()
		} else if ri.StartedAt != nil {
			ti = (*ri.StartedAt).UTC()
		}

		tj := time.Time{}
		if rj.EndedAt != nil {
			tj = (*rj.EndedAt).UTC()
		} else if rj.StartedAt != nil {
			tj = (*rj.StartedAt).UTC()
		}

		if ti.Equal(tj) {
			return ids[i] < ids[j]
		}

		if ti.IsZero() {
			return false
		}

		if tj.IsZero() {
			return true
		}

		return ti.After(tj)
	})

	return ids
}

func (s *FileStore) UpsertNodeExecution(n NodeExecution) error {
	if err := n.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.runs[n.RunID]; !ok {
		return ErrRunNotFound
	}

	if err := s.wal.Append(opUpsertNode, n); err != nil {
		return err
	}

	s.nodes[nodeKey{RunID: n.RunID, NodeID: n.NodeID, Attempt: n.Attempt}] = cloneNodeExecution(n)
	return nil
}

func (s *FileStore) GetNodeExecution(runID, nodeID string, attempt int) (NodeExecution, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n, ok := s.nodes[nodeKey{RunID: runID, NodeID: nodeID, Attempt: attempt}]
	if !ok {
		return NodeExecution{}, false
	}

	return cloneNodeExecution(n), true
}

func (s *FileStore) ListNodeExecutions(runID string) []NodeExecution {
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

func (s *FileStore) AppendEvent(e RunEvent) (RunEvent, error) {
	if e.At.IsZero() {
		e.At = time.Now().UTC()
	} else {
		e.At = e.At.UTC()
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

	e.Seq = seq
	s.nextSeq[e.RunID] = seq + 1

	if err := e.Validate(); err != nil {
		return RunEvent{}, err
	}

	// Persist first, then apply
	if err := s.wal.Append(opAppendEvent, e); err != nil {
		// Roll back nextSeq increment on failure
		s.nextSeq[e.RunID] = seq
		return RunEvent{}, err
	}

	stored := cloneRunEvent(e)
	s.events[e.RunID] = append(s.events[e.RunID], stored)
	return cloneRunEvent(stored), nil
}

func (s *FileStore) ListEvents(runID string) []RunEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()

	evs := s.events[runID]
	out := make([]RunEvent, 0, len(evs))
	for _, e := range evs {
		out = append(out, cloneRunEvent(e))
	}
	return out
}

func (s *FileStore) UpsertTimer(t Timer) error {
	// Minimal validation
	if t.RunID == "" || t.NodeID == "" || t.Attempt < 1 {
		return errors.New("invalid timer")
	}
	t.FireAt = t.FireAt.UTC()
	t.CreatedAt = t.CreatedAt.UTC()
	if t.FiredAt != nil {
		x := (*t.FiredAt).UTC()
		t.FiredAt = &x
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.runs[t.RunID]; !ok {
		return ErrRunNotFound
	}

	if err := s.wal.Append(opUpsertTimer, t); err != nil {
		return err
	}

	s.timers[timerKey(t.RunID, t.NodeID, t.Attempt)] = cloneTimer(t)
	return nil
}

func (s *FileStore) GetTimer(runID, nodeID string, attempt int) (Timer, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.timers[timerKey(runID, nodeID, attempt)]
	if !ok {
		return Timer{}, false
	}

	return cloneTimer(t), true
}

func (s *FileStore) ListTimers(runID string) []Timer {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Timer, 0)
	for _, t := range s.timers {
		if t.RunID == runID {
			out = append(out, cloneTimer(t))
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].FireAt.Equal(out[j].FireAt) {
			if out[i].NodeID == out[j].NodeID {
				return out[i].Attempt < out[j].Attempt
			}

			return out[i].NodeID < out[j].NodeID
		}

		return out[i].FireAt.Before(out[j].FireAt)
	})

	return out
}

func (s *FileStore) ListDueTimers(now time.Time) []Timer {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now = now.UTC()
	out := make([]Timer, 0)
	for _, t := range s.timers {
		if t.Status != TimerScheduled {
			continue
		}
		if !t.FireAt.After(now) {
			out = append(out, cloneTimer(t))
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].FireAt.Equal(out[j].FireAt) {
			if out[i].RunID == out[j].RunID {
				if out[i].NodeID == out[j].NodeID {
					return out[i].Attempt < out[j].Attempt
				}
				return out[i].NodeID < out[j].NodeID
			}
			return out[i].RunID < out[j].RunID
		}
		return out[i].FireAt.Before(out[j].FireAt)
	})

	return out
}

// WAL implementation section

type engineWAL struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

func openEngineWAL(path string) (*engineWAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &engineWAL{f: f, path: path}, nil
}

func (w *engineWAL) Append(op walOp, v any) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	vb, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal wal value: %w", err)
	}

	rec := walRecord{Op: op, At: time.Now().UTC(), Value: vb}
	line, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal wal record: %w", err)
	}

	if _, err := w.f.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("write wal record: %w", err)
	}

	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("fsync wal: %w", err)
	}
	return nil
}

func (w *engineWAL) Replay() ([]walRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek wal start: %w", err)
	}

	scanner := bufio.NewScanner(w.f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 10*1024*1024)

	out := make([]walRecord, 0)
	for scanner.Scan() {
		b := scanner.Bytes()
		if len(b) == 0 {
			continue
		}
		var rec walRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return nil, fmt.Errorf("decode wal record: %w", err)
		}
		out = append(out, rec)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan wal: %w", err)
	}

	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seek wal end: %w", err)
	}
	return out, nil
}

func (w *engineWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}
