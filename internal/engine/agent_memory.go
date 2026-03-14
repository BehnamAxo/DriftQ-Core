package engine

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"mime"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

var (
	ErrAgentMemoryNotFound          = errors.New("agent memory not found")
	ErrAgentMemoryAccessDenied      = errors.New("agent memory access denied")
	ErrAgentMemoryReplayWriteDenied = errors.New("agent memory writes are blocked during replay")
	ErrAgentMemoryUnsupportedSource = errors.New("agent memory source is unsupported")
)

var agentMemoryIDRe = regexp.MustCompile(`^[a-zA-Z0-9._:-]+$`)

const (
	agentMemoryVectorDims = 128
)

type AgentMemorySourceKind string

const (
	AgentMemorySourceNote     AgentMemorySourceKind = "note"
	AgentMemorySourceRun      AgentMemorySourceKind = "run"
	AgentMemorySourceArtifact AgentMemorySourceKind = "artifact"
	AgentMemorySourceState    AgentMemorySourceKind = "state"
)

type AgentMemoryEntry struct {
	ID           string                `json:"id"`
	TenantID     string                `json:"tenant_id"`
	AgentID      string                `json:"agent_id"`
	SourceKind   AgentMemorySourceKind `json:"source_kind"`
	Content      string                `json:"content"`
	Vector       []float64             `json:"vector,omitempty"`
	CreatedAt    time.Time             `json:"created_at"`
	TraceID      string                `json:"trace_id,omitempty"`
	PrincipalID  string                `json:"principal_id,omitempty"`
	RunID        string                `json:"run_id,omitempty"`
	WorkflowID   string                `json:"workflow_id,omitempty"`
	NodeID       string                `json:"node_id,omitempty"`
	Attempt      int                   `json:"attempt,omitempty"`
	ArtifactID   string                `json:"artifact_id,omitempty"`
	StateVersion int                   `json:"state_version,omitempty"`
	Labels       map[string]string     `json:"labels,omitempty"`
}

type AgentMemoryWriteRequest struct {
	AgentID      string                `json:"agent_id"`
	SourceKind   AgentMemorySourceKind `json:"source_kind,omitempty"`
	Content      string                `json:"content,omitempty"`
	RunID        string                `json:"run_id,omitempty"`
	ArtifactID   string                `json:"artifact_id,omitempty"`
	StateVersion int                   `json:"state_version,omitempty"`
	Labels       map[string]string     `json:"labels,omitempty"`
}

type AgentMemorySearchRequest struct {
	AgentID       string    `json:"agent_id"`
	Query         string    `json:"query"`
	Limit         int       `json:"limit,omitempty"`
	MinScore      float64   `json:"min_score,omitempty"`
	CreatedBefore time.Time `json:"created_before,omitempty"`
}

type AgentMemorySearchResult struct {
	Score float64          `json:"score"`
	Entry AgentMemoryEntry `json:"entry"`
}

type AgentMemoryAccessor interface {
	WriteAgentMemory(ctx context.Context, req AgentMemoryWriteRequest) (AgentMemoryEntry, error)
	SearchAgentMemory(ctx context.Context, req AgentMemorySearchRequest) ([]AgentMemorySearchResult, error)
	ListAgentMemory(ctx context.Context, agentID string, limit int) ([]AgentMemoryEntry, error)
}

type MemoryEmbedder interface {
	Embed(text string) []float64
}

type agentMemoryCtxKey struct{}

func WithAgentMemoryContext(ctx context.Context, accessor AgentMemoryAccessor) context.Context {
	if accessor == nil {
		return ctx
	}

	return context.WithValue(ctx, agentMemoryCtxKey{}, accessor)
}

func AgentMemoryFrom(ctx context.Context) AgentMemoryAccessor {
	v := ctx.Value(agentMemoryCtxKey{})
	accessor, _ := v.(AgentMemoryAccessor)

	return accessor
}

type hashedTextEmbedder struct {
	dims int
}

func newHashedTextEmbedder(dims int) MemoryEmbedder {
	if dims < 16 {
		dims = agentMemoryVectorDims
	}

	return hashedTextEmbedder{dims: dims}
}

func (h hashedTextEmbedder) Embed(text string) []float64 {
	vector := make([]float64, h.dims)
	tokens := semanticTokens(text)

	if len(tokens) == 0 {
		return vector
	}

	for _, token := range tokens {
		hasher := fnv.New64a()
		_, _ = hasher.Write([]byte(token))
		sum := hasher.Sum64()
		idx := int(sum % uint64(h.dims))
		sign := 1.0

		if (sum>>63)&1 == 1 {
			sign = -1.0
		}

		vector[idx] += sign
	}

	normalizeVector(vector)
	return vector
}

func semanticTokens(text string) []string {
	fields := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		switch {
		case r >= 'a' && r <= 'z':
			return false
		case r >= '0' && r <= '9':
			return false
		case r == '_':
			return false
		default:
			return true
		}
	})

	out := make([]string, 0, len(fields))
	for _, token := range fields {
		token = strings.TrimSpace(token)
		if len(token) < 2 {
			continue
		}
		out = append(out, token)
	}

	return out
}

func normalizeVector(vector []float64) {
	var sumSquares float64
	for _, value := range vector {
		sumSquares += value * value
	}

	if sumSquares == 0 {
		return
	}

	norm := math.Sqrt(sumSquares)
	for i := range vector {
		vector[i] /= norm
	}
}

func cosineSimilarity(a, b []float64) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}

	if len(a) > len(b) {
		a, b = b, a
	}

	var dot float64
	for i := range a {
		dot += a[i] * b[i]
	}

	return dot
}

func agentMemoryKeyPart(v string) string {
	return url.PathEscape(strings.TrimSpace(v))
}

func agentMemoryIndexKey(tenantID, agentID string) string {
	return "agent_memory/index/" + agentMemoryKeyPart(tenantID) + "/" + agentMemoryKeyPart(agentID)
}

func agentMemoryEntryKey(tenantID, agentID, memoryID string) string {
	return "agent_memory/entries/" + agentMemoryKeyPart(tenantID) + "/" + agentMemoryKeyPart(agentID) + "/" + agentMemoryKeyPart(memoryID)
}

func normalizeMemoryID(memoryID string) (string, error) {
	memoryID = strings.TrimSpace(memoryID)
	if memoryID == "" {
		return "", fmt.Errorf("%w: memory id is required", ErrAgentMemoryAccessDenied)
	}

	if !agentMemoryIDRe.MatchString(memoryID) {
		return "", fmt.Errorf("%w: memory id %q must match %s", ErrAgentMemoryAccessDenied, memoryID, agentMemoryIDRe.String())
	}

	return memoryID, nil
}

func cloneAgentMemoryEntry(entry AgentMemoryEntry) AgentMemoryEntry {
	cloned := entry
	cloned.TenantID = strings.TrimSpace(entry.TenantID)
	cloned.AgentID = strings.TrimSpace(entry.AgentID)
	cloned.ID = strings.TrimSpace(entry.ID)
	cloned.SourceKind = AgentMemorySourceKind(strings.TrimSpace(string(entry.SourceKind)))
	cloned.Content = strings.TrimSpace(entry.Content)
	cloned.TraceID = strings.TrimSpace(entry.TraceID)
	cloned.PrincipalID = strings.TrimSpace(entry.PrincipalID)
	cloned.RunID = strings.TrimSpace(entry.RunID)
	cloned.WorkflowID = strings.TrimSpace(entry.WorkflowID)
	cloned.NodeID = strings.TrimSpace(entry.NodeID)
	cloned.ArtifactID = strings.TrimSpace(entry.ArtifactID)
	cloned.CreatedAt = entry.CreatedAt.UTC()

	if len(entry.Vector) > 0 {
		cloned.Vector = append([]float64(nil), entry.Vector...)
	}

	if len(entry.Labels) > 0 {
		cloned.Labels = make(map[string]string, len(entry.Labels))
		for k, v := range entry.Labels {
			k = strings.TrimSpace(k)
			v = strings.TrimSpace(v)

			if k == "" || v == "" {
				continue
			}

			cloned.Labels[k] = v
		}
	}
	return cloned
}

func normalizeAgentMemorySource(source AgentMemorySourceKind) (AgentMemorySourceKind, error) {
	source = AgentMemorySourceKind(strings.TrimSpace(string(source)))
	if source == "" {
		source = AgentMemorySourceNote
	}

	switch source {
	case AgentMemorySourceNote, AgentMemorySourceRun, AgentMemorySourceArtifact, AgentMemorySourceState:
		return source, nil
	default:
		return "", fmt.Errorf("%w: %q", ErrAgentMemoryUnsupportedSource, source)
	}
}

func normalizeAgentMemoryLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}

	out := make(map[string]string, len(labels))
	for k, v := range labels {
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)

		if k == "" || v == "" {
			continue
		}

		out[k] = v
	}

	if len(out) == 0 {
		return nil
	}
	return out
}

func (r *Runner) memoryEmbedder() MemoryEmbedder {
	return newHashedTextEmbedder(agentMemoryVectorDims)
}

func (r *Runner) getAgentMemoryEntry(tenantID, agentID, memoryID string) (AgentMemoryEntry, bool, error) {
	var entry AgentMemoryEntry
	ok, err := getJSONKV(r.store, agentMemoryEntryKey(tenantID, agentID, memoryID), &entry)

	if err != nil || !ok {
		return AgentMemoryEntry{}, ok, err
	}

	return cloneAgentMemoryEntry(entry), true, nil
}

func (r *Runner) latestReplayReadableMemoryBoundary(ctx context.Context) (time.Time, bool) {
	scope, ok := ReplayScopeFrom(ctx)

	if !ok {
		return time.Time{}, false
	}

	return scope.StartedAt.UTC(), true
}

func (r *Runner) ensureAgentMemoryAccess(ctx context.Context, tenantID, agentID, action string) error {
	if err := r.ensureAgentStateAccess(ctx, tenantID, agentID, action); err != nil {
		if errors.Is(err, ErrAgentStateAccessDenied) {
			return fmt.Errorf("%w: %s", ErrAgentMemoryAccessDenied, err.Error())
		}

		return err
	}
	return nil
}

func (r *Runner) resolveAgentMemoryContent(ctx context.Context, tenantID string, req AgentMemoryWriteRequest) (AgentMemoryEntry, error) {
	sourceKind, err := normalizeAgentMemorySource(req.SourceKind)
	if err != nil {
		return AgentMemoryEntry{}, err
	}

	entry := AgentMemoryEntry{
		TenantID:   tenantID,
		AgentID:    req.AgentID,
		SourceKind: sourceKind,
		Content:    strings.TrimSpace(req.Content),
		Labels:     normalizeAgentMemoryLabels(req.Labels),
	}

	switch sourceKind {
	case AgentMemorySourceNote:
		if entry.Content == "" {
			return AgentMemoryEntry{}, fmt.Errorf("%w: content is required for note memory", ErrAgentMemoryUnsupportedSource)
		}

	case AgentMemorySourceState:
		snapshot, err := r.ReadAgentState(ctx, req.AgentID, AgentStateReadOptions{Version: req.StateVersion})
		if err != nil {
			return AgentMemoryEntry{}, err
		}

		entry.StateVersion = snapshot.Version
		entry.RunID = snapshot.RunID
		entry.WorkflowID = snapshot.WorkflowID
		entry.NodeID = snapshot.NodeID
		entry.Attempt = snapshot.Attempt

		if entry.Content == "" {
			entry.Content = string(snapshot.State)
		}

	case AgentMemorySourceArtifact:
		artifactID := strings.TrimSpace(req.ArtifactID)
		if artifactID == "" {
			return AgentMemoryEntry{}, fmt.Errorf("%w: artifact_id is required", ErrAgentMemoryUnsupportedSource)
		}

		data, meta, err := r.GetArtifact(ctx, artifactID)
		if err != nil {
			return AgentMemoryEntry{}, err
		}

		entry.ArtifactID = meta.ArtifactID
		entry.RunID = meta.RunID
		entry.WorkflowID = meta.WorkflowID
		entry.NodeID = meta.NodeID
		entry.Attempt = meta.Attempt

		if entry.Content == "" {
			text, ok := semanticArtifactText(data, meta.ContentType)
			if !ok {
				return AgentMemoryEntry{}, fmt.Errorf("%w: artifact %s content type %q is not text-like", ErrAgentMemoryUnsupportedSource, artifactID, meta.ContentType)
			}

			entry.Content = text
		}

	case AgentMemorySourceRun:
		runID := strings.TrimSpace(req.RunID)
		if runID == "" {
			return AgentMemoryEntry{}, fmt.Errorf("%w: run_id is required", ErrAgentMemoryUnsupportedSource)
		}

		run, ok := r.store.GetRun(runID)
		if !ok {
			return AgentMemoryEntry{}, ErrRunNotFound
		}

		if err := r.ensureRunTenantAccess(ctx, run, "agent_memory.write"); err != nil {
			return AgentMemoryEntry{}, err
		}

		entry.RunID = run.RunID
		entry.WorkflowID = run.WorkflowID
		if entry.Content == "" {
			entry.Content = buildRunSemanticText(run, r.store.ListNodeExecutions(runID))
		}
	}

	entry.Content = strings.TrimSpace(entry.Content)
	if entry.Content == "" {
		return AgentMemoryEntry{}, fmt.Errorf("%w: resolved content is empty", ErrAgentMemoryUnsupportedSource)
	}

	return entry, nil
}

func semanticArtifactText(data []byte, contentType string) (string, bool) {
	contentType = strings.TrimSpace(contentType)
	if contentType == "" {
		return strings.TrimSpace(string(data)), true
	}

	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		mediaType = contentType
	}

	mediaType = strings.ToLower(strings.TrimSpace(mediaType))

	switch {
	case strings.HasPrefix(mediaType, "text/"):
		return strings.TrimSpace(string(data)), true
	case mediaType == "application/json",
		mediaType == "application/ld+json",
		mediaType == "application/xml",
		mediaType == "application/x-yaml",
		mediaType == "application/yaml",
		mediaType == "application/javascript",
		mediaType == "application/x-javascript":
		return strings.TrimSpace(string(data)), true
	default:
		return "", false
	}
}

func buildRunSemanticText(run Run, nodes []NodeExecution) string {
	var lines []string
	lines = append(lines,
		"run_id: "+strings.TrimSpace(run.RunID),
		"workflow_id: "+strings.TrimSpace(run.WorkflowID),
		"status: "+strings.TrimSpace(string(run.Status)),
	)

	if reason := strings.TrimSpace(run.TerminalReason); reason != "" {
		lines = append(lines, "terminal_reason: "+reason)
	}

	if len(run.InitialInput) > 0 {
		lines = append(lines, "initial_input: "+strings.TrimSpace(string(run.InitialInput)))
	}

	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].NodeID == nodes[j].NodeID {
			return nodes[i].Attempt < nodes[j].Attempt
		}
		return nodes[i].NodeID < nodes[j].NodeID
	})

	for _, node := range nodes {
		line := fmt.Sprintf("node %s attempt %d status %s", strings.TrimSpace(node.NodeID), node.Attempt, strings.TrimSpace(string(node.Status)))
		if errText := strings.TrimSpace(node.Error); errText != "" {
			line += " error " + errText
		}

		if len(node.Output) > 0 {
			line += " output " + strings.TrimSpace(string(node.Output))
		}

		lines = append(lines, line)
	}

	return strings.Join(lines, "\n")
}

func (r *Runner) WriteAgentMemory(ctx context.Context, req AgentMemoryWriteRequest) (AgentMemoryEntry, error) {
	tenantID := effectiveTenantFromContext(ctx)
	agentID, err := normalizeAgentStateID(req.AgentID)
	if err != nil {
		return AgentMemoryEntry{}, fmt.Errorf("%w: %s", ErrAgentMemoryAccessDenied, err.Error())
	}

	req.AgentID = agentID

	if strings.TrimSpace(tenantID) == "" {
		return AgentMemoryEntry{}, ErrTenantAccessDenied
	}

	if err := r.ensureAgentMemoryAccess(ctx, tenantID, req.AgentID, "agent_memory.write"); err != nil {
		return AgentMemoryEntry{}, err
	}

	if _, ok := ReplayScopeFrom(ctx); ok {
		return AgentMemoryEntry{}, ErrAgentMemoryReplayWriteDenied
	}

	ctx, span := r.startSpan(ctx, "driftq.agent_memory.write",
		attribute.String("driftq.tenant_id", strings.TrimSpace(tenantID)),
		attribute.String("driftq.agent_id", req.AgentID),
	)

	defer func() {
		r.finishSpan(span, err)
	}()

	entry, err := r.resolveAgentMemoryContent(ctx, tenantID, req)
	if err != nil {
		return AgentMemoryEntry{}, err
	}

	entry.ID = "memory-" + time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + NewTraceID()
	if _, err := normalizeMemoryID(entry.ID); err != nil {
		return AgentMemoryEntry{}, err
	}

	entry.CreatedAt = time.Now().UTC()
	entry.TraceID = TraceIDFrom(ctx)
	if principal, ok := PrincipalFrom(ctx); ok {
		entry.PrincipalID = strings.TrimSpace(principal.ID)
	}

	if ref, ok := ExecutionRefFrom(ctx); ok {
		if entry.RunID == "" {
			entry.RunID = ref.RunID
		}

		if entry.WorkflowID == "" {
			entry.WorkflowID = ref.WorkflowID
		}

		if entry.NodeID == "" {
			entry.NodeID = ref.NodeID
		}

		if entry.Attempt == 0 {
			entry.Attempt = ref.Attempt
		}
	}

	entry.Vector = r.memoryEmbedder().Embed(entry.Content)

	if err := putJSONKV(r.store, agentMemoryEntryKey(tenantID, req.AgentID, entry.ID), entry); err != nil {
		return AgentMemoryEntry{}, err
	}

	ids, err := indexIDs(r.store, agentMemoryIndexKey(tenantID, req.AgentID))
	if err != nil {
		return AgentMemoryEntry{}, err
	}

	ids = append(ids, entry.ID)
	if err := putJSONKV(r.store, agentMemoryIndexKey(tenantID, req.AgentID), ids); err != nil {
		return AgentMemoryEntry{}, err
	}

	return cloneAgentMemoryEntry(entry), nil
}

func (r *Runner) ListAgentMemory(ctx context.Context, agentID string, limit int) ([]AgentMemoryEntry, error) {
	tenantID := effectiveTenantFromContext(ctx)
	agentID, err := normalizeAgentStateID(agentID)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrAgentMemoryAccessDenied, err.Error())
	}

	if strings.TrimSpace(tenantID) == "" {
		return nil, ErrTenantAccessDenied
	}

	if err := r.ensureAgentMemoryAccess(ctx, tenantID, agentID, "agent_memory.list"); err != nil {
		return nil, err
	}

	if limit <= 0 {
		limit = 50
	}

	if limit > 200 {
		limit = 200
	}

	ctx, span := r.startSpan(ctx, "driftq.agent_memory.list",
		attribute.String("driftq.tenant_id", strings.TrimSpace(tenantID)),
		attribute.String("driftq.agent_id", agentID),
	)

	defer func() {
		r.finishSpan(span, err)
	}()

	ids, err := indexIDs(r.store, agentMemoryIndexKey(tenantID, agentID))
	if err != nil {
		return nil, err
	}

	boundary, replayPinned := r.latestReplayReadableMemoryBoundary(ctx)
	out := make([]AgentMemoryEntry, 0, min(limit, len(ids)))
	for i := len(ids) - 1; i >= 0; i-- {
		entry, ok, err := r.getAgentMemoryEntry(tenantID, agentID, ids[i])
		if err != nil || !ok {
			continue
		}

		if replayPinned && entry.CreatedAt.After(boundary) {
			continue
		}

		out = append(out, entry)
		if len(out) >= limit {
			break
		}
	}

	return out, nil
}

func (r *Runner) SearchAgentMemory(ctx context.Context, req AgentMemorySearchRequest) (results []AgentMemorySearchResult, err error) {
	tenantID := effectiveTenantFromContext(ctx)
	agentID, err := normalizeAgentStateID(req.AgentID)

	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrAgentMemoryAccessDenied, err.Error())
	}

	req.AgentID = agentID
	req.Query = strings.TrimSpace(req.Query)

	if req.Query == "" {
		return nil, fmt.Errorf("query is required")
	}

	if strings.TrimSpace(tenantID) == "" {
		return nil, ErrTenantAccessDenied
	}

	if err := r.ensureAgentMemoryAccess(ctx, tenantID, req.AgentID, "agent_memory.search"); err != nil {
		return nil, err
	}

	if req.Limit <= 0 {
		req.Limit = 5
	}

	if req.Limit > 50 {
		req.Limit = 50
	}

	ctx, span := r.startSpan(ctx, "driftq.agent_memory.search",
		attribute.String("driftq.tenant_id", strings.TrimSpace(tenantID)),
		attribute.String("driftq.agent_id", req.AgentID),
		attribute.Int("driftq.result_limit", req.Limit),
	)
	defer func() {
		r.finishSpan(span, err, attribute.Int("driftq.result_count", len(results)))
	}()

	ids, err := indexIDs(r.store, agentMemoryIndexKey(tenantID, req.AgentID))
	if err != nil {
		return nil, err
	}

	if len(ids) == 0 {
		return []AgentMemorySearchResult{}, nil
	}

	queryVector := r.memoryEmbedder().Embed(req.Query)
	replayBoundary, replayPinned := r.latestReplayReadableMemoryBoundary(ctx)
	if req.CreatedBefore.IsZero() && replayPinned {
		req.CreatedBefore = replayBoundary
	}

	results = make([]AgentMemorySearchResult, 0, min(req.Limit, len(ids)))
	for _, memoryID := range ids {
		entry, ok, err := r.getAgentMemoryEntry(tenantID, req.AgentID, memoryID)
		if err != nil || !ok {
			continue
		}

		if !req.CreatedBefore.IsZero() && entry.CreatedAt.After(req.CreatedBefore.UTC()) {
			continue
		}

		score := cosineSimilarity(queryVector, entry.Vector)
		if score < req.MinScore {
			continue
		}

		results = append(results, AgentMemorySearchResult{
			Score: score,
			Entry: entry,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].Score == results[j].Score {
			return results[i].Entry.CreatedAt.After(results[j].Entry.CreatedAt)
		}
		return results[i].Score > results[j].Score
	})

	if len(results) > req.Limit {
		results = results[:req.Limit]
	}

	return results, nil
}
