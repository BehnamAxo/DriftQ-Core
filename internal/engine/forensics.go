package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type ForensicExecutionNode struct {
	NodeID             string              `json:"node_id"`
	Topic              string              `json:"topic,omitempty"`
	Dependencies       []string            `json:"dependencies,omitempty"`
	Dependents         []string            `json:"dependents,omitempty"`
	LatestStatus       NodeStatus          `json:"latest_status,omitempty"`
	LatestAttempt      int                 `json:"latest_attempt,omitempty"`
	LatestError        string              `json:"latest_error,omitempty"`
	Executions         []NodeExecution     `json:"executions,omitempty"`
	ToolCalls          []ToolCallRecord    `json:"tool_calls,omitempty"`
	HumanTasks         []HumanTask         `json:"human_tasks,omitempty"`
	SideEffectReceipts []SideEffectReceipt `json:"side_effect_receipts,omitempty"`
}

type ForensicExecutionGraph struct {
	Run          Run                     `json:"run"`
	Nodes        []ForensicExecutionNode `json:"nodes"`
	Edges        []NodeEdge              `json:"edges,omitempty"`
	Events       []RunEvent              `json:"events,omitempty"`
	Timers       []Timer                 `json:"timers,omitempty"`
	SelfHealing  *SelfHealingArtifact    `json:"self_healing,omitempty"`
	HasSpecGraph bool                    `json:"has_spec_graph"`
}

type ForensicValueChange struct {
	Changed bool            `json:"changed"`
	From    json.RawMessage `json:"from,omitempty"`
	To      json.RawMessage `json:"to,omitempty"`
}

type ForensicTextChange struct {
	Changed bool   `json:"changed"`
	From    string `json:"from,omitempty"`
	To      string `json:"to,omitempty"`
}

type ForensicNodeDiff struct {
	NodeID        string              `json:"node_id"`
	FromAttempt   int                 `json:"from_attempt,omitempty"`
	ToAttempt     int                 `json:"to_attempt,omitempty"`
	FromStatus    NodeStatus          `json:"from_status,omitempty"`
	ToStatus      NodeStatus          `json:"to_status,omitempty"`
	StatusChanged bool                `json:"status_changed"`
	Error         ForensicTextChange  `json:"error"`
	Input         ForensicValueChange `json:"input"`
	Output        ForensicValueChange `json:"output"`
	Appeared      bool                `json:"appeared,omitempty"`
	Disappeared   bool                `json:"disappeared,omitempty"`
}

type ForensicRunDiff struct {
	FromRunID      string                    `json:"from_run_id"`
	ToRunID        string                    `json:"to_run_id"`
	WorkflowID     string                    `json:"workflow_id,omitempty"`
	Status         ForensicTextChange        `json:"status"`
	TerminalReason ForensicTextChange        `json:"terminal_reason"`
	WorkflowSpec   WorkflowReleaseDiffDetail `json:"workflow_spec"`
	NodeDiffs      []ForensicNodeDiff        `json:"node_diffs,omitempty"`
	ChangedNodes   []string                  `json:"changed_nodes,omitempty"`
	ChangedDomains []string                  `json:"changed_domains,omitempty"`
}

type ForensicRootCauseView struct {
	Run                Run                      `json:"run"`
	PrimaryFailureNode string                   `json:"primary_failure_node,omitempty"`
	FailureNodes       []SelfHealingFailureNode `json:"failure_nodes,omitempty"`
	RiskReport         *WorkflowRiskReport      `json:"risk_report,omitempty"`
	HumanTasks         []HumanTask              `json:"human_tasks,omitempty"`
	ToolCalls          []ToolCallRecord         `json:"tool_calls,omitempty"`
	SideEffects        []SideEffectReceipt      `json:"side_effects,omitempty"`
	SelfHealing        *SelfHealingArtifact     `json:"self_healing,omitempty"`
	Summary            []string                 `json:"summary,omitempty"`
}

type ForensicWhatChangedView struct {
	FromRunID string                 `json:"from_run_id"`
	ToRunID   string                 `json:"to_run_id"`
	RunDiff   ForensicRunDiff        `json:"run_diff"`
	RootCause *ForensicRootCauseView `json:"root_cause,omitempty"`
	Summary   []string               `json:"summary,omitempty"`
}

func parseWorkflowGraphFromSpec(spec json.RawMessage) (WorkflowGraph, bool) {
	if len(spec) == 0 {
		return WorkflowGraph{}, false
	}

	graph, _, err := ParseWorkflowSpecJSON(spec)
	if err != nil {
		return WorkflowGraph{}, false
	}

	return graph, true
}

func executionsByNode(runID string, nodes []NodeExecution) map[string][]NodeExecution {
	out := make(map[string][]NodeExecution)
	for _, node := range nodes {
		if strings.TrimSpace(node.RunID) != strings.TrimSpace(runID) {
			continue
		}
		out[node.NodeID] = append(out[node.NodeID], node)
	}

	for nodeID := range out {
		sort.Slice(out[nodeID], func(i, j int) bool {
			return out[nodeID][i].Attempt < out[nodeID][j].Attempt
		})
	}

	return out
}

func latestNodeExecution(nodes []NodeExecution) (NodeExecution, bool) {
	if len(nodes) == 0 {
		return NodeExecution{}, false
	}

	best := nodes[0]
	for _, node := range nodes[1:] {
		if node.Attempt > best.Attempt {
			best = node
		}
	}

	return best, true
}

func toolCallsByNode(records []ToolCallRecord) map[string][]ToolCallRecord {
	out := make(map[string][]ToolCallRecord)
	for _, rec := range records {
		out[rec.NodeID] = append(out[rec.NodeID], rec)
	}

	return out
}

func humanTasksByNode(tasks []HumanTask) map[string][]HumanTask {
	out := make(map[string][]HumanTask)
	for _, task := range tasks {
		out[task.NodeID] = append(out[task.NodeID], task)
	}

	return out
}

func sideEffectsByNode(receipts []SideEffectReceipt) map[string][]SideEffectReceipt {
	out := make(map[string][]SideEffectReceipt)
	for _, receipt := range receipts {
		out[receipt.NodeID] = append(out[receipt.NodeID], receipt)
	}

	return out
}

func forensicNodeIDs(graph WorkflowGraph, nodes map[string][]NodeExecution) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(graph.Nodes)+len(nodes))

	for _, node := range graph.Nodes {
		if _, ok := seen[node.NodeID]; ok {
			continue
		}
		seen[node.NodeID] = struct{}{}
		out = append(out, node.NodeID)
	}

	for nodeID := range nodes {
		if _, ok := seen[nodeID]; ok {
			continue
		}
		seen[nodeID] = struct{}{}
		out = append(out, nodeID)
	}

	sort.Strings(out)
	return out
}

func graphNodeMetadata(graph WorkflowGraph) (topics map[string]string, deps map[string][]string, children map[string][]string) {
	topics = make(map[string]string, len(graph.Nodes))
	deps = make(map[string][]string, len(graph.Nodes))
	children = make(map[string][]string, len(graph.Nodes))

	for _, node := range graph.Nodes {
		topics[node.NodeID] = strings.TrimSpace(node.Topic)
	}

	for _, edge := range graph.Edges {
		deps[edge.To] = append(deps[edge.To], edge.From)
		children[edge.From] = append(children[edge.From], edge.To)
	}

	for nodeID := range deps {
		sort.Strings(deps[nodeID])
	}

	for nodeID := range children {
		sort.Strings(children[nodeID])
	}

	return
}

func (r *Runner) BuildExecutionGraph(ctx context.Context, runID string) (ForensicExecutionGraph, error) {
	run, ok := r.store.GetRun(strings.TrimSpace(runID))
	if !ok {
		return ForensicExecutionGraph{}, ErrRunNotFound
	}

	if err := r.ensureRunTenantAccess(ctx, run, "forensics.execution_graph"); err != nil {
		return ForensicExecutionGraph{}, err
	}

	graph, hasGraph := parseWorkflowGraphFromSpec(run.Spec)
	nodes := r.store.ListNodeExecutions(run.RunID)
	execsByNode := executionsByNode(run.RunID, nodes)

	toolCalls, _ := r.ListToolCallRecords(ctx, run.RunID, "", 500)
	humanTasks, _ := r.ListHumanTasks(run.RunID, "", 500)
	sideEffects, _ := r.ListSideEffectReceipts(ctx, run.RunID, "", 500)
	selfHeal, ok, _ := r.GetSelfHealingArtifactByRun(ctx, run.RunID)

	toolByNode := toolCallsByNode(toolCalls)
	humanByNode := humanTasksByNode(humanTasks)
	receiptsByNode := sideEffectsByNode(sideEffects)
	topics, deps, children := graphNodeMetadata(graph)

	orderedNodeIDs := forensicNodeIDs(graph, execsByNode)
	forensicNodes := make([]ForensicExecutionNode, 0, len(orderedNodeIDs))

	for _, nodeID := range orderedNodeIDs {
		nodeExecutions := append([]NodeExecution(nil), execsByNode[nodeID]...)
		latest, _ := latestNodeExecution(nodeExecutions)
		forensicNodes = append(forensicNodes, ForensicExecutionNode{
			NodeID:             nodeID,
			Topic:              topics[nodeID],
			Dependencies:       append([]string(nil), deps[nodeID]...),
			Dependents:         append([]string(nil), children[nodeID]...),
			LatestStatus:       latest.Status,
			LatestAttempt:      latest.Attempt,
			LatestError:        strings.TrimSpace(latest.Error),
			Executions:         nodeExecutions,
			ToolCalls:          append([]ToolCallRecord(nil), toolByNode[nodeID]...),
			HumanTasks:         append([]HumanTask(nil), humanByNode[nodeID]...),
			SideEffectReceipts: append([]SideEffectReceipt(nil), receiptsByNode[nodeID]...),
		})
	}

	out := ForensicExecutionGraph{
		Run:          run,
		Nodes:        forensicNodes,
		Edges:        append([]NodeEdge(nil), graph.Edges...),
		Events:       r.store.ListEvents(run.RunID),
		Timers:       r.store.ListTimers(run.RunID),
		HasSpecGraph: hasGraph,
	}

	if ok {
		clone := cloneSelfHealingArtifact(selfHeal)
		out.SelfHealing = &clone
	}
	return out, nil
}

func parseRiskReportFromEvents(events []RunEvent) *WorkflowRiskReport {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Type != EventRiskAssessed || len(events[i].Payload) == 0 {
			continue
		}

		var report WorkflowRiskReport
		if err := json.Unmarshal(events[i].Payload, &report); err == nil {
			return &report
		}
	}
	return nil
}

func (r *Runner) BuildRootCauseView(ctx context.Context, runID string) (ForensicRootCauseView, error) {
	graph, err := r.BuildExecutionGraph(ctx, runID)
	if err != nil {
		return ForensicRootCauseView{}, err
	}

	view := ForensicRootCauseView{
		Run:        graph.Run,
		RiskReport: parseRiskReportFromEvents(graph.Events),
	}

	if graph.SelfHealing != nil {
		clone := cloneSelfHealingArtifact(*graph.SelfHealing)
		view.SelfHealing = &clone
		view.FailureNodes = append([]SelfHealingFailureNode(nil), clone.FailureNodes...)
		view.PrimaryFailureNode = clone.FailureNodeID
		view.Summary = append(view.Summary, "self-heal artifact captured for failed run")
	}

	for _, node := range graph.Nodes {
		if node.LatestStatus == NodeStatusFailed {
			if view.PrimaryFailureNode == "" {
				view.PrimaryFailureNode = node.NodeID
			}

			view.ToolCalls = append(view.ToolCalls, node.ToolCalls...)
			view.HumanTasks = append(view.HumanTasks, node.HumanTasks...)
			view.SideEffects = append(view.SideEffects, node.SideEffectReceipts...)
		}
	}

	if strings.TrimSpace(view.Run.TerminalReason) != "" {
		view.Summary = append(view.Summary, "terminal reason: "+strings.TrimSpace(view.Run.TerminalReason))
	}

	if view.PrimaryFailureNode != "" {
		view.Summary = append(view.Summary, "primary failed node: "+view.PrimaryFailureNode)
	}

	if view.RiskReport != nil && view.RiskReport.Action != "" {
		view.Summary = append(view.Summary, "risk action before execution: "+string(view.RiskReport.Action))
	}

	if len(view.ToolCalls) > 0 {
		view.Summary = append(view.Summary, fmt.Sprintf("captured %d tool call records around failure", len(view.ToolCalls)))
	}

	if len(view.HumanTasks) > 0 {
		view.Summary = append(view.Summary, fmt.Sprintf("captured %d human task records around failure", len(view.HumanTasks)))
	}

	return view, nil
}

func runSpecDiff(fromRun, toRun Run) WorkflowReleaseDiffDetail {
	return releaseDiffDetail(fromRun.Spec, toRun.Spec)
}

func nodeDiffText(from, to string) ForensicTextChange {
	return ForensicTextChange{
		Changed: strings.TrimSpace(from) != strings.TrimSpace(to),
		From:    strings.TrimSpace(from),
		To:      strings.TrimSpace(to),
	}
}

func nodeDiffValue(from, to json.RawMessage) ForensicValueChange {
	changed := false
	if len(from) == 0 && len(to) == 0 {
		changed = false
	} else {
		changed = !jsonEqual(from, to)
	}

	return ForensicValueChange{
		Changed: changed,
		From:    cloneRaw(from),
		To:      cloneRaw(to),
	}
}

func (r *Runner) DiffRuns(ctx context.Context, fromRunID, toRunID string) (ForensicRunDiff, error) {
	fromRun, ok := r.store.GetRun(strings.TrimSpace(fromRunID))
	if !ok {
		return ForensicRunDiff{}, ErrRunNotFound
	}

	if err := r.ensureRunTenantAccess(ctx, fromRun, "forensics.run_diff"); err != nil {
		return ForensicRunDiff{}, err
	}

	toRun, ok := r.store.GetRun(strings.TrimSpace(toRunID))
	if !ok {
		return ForensicRunDiff{}, ErrRunNotFound
	}

	if err := r.ensureRunTenantAccess(ctx, toRun, "forensics.run_diff"); err != nil {
		return ForensicRunDiff{}, err
	}

	fromNodes := latestNodeExecutionByNode(r.store.ListNodeExecutions(fromRun.RunID))
	toNodes := latestNodeExecutionByNode(r.store.ListNodeExecutions(toRun.RunID))

	nodeIDs := make([]string, 0, len(fromNodes)+len(toNodes))
	seen := map[string]struct{}{}
	for nodeID := range fromNodes {
		seen[nodeID] = struct{}{}
		nodeIDs = append(nodeIDs, nodeID)
	}

	for nodeID := range toNodes {
		if _, ok := seen[nodeID]; ok {
			continue
		}
		nodeIDs = append(nodeIDs, nodeID)
	}

	sort.Strings(nodeIDs)

	diff := ForensicRunDiff{
		FromRunID:      fromRun.RunID,
		ToRunID:        toRun.RunID,
		WorkflowID:     firstNonEmpty(toRun.WorkflowID, fromRun.WorkflowID),
		Status:         nodeDiffText(string(fromRun.Status), string(toRun.Status)),
		TerminalReason: nodeDiffText(fromRun.TerminalReason, toRun.TerminalReason),
		WorkflowSpec:   runSpecDiff(fromRun, toRun),
	}

	if diff.WorkflowSpec.Changed {
		diff.ChangedDomains = append(diff.ChangedDomains, "workflow_spec")
	}

	for _, nodeID := range nodeIDs {
		fromNode, hasFrom := fromNodes[nodeID]
		toNode, hasTo := toNodes[nodeID]
		nodeDiff := ForensicNodeDiff{
			NodeID:      nodeID,
			Appeared:    !hasFrom && hasTo,
			Disappeared: hasFrom && !hasTo,
		}

		if hasFrom {
			nodeDiff.FromAttempt = fromNode.Attempt
			nodeDiff.FromStatus = fromNode.Status
		}

		if hasTo {
			nodeDiff.ToAttempt = toNode.Attempt
			nodeDiff.ToStatus = toNode.Status
		}

		nodeDiff.StatusChanged = nodeDiff.FromStatus != nodeDiff.ToStatus || nodeDiff.Appeared || nodeDiff.Disappeared
		nodeDiff.Error = nodeDiffText(fromNode.Error, toNode.Error)
		nodeDiff.Input = nodeDiffValue(fromNode.Input, toNode.Input)
		nodeDiff.Output = nodeDiffValue(fromNode.Output, toNode.Output)

		if nodeDiff.StatusChanged || nodeDiff.Error.Changed || nodeDiff.Input.Changed || nodeDiff.Output.Changed {
			diff.NodeDiffs = append(diff.NodeDiffs, nodeDiff)
			diff.ChangedNodes = append(diff.ChangedNodes, nodeID)
		}
	}

	if diff.Status.Changed {
		diff.ChangedDomains = append(diff.ChangedDomains, "run_status")
	}

	if diff.TerminalReason.Changed {
		diff.ChangedDomains = append(diff.ChangedDomains, "terminal_reason")
	}

	if len(diff.ChangedNodes) > 0 {
		diff.ChangedDomains = append(diff.ChangedDomains, "node_executions")
	}

	return diff, nil
}

func (r *Runner) BuildWhatChangedView(ctx context.Context, fromRunID, toRunID string) (ForensicWhatChangedView, error) {
	diff, err := r.DiffRuns(ctx, fromRunID, toRunID)
	if err != nil {
		return ForensicWhatChangedView{}, err
	}

	view := ForensicWhatChangedView{
		FromRunID: fromRunID,
		ToRunID:   toRunID,
		RunDiff:   diff,
	}

	if toRun, ok := r.store.GetRun(strings.TrimSpace(toRunID)); ok && toRun.Status == RunStatusFailed {
		rootCause, err := r.BuildRootCauseView(ctx, toRun.RunID)
		if err == nil {
			view.RootCause = &rootCause
		}
	}

	if diff.WorkflowSpec.Changed {
		view.Summary = append(view.Summary, "workflow spec changed between runs")
	}

	if diff.Status.Changed {
		view.Summary = append(view.Summary, fmt.Sprintf("run status changed: %s -> %s", diff.Status.From, diff.Status.To))
	}

	if diff.TerminalReason.Changed {
		view.Summary = append(view.Summary, fmt.Sprintf("terminal reason changed: %s -> %s", diff.TerminalReason.From, diff.TerminalReason.To))
	}

	if len(diff.ChangedNodes) > 0 {
		view.Summary = append(view.Summary, "changed nodes: "+strings.Join(diff.ChangedNodes, ", "))
	}

	return view, nil
}
