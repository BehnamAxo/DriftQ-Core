package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const brainPolicyKey = "brain/policy"

type BrainWeights struct {
	SuccessRate      float64 `json:"success_rate,omitempty"`
	WorkflowAffinity float64 `json:"workflow_affinity,omitempty"`
	Latency          float64 `json:"latency,omitempty"`
	Cost             float64 `json:"cost,omitempty"`
	Priority         float64 `json:"priority,omitempty"`
	Escalation       float64 `json:"escalation,omitempty"`
}

type BrainPolicy struct {
	Enabled            bool         `json:"enabled"`
	LookbackRecords    int          `json:"lookback_records,omitempty"`
	MinSamples         int          `json:"min_samples,omitempty"`
	DefaultSuccessRate float64      `json:"default_success_rate,omitempty"`
	DefaultLatencyMS   int64        `json:"default_latency_ms,omitempty"`
	Weights            BrainWeights `json:"weights,omitempty"`
}

type BrainRouteMetrics struct {
	Samples      int     `json:"samples"`
	Successes    int     `json:"successes"`
	SuccessRate  float64 `json:"success_rate"`
	AvgLatencyMS int64   `json:"avg_latency_ms,omitempty"`
}

type BrainSignals struct {
	Uncertainty float64    `json:"uncertainty,omitempty"`
	Attempt     int        `json:"attempt,omitempty"`
	RiskScore   int        `json:"risk_score,omitempty"`
	RiskAction  RiskAction `json:"risk_action,omitempty"`
	Triggers    []string   `json:"triggers,omitempty"`
}

type BrainRouteScore struct {
	Route           AdaptiveRoute     `json:"route"`
	Global          BrainRouteMetrics `json:"global"`
	Workflow        BrainRouteMetrics `json:"workflow"`
	SuccessScore    float64           `json:"success_score"`
	WorkflowScore   float64           `json:"workflow_score"`
	LatencyScore    float64           `json:"latency_score"`
	CostScore       float64           `json:"cost_score"`
	PriorityScore   float64           `json:"priority_score"`
	EscalationScore float64           `json:"escalation_score"`
	FinalScore      float64           `json:"final_score"`
	Reasons         []string          `json:"reasons,omitempty"`
}

type BrainDecision struct {
	Tool          string            `json:"tool"`
	Selected      AdaptiveRoute     `json:"selected"`
	SelectedScore float64           `json:"selected_score,omitempty"`
	Policy        BrainPolicy       `json:"policy"`
	Signals       BrainSignals      `json:"signals"`
	Candidates    []BrainRouteScore `json:"candidates,omitempty"`
	Summary       []string          `json:"summary,omitempty"`
}

type brainRouteAggregate struct {
	samples      int
	successes    int
	totalLatency int64
}

func defaultBrainPolicy() BrainPolicy {
	return BrainPolicy{
		Enabled:            false,
		LookbackRecords:    250,
		MinSamples:         3,
		DefaultSuccessRate: 0.75,
		DefaultLatencyMS:   1200,
		Weights: BrainWeights{
			SuccessRate:      0.45,
			WorkflowAffinity: 0.20,
			Latency:          0.10,
			Cost:             0.10,
			Priority:         0.05,
			Escalation:       0.10,
		},
	}
}

func normalizeBrainPolicy(policy BrainPolicy) (BrainPolicy, error) {
	if policy.LookbackRecords <= 0 {
		policy.LookbackRecords = defaultBrainPolicy().LookbackRecords
	}

	if policy.MinSamples <= 0 {
		policy.MinSamples = defaultBrainPolicy().MinSamples
	}

	if policy.DefaultSuccessRate < 0 {
		policy.DefaultSuccessRate = 0
	}

	if policy.DefaultSuccessRate > 1 {
		policy.DefaultSuccessRate = 1
	}

	if policy.DefaultSuccessRate == 0 {
		policy.DefaultSuccessRate = defaultBrainPolicy().DefaultSuccessRate
	}

	if policy.DefaultLatencyMS <= 0 {
		policy.DefaultLatencyMS = defaultBrainPolicy().DefaultLatencyMS
	}

	def := defaultBrainPolicy().Weights
	if policy.Weights.SuccessRate == 0 {
		policy.Weights.SuccessRate = def.SuccessRate
	}

	if policy.Weights.WorkflowAffinity == 0 {
		policy.Weights.WorkflowAffinity = def.WorkflowAffinity
	}

	if policy.Weights.Latency == 0 {
		policy.Weights.Latency = def.Latency
	}

	if policy.Weights.Cost == 0 {
		policy.Weights.Cost = def.Cost
	}

	if policy.Weights.Priority == 0 {
		policy.Weights.Priority = def.Priority
	}

	if policy.Weights.Escalation == 0 {
		policy.Weights.Escalation = def.Escalation
	}

	return policy, nil
}

func (r *Runner) SaveBrainPolicy(policy BrainPolicy) error {
	normalized, err := normalizeBrainPolicy(policy)
	if err != nil {
		return err
	}

	if err := putJSONKV(r.store, brainPolicyKey, normalized); err != nil {
		return err
	}

	r.brainMu.Lock()
	r.brainPolicy = &normalized
	r.brainMu.Unlock()

	return nil
}

func (r *Runner) GetBrainPolicy() (BrainPolicy, bool, error) {
	r.brainMu.RLock()
	if r.brainPolicy != nil {
		out := *r.brainPolicy
		r.brainMu.RUnlock()
		return out, true, nil
	}
	r.brainMu.RUnlock()

	var policy BrainPolicy
	ok, err := getJSONKV(r.store, brainPolicyKey, &policy)
	if err != nil || !ok {
		return BrainPolicy{}, ok, err
	}

	policy, err = normalizeBrainPolicy(policy)
	if err != nil {
		return BrainPolicy{}, true, err
	}

	r.brainMu.Lock()
	r.brainPolicy = &policy
	r.brainMu.Unlock()

	return policy, true, nil
}

func routeHistoryKey(route AdaptiveRoute) string {
	if id := strings.TrimSpace(route.ID); id != "" {
		return id
	}

	return strings.TrimSpace(route.Provider) + "|" + strings.TrimSpace(route.Model)
}

func recordHistoryKey(record ToolCallRecord) string {
	if id := strings.TrimSpace(record.RouteID); id != "" {
		return id
	}

	if strings.TrimSpace(record.Provider) == "" && strings.TrimSpace(record.Model) == "" {
		return ""
	}
	return strings.TrimSpace(record.Provider) + "|" + strings.TrimSpace(record.Model)
}

func (a brainRouteAggregate) metrics(policy BrainPolicy) BrainRouteMetrics {
	successRate := policy.DefaultSuccessRate
	avgLatency := policy.DefaultLatencyMS

	if a.samples > 0 {
		successRate = float64(a.successes) / float64(a.samples)
		if a.totalLatency > 0 {
			avgLatency = a.totalLatency / int64(a.samples)
		}
	}

	return BrainRouteMetrics{
		Samples:      a.samples,
		Successes:    a.successes,
		SuccessRate:  successRate,
		AvgLatencyMS: avgLatency,
	}
}

func scoreNormalizedAscending(value, minValue, maxValue float64) float64 {
	if maxValue <= minValue {
		return 1
	}

	norm := (value - minValue) / (maxValue - minValue)
	if norm < 0 {
		norm = 0
	}

	if norm > 1 {
		norm = 1
	}

	return 1 - norm
}

func candidateCostValue(route AdaptiveRoute) float64 {
	if route.EstimatedDollars > 0 {
		return route.EstimatedDollars
	}

	if route.EstimatedTokens > 0 {
		return float64(route.EstimatedTokens) / 1000.0
	}

	return 0
}

func candidatePriorityValue(route AdaptiveRoute) float64 {
	return float64(route.Priority)
}

func (r *Runner) brainRouteAggregates(ctx context.Context, tool, workflowID string, lookback int) (map[string]brainRouteAggregate, map[string]brainRouteAggregate, error) {
	records, err := r.ListToolCallRecords(ctx, "", tool, lookback)
	if err != nil {
		return nil, nil, err
	}

	global := make(map[string]brainRouteAggregate)
	workflow := make(map[string]brainRouteAggregate)
	workflowID = strings.TrimSpace(workflowID)

	for _, record := range records {
		key := recordHistoryKey(record)
		if key == "" {
			continue
		}

		agg := global[key]
		agg.samples++
		if record.Allowed && strings.TrimSpace(record.Error) == "" {
			agg.successes++
		}

		if record.DurationMS > 0 {
			agg.totalLatency += record.DurationMS
		}

		global[key] = agg

		if workflowID != "" && strings.TrimSpace(record.WorkflowID) == workflowID {
			workflowAgg := workflow[key]
			workflowAgg.samples++

			if record.Allowed && strings.TrimSpace(record.Error) == "" {
				workflowAgg.successes++
			}

			if record.DurationMS > 0 {
				workflowAgg.totalLatency += record.DurationMS
			}

			workflow[key] = workflowAgg
		}
	}

	return global, workflow, nil
}

func buildBrainSignals(ctx context.Context, attempt int, hints adaptiveRoutingHints) BrainSignals {
	signals := BrainSignals{
		Uncertainty: hints.Uncertainty,
		Attempt:     attempt,
	}

	if decision, ok := RiskDecisionFrom(ctx); ok {
		signals.RiskScore = decision.Score
		signals.RiskAction = decision.Action
	}

	return signals
}

func (r *Runner) evaluateBrainDecision(ctx context.Context, inv toolInvocation, policy ToolPolicy, candidates []AdaptiveRoute, input json.RawMessage, triggers []string) (BrainDecision, bool, error) {
	brainPolicy, ok, err := r.GetBrainPolicy()
	if err != nil {
		return BrainDecision{}, false, err
	}

	if !ok {
		brainPolicy = defaultBrainPolicy()
	}

	if !brainPolicy.Enabled || len(candidates) == 0 {
		return BrainDecision{}, false, nil
	}

	hints := routingHintsFromInput(input)
	signals := buildBrainSignals(ctx, inv.Attempt, hints)
	signals.Triggers = append([]string(nil), triggers...)
	globalAgg, workflowAgg, err := r.brainRouteAggregates(ctx, inv.Tool, inv.WorkflowID, brainPolicy.LookbackRecords)

	if err != nil {
		return BrainDecision{}, false, err
	}

	minLatency, maxLatency := 0.0, 0.0
	minCost, maxCost := 0.0, 0.0
	minPriority, maxPriority := 0.0, 0.0
	costs := make([]float64, 0, len(candidates))
	latencies := make([]float64, 0, len(candidates))
	priorities := make([]float64, 0, len(candidates))

	for _, route := range candidates {
		key := routeHistoryKey(route)
		globalMetrics := globalAgg[key].metrics(brainPolicy)
		latencies = append(latencies, float64(globalMetrics.AvgLatencyMS))
		costs = append(costs, candidateCostValue(route))
		priorities = append(priorities, candidatePriorityValue(route))
	}

	if len(latencies) > 0 {
		minLatency, maxLatency = latencies[0], latencies[0]
		minCost, maxCost = costs[0], costs[0]
		minPriority, maxPriority = priorities[0], priorities[0]
		for i := 1; i < len(latencies); i++ {
			if latencies[i] < minLatency {
				minLatency = latencies[i]
			}

			if latencies[i] > maxLatency {
				maxLatency = latencies[i]
			}

			if costs[i] < minCost {
				minCost = costs[i]
			}

			if costs[i] > maxCost {
				maxCost = costs[i]
			}

			if priorities[i] < minPriority {
				minPriority = priorities[i]
			}

			if priorities[i] > maxPriority {
				maxPriority = priorities[i]
			}
		}
	}

	decision := BrainDecision{
		Tool:    strings.TrimSpace(inv.Tool),
		Policy:  brainPolicy,
		Signals: signals,
	}

	for _, route := range candidates {
		key := routeHistoryKey(route)
		globalMetrics := globalAgg[key].metrics(brainPolicy)
		workflowMetrics := workflowAgg[key].metrics(brainPolicy)

		successScore := globalMetrics.SuccessRate * brainPolicy.Weights.SuccessRate
		workflowScore := 0.0

		if workflowAgg[key].samples >= brainPolicy.MinSamples {
			workflowScore = workflowMetrics.SuccessRate * brainPolicy.Weights.WorkflowAffinity
		}

		latencyScore := scoreNormalizedAscending(float64(globalMetrics.AvgLatencyMS), minLatency, maxLatency) * brainPolicy.Weights.Latency
		costScore := scoreNormalizedAscending(candidateCostValue(route), minCost, maxCost) * brainPolicy.Weights.Cost
		priorityScore := scoreNormalizedAscending(minPriority+(maxPriority-candidatePriorityValue(route)), minPriority, maxPriority) * brainPolicy.Weights.Priority
		escalationScore := scoreNormalizedAscending(minPriority+(maxPriority-candidatePriorityValue(route)), minPriority, maxPriority) * brainPolicy.Weights.Escalation * float64(len(triggers))
		finalScore := successScore + workflowScore + latencyScore + costScore + priorityScore + escalationScore

		reasons := []string{
			fmt.Sprintf("historical success %.2f across %d samples", globalMetrics.SuccessRate, globalMetrics.Samples),
			fmt.Sprintf("average latency %dms", globalMetrics.AvgLatencyMS),
		}

		if workflowAgg[key].samples >= brainPolicy.MinSamples {
			reasons = append(reasons, fmt.Sprintf("workflow-specific success %.2f across %d samples", workflowMetrics.SuccessRate, workflowMetrics.Samples))
		}

		if len(triggers) > 0 {
			reasons = append(reasons, "escalation triggers: "+strings.Join(triggers, ", "))
		}

		decision.Candidates = append(decision.Candidates, BrainRouteScore{
			Route:           route,
			Global:          globalMetrics,
			Workflow:        workflowMetrics,
			SuccessScore:    successScore,
			WorkflowScore:   workflowScore,
			LatencyScore:    latencyScore,
			CostScore:       costScore,
			PriorityScore:   priorityScore,
			EscalationScore: escalationScore,
			FinalScore:      finalScore,
			Reasons:         reasons,
		})
	}

	sort.Slice(decision.Candidates, func(i, j int) bool {
		if decision.Candidates[i].FinalScore != decision.Candidates[j].FinalScore {
			return decision.Candidates[i].FinalScore > decision.Candidates[j].FinalScore
		}

		if decision.Candidates[i].Route.EstimatedDollars != decision.Candidates[j].Route.EstimatedDollars {
			return decision.Candidates[i].Route.EstimatedDollars < decision.Candidates[j].Route.EstimatedDollars
		}

		return decision.Candidates[i].Route.ID < decision.Candidates[j].Route.ID
	})

	if len(decision.Candidates) > 0 {
		decision.Selected = decision.Candidates[0].Route
		decision.SelectedScore = decision.Candidates[0].FinalScore
		decision.Summary = append(decision.Summary,
			"brain selected route "+strings.TrimSpace(decision.Selected.ID),
			fmt.Sprintf("top score %.3f", decision.SelectedScore),
		)
	}

	return decision, true, nil
}

type BrainRouteRequest struct {
	RunID              string          `json:"run_id,omitempty"`
	WorkflowID         string          `json:"workflow_id,omitempty"`
	NodeID             string          `json:"node_id,omitempty"`
	Attempt            int             `json:"attempt,omitempty"`
	Tool               string          `json:"tool"`
	RequiredCapability string          `json:"required_capability,omitempty"`
	Input              json.RawMessage `json:"input,omitempty"`
}

func (r *Runner) ExplainBrainRoute(ctx context.Context, req BrainRouteRequest) (BrainDecision, error) {
	req.RunID = strings.TrimSpace(req.RunID)
	req.WorkflowID = strings.TrimSpace(req.WorkflowID)
	req.NodeID = strings.TrimSpace(req.NodeID)
	req.Tool = strings.TrimSpace(req.Tool)
	req.RequiredCapability = strings.TrimSpace(req.RequiredCapability)

	if req.Tool == "" {
		return BrainDecision{}, fmt.Errorf("tool is required")
	}

	if req.Attempt <= 0 {
		req.Attempt = 1
	}

	if req.RunID != "" {
		if run, ok := r.store.GetRun(req.RunID); ok {
			if err := r.ensureRunTenantAccess(ctx, run, "brain.route_explain"); err != nil {
				return BrainDecision{}, err
			}
			if req.WorkflowID == "" {
				req.WorkflowID = run.WorkflowID
			}
		}
	}

	policy, err := r.resolveToolPolicy(ctx, req.Tool)
	if err != nil {
		return BrainDecision{}, err
	}

	adaptive := policy.AdaptiveRouting
	if adaptive == nil || len(adaptive.Routes) == 0 {
		return BrainDecision{}, fmt.Errorf("tool %q has no adaptive routes", req.Tool)
	}

	candidates, triggers, err := r.adaptiveRouteCandidates(ctx, toolInvocation{
		RunID:              req.RunID,
		WorkflowID:         req.WorkflowID,
		NodeID:             req.NodeID,
		Attempt:            req.Attempt,
		Tool:               req.Tool,
		RequiredCapability: req.RequiredCapability,
	}, policy, req.Input)

	if err != nil {
		return BrainDecision{}, err
	}

	decision, used, err := r.evaluateBrainDecision(ctx, toolInvocation{
		RunID:              req.RunID,
		WorkflowID:         req.WorkflowID,
		NodeID:             req.NodeID,
		Attempt:            req.Attempt,
		Tool:               req.Tool,
		RequiredCapability: req.RequiredCapability,
	}, policy, candidates, req.Input, triggers)

	if err != nil {
		return BrainDecision{}, err
	}

	if used {
		return decision, nil
	}

	selected, err := r.legacyAdaptiveRouteSelection(policy, candidates, req.Input, triggers)
	if err != nil {
		return BrainDecision{}, err
	}

	return BrainDecision{
		Tool:     req.Tool,
		Selected: selected,
		Policy:   defaultBrainPolicy(),
		Summary:  []string{"brain disabled; using legacy adaptive route selection"},
	}, nil
}
