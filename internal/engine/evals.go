package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type EvalEvaluator string

const (
	EvalEvaluatorRunSucceeded    EvalEvaluator = "run_succeeded"
	EvalEvaluatorNodeOutputExact EvalEvaluator = "node_output_exact"
)

const (
	evalDatasetIndexKey = "evals/datasets/index"
	evalSuiteIndexKey   = "evals/suites/index"
	evalReportIndexKey  = "evals/reports/index"
)

type EvalDataset struct {
	ID          string     `json:"id"`
	Name        string     `json:"name,omitempty"`
	Description string     `json:"description,omitempty"`
	Cases       []EvalCase `json:"cases"`
	CreatedAt   *time.Time `json:"created_at,omitempty"`
	UpdatedAt   *time.Time `json:"updated_at,omitempty"`
}

type EvalCase struct {
	ID             string            `json:"id"`
	Name           string            `json:"name,omitempty"`
	SourceRunID    string            `json:"source_run_id,omitempty"`
	Spec           json.RawMessage   `json:"spec,omitempty"`
	Input          json.RawMessage   `json:"input,omitempty"`
	TargetNodeID   string            `json:"target_node_id,omitempty"`
	ExpectedOutput json.RawMessage   `json:"expected_output,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
}

type EvalSuite struct {
	ID            string        `json:"id"`
	Name          string        `json:"name,omitempty"`
	Description   string        `json:"description,omitempty"`
	DatasetID     string        `json:"dataset_id"`
	Evaluator     EvalEvaluator `json:"evaluator"`
	TargetNodeID  string        `json:"target_node_id,omitempty"`
	PassThreshold float64       `json:"pass_threshold,omitempty"`
	CreatedAt     *time.Time    `json:"created_at,omitempty"`
	UpdatedAt     *time.Time    `json:"updated_at,omitempty"`
}

type EvalRunRequest struct {
	EvalRunID    string          `json:"eval_run_id,omitempty"`
	SuiteID      string          `json:"suite_id"`
	SpecOverride json.RawMessage `json:"spec_override,omitempty"`
}

type EvalReport struct {
	ID            string           `json:"id"`
	SuiteID       string           `json:"suite_id"`
	DatasetID     string           `json:"dataset_id"`
	Status        string           `json:"status"`
	Evaluator     EvalEvaluator    `json:"evaluator"`
	TargetNodeID  string           `json:"target_node_id,omitempty"`
	PassThreshold float64          `json:"pass_threshold"`
	CaseCount     int              `json:"case_count"`
	PassedCount   int              `json:"passed_count"`
	PassRate      float64          `json:"pass_rate"`
	Passed        bool             `json:"passed"`
	CreatedAt     *time.Time       `json:"created_at,omitempty"`
	CompletedAt   *time.Time       `json:"completed_at,omitempty"`
	Results       []EvalCaseResult `json:"results,omitempty"`
}

type EvalCaseResult struct {
	CaseID         string          `json:"case_id"`
	Name           string          `json:"name,omitempty"`
	SourceRunID    string          `json:"source_run_id,omitempty"`
	RunID          string          `json:"run_id,omitempty"`
	RunStatus      RunStatus       `json:"run_status,omitempty"`
	Passed         bool            `json:"passed"`
	Score          float64         `json:"score"`
	Reason         string          `json:"reason,omitempty"`
	TargetNodeID   string          `json:"target_node_id,omitempty"`
	ExpectedOutput json.RawMessage `json:"expected_output,omitempty"`
	ActualOutput   json.RawMessage `json:"actual_output,omitempty"`
}

func AvailableEvalEvaluators() []EvalEvaluator {
	return []EvalEvaluator{
		EvalEvaluatorRunSucceeded,
		EvalEvaluatorNodeOutputExact,
	}
}

func (d *EvalDataset) NormalizeAndValidate() error {
	if d == nil {
		return fmt.Errorf("eval dataset is nil")
	}

	d.ID = strings.TrimSpace(d.ID)
	d.Name = strings.TrimSpace(d.Name)
	d.Description = strings.TrimSpace(d.Description)
	if d.ID == "" {
		return fmt.Errorf("eval dataset id is required")
	}

	seen := make(map[string]struct{}, len(d.Cases))
	for i := range d.Cases {
		c := &d.Cases[i]
		c.ID = strings.TrimSpace(c.ID)
		c.Name = strings.TrimSpace(c.Name)
		c.SourceRunID = strings.TrimSpace(c.SourceRunID)
		c.TargetNodeID = strings.TrimSpace(c.TargetNodeID)
		if c.ID == "" {
			return fmt.Errorf("eval case id is required")
		}

		if _, ok := seen[c.ID]; ok {
			return fmt.Errorf("duplicate eval case id %q", c.ID)
		}

		seen[c.ID] = struct{}{}
		if err := validateOptionalJSON("eval case spec", c.Spec); err != nil {
			return err
		}

		if err := validateOptionalJSON("eval case input", c.Input); err != nil {
			return err
		}

		if err := validateOptionalJSON("eval case expected_output", c.ExpectedOutput); err != nil {
			return err
		}
	}

	return nil
}

func (s *EvalSuite) NormalizeAndValidate() error {
	if s == nil {
		return fmt.Errorf("eval suite is nil")
	}

	s.ID = strings.TrimSpace(s.ID)
	s.Name = strings.TrimSpace(s.Name)
	s.Description = strings.TrimSpace(s.Description)
	s.DatasetID = strings.TrimSpace(s.DatasetID)
	s.TargetNodeID = strings.TrimSpace(s.TargetNodeID)

	if s.ID == "" {
		return fmt.Errorf("eval suite id is required")
	}

	if s.DatasetID == "" {
		return fmt.Errorf("eval suite dataset_id is required")
	}

	switch s.Evaluator {
	case EvalEvaluatorRunSucceeded, EvalEvaluatorNodeOutputExact:
	default:
		return fmt.Errorf("unsupported eval evaluator %q", s.Evaluator)
	}

	if s.PassThreshold < 0 || s.PassThreshold > 1 {
		return fmt.Errorf("eval suite pass_threshold must be between 0 and 1")
	}

	if s.PassThreshold == 0 {
		s.PassThreshold = 1
	}

	if s.Evaluator == EvalEvaluatorNodeOutputExact && s.TargetNodeID == "" {
		return fmt.Errorf("eval suite target_node_id is required for %s", s.Evaluator)
	}

	return nil
}

func (r *EvalReport) NormalizeAndValidate() error {
	if r == nil {
		return fmt.Errorf("eval report is nil")
	}

	r.ID = strings.TrimSpace(r.ID)
	r.SuiteID = strings.TrimSpace(r.SuiteID)
	r.DatasetID = strings.TrimSpace(r.DatasetID)
	r.Status = strings.TrimSpace(r.Status)
	r.TargetNodeID = strings.TrimSpace(r.TargetNodeID)

	if r.ID == "" {
		return fmt.Errorf("eval report id is required")
	}

	if r.SuiteID == "" {
		return fmt.Errorf("eval report suite_id is required")
	}

	if r.DatasetID == "" {
		return fmt.Errorf("eval report dataset_id is required")
	}

	if r.Status == "" {
		return fmt.Errorf("eval report status is required")
	}

	return nil
}

func validateOptionalJSON(name string, b json.RawMessage) error {
	if len(b) == 0 {
		return nil
	}

	if !json.Valid(b) {
		return fmt.Errorf("%s must be valid json", name)
	}
	return nil
}

func evalDatasetKey(id string) string { return "evals/datasets/" + strings.TrimSpace(id) }
func evalSuiteKey(id string) string   { return "evals/suites/" + strings.TrimSpace(id) }
func evalReportKey(id string) string  { return "evals/reports/" + strings.TrimSpace(id) }

func putJSONKV(store Store, key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	return store.PutKV(key, string(b))
}

func getJSONKV(store Store, key string, out any) (bool, error) {
	raw, ok := store.GetKV(key)
	if !ok {
		return false, nil
	}

	if err := json.Unmarshal([]byte(raw), out); err != nil {
		return true, err
	}

	return true, nil
}

func indexIDs(store Store, key string) ([]string, error) {
	raw, ok := store.GetKV(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return []string{}, nil
	}

	var ids []string
	if err := json.Unmarshal([]byte(raw), &ids); err != nil {
		return nil, err
	}

	out := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))

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

	sort.Strings(out)
	return out, nil
}

func upsertIndexID(store Store, indexKey, id string) error {
	ids, err := indexIDs(store, indexKey)
	if err != nil {
		return err
	}

	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("index id is required")
	}

	for _, existing := range ids {
		if existing == id {
			return nil
		}
	}

	ids = append(ids, id)
	sort.Strings(ids)
	return putJSONKV(store, indexKey, ids)
}

func (r *Runner) SaveEvalDataset(dataset EvalDataset) error {
	if err := dataset.NormalizeAndValidate(); err != nil {
		return err
	}

	if existing, ok, err := r.GetEvalDataset(dataset.ID); err != nil {
		return err
	} else if ok && existing.CreatedAt != nil {
		dataset.CreatedAt = existing.CreatedAt
	}

	now := time.Now().UTC()
	if dataset.CreatedAt == nil {
		dataset.CreatedAt = &now
	}

	dataset.UpdatedAt = &now

	if err := putJSONKV(r.store, evalDatasetKey(dataset.ID), dataset); err != nil {
		return err
	}

	return upsertIndexID(r.store, evalDatasetIndexKey, dataset.ID)
}

func (r *Runner) GetEvalDataset(id string) (EvalDataset, bool, error) {
	var dataset EvalDataset
	ok, err := getJSONKV(r.store, evalDatasetKey(id), &dataset)

	if err != nil || !ok {
		return dataset, ok, err
	}

	return dataset, true, nil
}

func (r *Runner) ListEvalDatasets() ([]EvalDataset, error) {
	ids, err := indexIDs(r.store, evalDatasetIndexKey)
	if err != nil {
		return nil, err
	}

	out := make([]EvalDataset, 0, len(ids))
	for _, id := range ids {
		dataset, ok, err := r.GetEvalDataset(id)
		if err != nil {
			return nil, err
		}

		if ok {
			out = append(out, dataset)
		}
	}
	return out, nil
}

func (r *Runner) SaveEvalSuite(suite EvalSuite) error {
	if err := suite.NormalizeAndValidate(); err != nil {
		return err
	}

	if _, ok, err := r.GetEvalDataset(suite.DatasetID); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("eval dataset %q not found", suite.DatasetID)
	}

	if existing, ok, err := r.GetEvalSuite(suite.ID); err != nil {
		return err
	} else if ok && existing.CreatedAt != nil {
		suite.CreatedAt = existing.CreatedAt
	}

	now := time.Now().UTC()
	if suite.CreatedAt == nil {
		suite.CreatedAt = &now
	}
	suite.UpdatedAt = &now

	if err := putJSONKV(r.store, evalSuiteKey(suite.ID), suite); err != nil {
		return err
	}

	return upsertIndexID(r.store, evalSuiteIndexKey, suite.ID)
}

func (r *Runner) GetEvalSuite(id string) (EvalSuite, bool, error) {
	var suite EvalSuite
	ok, err := getJSONKV(r.store, evalSuiteKey(id), &suite)
	if err != nil || !ok {
		return suite, ok, err
	}

	return suite, true, nil
}

func (r *Runner) ListEvalSuites() ([]EvalSuite, error) {
	ids, err := indexIDs(r.store, evalSuiteIndexKey)
	if err != nil {
		return nil, err
	}

	out := make([]EvalSuite, 0, len(ids))
	for _, id := range ids {
		suite, ok, err := r.GetEvalSuite(id)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, suite)
		}
	}

	return out, nil
}

func (r *Runner) SaveEvalReport(report EvalReport) error {
	if err := report.NormalizeAndValidate(); err != nil {
		return err
	}

	if err := putJSONKV(r.store, evalReportKey(report.ID), report); err != nil {
		return err
	}

	return upsertIndexID(r.store, evalReportIndexKey, report.ID)
}

func (r *Runner) GetEvalReport(id string) (EvalReport, bool, error) {
	var report EvalReport
	ok, err := getJSONKV(r.store, evalReportKey(id), &report)

	if err != nil || !ok {
		return report, ok, err
	}

	return report, true, nil
}

func (r *Runner) ListEvalReports() ([]EvalReport, error) {
	ids, err := indexIDs(r.store, evalReportIndexKey)
	if err != nil {
		return nil, err
	}

	out := make([]EvalReport, 0, len(ids))
	for _, id := range ids {
		report, ok, err := r.GetEvalReport(id)
		if err != nil {
			return nil, err
		}

		if ok {
			out = append(out, report)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		ti := time.Time{}
		tj := time.Time{}
		if out[i].CreatedAt != nil {
			ti = out[i].CreatedAt.UTC()
		}

		if out[j].CreatedAt != nil {
			tj = out[j].CreatedAt.UTC()
		}

		if ti.Equal(tj) {
			return out[i].ID < out[j].ID
		}

		if ti.IsZero() {
			return false
		}

		if tj.IsZero() {
			return true
		}

		return ti.After(tj)
	})
	return out, nil
}

func (r *Runner) RunEvalSuite(ctx context.Context, req EvalRunRequest) (EvalReport, error) {
	var report EvalReport
	req.SuiteID = strings.TrimSpace(req.SuiteID)
	req.EvalRunID = strings.TrimSpace(req.EvalRunID)

	if req.SuiteID == "" {
		return report, fmt.Errorf("suite_id is required")
	}

	if err := validateOptionalJSON("eval run spec_override", req.SpecOverride); err != nil {
		return report, err
	}

	suite, ok, err := r.GetEvalSuite(req.SuiteID)
	if err != nil {
		return report, err
	}

	if !ok {
		return report, fmt.Errorf("eval suite %q not found", req.SuiteID)
	}

	dataset, ok, err := r.GetEvalDataset(suite.DatasetID)
	if err != nil {
		return report, err
	}

	if !ok {
		return report, fmt.Errorf("eval dataset %q not found", suite.DatasetID)
	}

	reportID := req.EvalRunID
	if reportID == "" {
		reportID = "eval-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	}

	now := time.Now().UTC()
	report = EvalReport{
		ID:            reportID,
		SuiteID:       suite.ID,
		DatasetID:     dataset.ID,
		Status:        "running",
		Evaluator:     suite.Evaluator,
		TargetNodeID:  suite.TargetNodeID,
		PassThreshold: suite.PassThreshold,
		CaseCount:     len(dataset.Cases),
		CreatedAt:     &now,
	}

	if err := r.SaveEvalReport(report); err != nil {
		return report, err
	}

	results := make([]EvalCaseResult, 0, len(dataset.Cases))
	passedCount := 0
	for _, c := range dataset.Cases {
		res := r.runEvalCase(ctx, reportID, suite, c, req.SpecOverride)
		results = append(results, res)
		if res.Passed {
			passedCount++
		}
	}

	completed := time.Now().UTC()
	report.Status = "completed"
	report.Results = results
	report.PassedCount = passedCount

	if report.CaseCount > 0 {
		report.PassRate = float64(report.PassedCount) / float64(report.CaseCount)
	}

	report.Passed = report.PassRate >= report.PassThreshold
	report.CompletedAt = &completed

	if err := r.SaveEvalReport(report); err != nil {
		return report, err
	}
	return report, nil
}

func (r *Runner) runEvalCase(ctx context.Context, reportID string, suite EvalSuite, c EvalCase, specOverride json.RawMessage) EvalCaseResult {
	result := EvalCaseResult{
		CaseID:         c.ID,
		Name:           c.Name,
		SourceRunID:    c.SourceRunID,
		TargetNodeID:   firstNonEmpty(c.TargetNodeID, suite.TargetNodeID),
		ExpectedOutput: cloneRaw(c.ExpectedOutput),
	}

	spec, input, err := r.resolveEvalCaseSpecAndInput(c, specOverride)
	if err != nil {
		result.Reason = err.Error()
		return result
	}

	runID := fmt.Sprintf("%s-case-%s", reportID, sanitizeEvalRunID(c.ID))
	result.RunID = runID

	reg := r.HandlerRegistry()
	if reg == nil {
		result.Reason = "no handler registry configured on runner"
		return result
	}

	runErr := r.RunSpecJSON(ctx, runID, spec, reg, input)
	run, ok := r.store.GetRun(runID)
	if ok {
		result.RunStatus = run.Status
	}

	if runErr != nil && !errors.Is(runErr, ErrNodeFailed) {
		result.Reason = runErr.Error()
		return result
	}

	switch suite.Evaluator {
	case EvalEvaluatorRunSucceeded:
		if ok && run.Status == RunStatusSucceeded {
			result.Passed = true
			result.Score = 1
			result.Reason = "run succeeded"
		} else if ok {
			result.Reason = fmt.Sprintf("run status=%s", run.Status)
		} else {
			result.Reason = "run result unavailable"
		}

	case EvalEvaluatorNodeOutputExact:
		targetNodeID := firstNonEmpty(c.TargetNodeID, suite.TargetNodeID)
		if targetNodeID == "" {
			result.Reason = "target_node_id is required"
			return result
		}

		output, found := latestNodeOutput(r.store.ListNodeExecutions(runID), targetNodeID)
		if !found {
			result.Reason = fmt.Sprintf("node output not found for %s", targetNodeID)
			return result
		}

		result.ActualOutput = cloneRaw(output)
		if len(c.ExpectedOutput) == 0 {
			result.Reason = "expected_output is required"
			return result
		}

		if jsonEqual(c.ExpectedOutput, output) {
			result.Passed = true
			result.Score = 1
			result.Reason = "exact output match"
			return result
		}

		result.Reason = "output mismatch"

	default:
		result.Reason = fmt.Sprintf("unsupported evaluator %q", suite.Evaluator)
	}

	return result
}

func (r *Runner) resolveEvalCaseSpecAndInput(c EvalCase, specOverride json.RawMessage) ([]byte, json.RawMessage, error) {
	spec := cloneRaw(specOverride)
	input := cloneRaw(c.Input)

	var srcRun Run
	if c.SourceRunID != "" {
		run, ok := r.store.GetRun(c.SourceRunID)
		if !ok {
			return nil, nil, fmt.Errorf("source run %q not found", c.SourceRunID)
		}
		srcRun = run
	}

	if len(spec) == 0 {
		if len(c.Spec) != 0 {
			spec = cloneRaw(c.Spec)
		} else if len(srcRun.Spec) != 0 {
			spec = cloneRaw(srcRun.Spec)
		}
	}

	if len(input) == 0 {
		if len(srcRun.InitialInput) != 0 {
			input = cloneRaw(srcRun.InitialInput)
		} else {
			input = json.RawMessage(`{}`)
		}
	}

	if len(spec) == 0 {
		return nil, nil, fmt.Errorf("eval case %q has no workflow spec", c.ID)
	}

	if !json.Valid(spec) {
		return nil, nil, fmt.Errorf("eval case %q resolved invalid workflow spec", c.ID)
	}

	if len(input) != 0 && !json.Valid(input) {
		return nil, nil, fmt.Errorf("eval case %q resolved invalid input", c.ID)
	}

	return []byte(spec), input, nil
}

func (r *Runner) CreateEvalCaseFromRun(datasetID, caseID, name, runID, nodeID string) (EvalDataset, EvalCase, error) {
	var emptyDataset EvalDataset
	var emptyCase EvalCase

	dataset, ok, err := r.GetEvalDataset(datasetID)
	if err != nil {
		return emptyDataset, emptyCase, err
	}

	if !ok {
		return emptyDataset, emptyCase, fmt.Errorf("eval dataset %q not found", datasetID)
	}

	run, ok := r.store.GetRun(strings.TrimSpace(runID))
	if !ok {
		return emptyDataset, emptyCase, fmt.Errorf("run %q not found", runID)
	}

	nodeID = strings.TrimSpace(nodeID)
	caseID = strings.TrimSpace(caseID)
	if caseID == "" {
		caseID = "run-" + sanitizeEvalRunID(run.RunID)
		if nodeID != "" {
			caseID += "-" + sanitizeEvalRunID(nodeID)
		}
	}

	name = strings.TrimSpace(name)
	if name == "" {
		name = caseID
	}

	capturedCase := EvalCase{
		ID:           caseID,
		Name:         name,
		SourceRunID:  run.RunID,
		Spec:         cloneRaw(run.Spec),
		Input:        cloneRaw(run.InitialInput),
		TargetNodeID: nodeID,
		Labels: map[string]string{
			"captured_from_run": "true",
			"source_run_status": string(run.Status),
		},
	}

	if nodeID != "" {
		if output, ok := latestNodeOutput(r.store.ListNodeExecutions(run.RunID), nodeID); ok {
			capturedCase.ExpectedOutput = cloneRaw(output)
		}
		capturedCase.Labels["source_node_id"] = nodeID
	}

	replaced := false
	for i := range dataset.Cases {
		if dataset.Cases[i].ID == capturedCase.ID {
			dataset.Cases[i] = capturedCase
			replaced = true
			break
		}
	}

	if !replaced {
		dataset.Cases = append(dataset.Cases, capturedCase)
		sort.Slice(dataset.Cases, func(i, j int) bool {
			return dataset.Cases[i].ID < dataset.Cases[j].ID
		})
	}

	if err := r.SaveEvalDataset(dataset); err != nil {
		return emptyDataset, emptyCase, err
	}

	return dataset, capturedCase, nil
}

func (r *Runner) PromoteRunWithEvalGate(evalRunID, runID, version string) (string, error) {
	report, ok, err := r.GetEvalReport(evalRunID)
	if err != nil {
		return "", err
	}

	if !ok {
		return "", fmt.Errorf("eval report %q not found", evalRunID)
	}

	if report.Status != "completed" {
		return "", fmt.Errorf("eval report %q status=%s (need completed)", evalRunID, report.Status)
	}

	if !report.Passed {
		return "", fmt.Errorf("eval report %q did not pass promotion gate", evalRunID)
	}

	return PromoteIndexVersion(r.store, runID, version)
}

func latestNodeOutput(nodes []NodeExecution, nodeID string) (json.RawMessage, bool) {
	bestAttempt := -1
	var output json.RawMessage
	for _, node := range nodes {
		if node.NodeID != nodeID || node.Status != NodeStatusSucceeded {
			continue
		}

		if node.Attempt > bestAttempt {
			bestAttempt = node.Attempt
			output = cloneRaw(node.Output)
		}
	}

	if bestAttempt < 0 {
		return nil, false
	}
	return output, true
}

func jsonEqual(a, b json.RawMessage) bool {
	var left any
	var right any
	if err := json.Unmarshal(a, &left); err != nil {
		return false
	}

	if err := json.Unmarshal(b, &right); err != nil {
		return false
	}

	lb, err := json.Marshal(left)
	if err != nil {
		return false
	}

	rb, err := json.Marshal(right)
	if err != nil {
		return false
	}

	return string(lb) == string(rb)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}

	return ""
}

func sanitizeEvalRunID(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "case"
	}

	replacer := strings.NewReplacer("/", "-", "\\", "-", " ", "-", "\t", "-", "\n", "-", ":", "-", ".", "-", "@", "-")
	s = replacer.Replace(s)
	s = strings.Trim(s, "-")

	if s == "" {
		return "case"
	}

	return s
}
