package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

type debugRunsResp struct {
	Ok    bool     `json:"ok"`
	Count int      `json:"count"`
	Runs  []string `json:"runs"`
}

type nodeStatusRow struct {
	NodeID      string     `json:"node_id"`
	Attempt     int        `json:"attempt"`
	Status      string     `json:"status"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	EndedAt     *time.Time `json:"ended_at,omitempty"`
	Error       string     `json:"error,omitempty"`
	HasInput    bool       `json:"has_input"`
	HasOutput   bool       `json:"has_output"`
	InputBytes  int        `json:"input_bytes,omitempty"`
	OutputBytes int        `json:"output_bytes,omitempty"`
}

type nodeExec struct {
	RunID      string     `json:"run_id"`
	WorkflowID string     `json:"workflow_id"`
	NodeID     string     `json:"node_id"`
	Attempt    int        `json:"attempt"`
	Status     string     `json:"status"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	EndedAt    *time.Time `json:"ended_at,omitempty"`
	Error      string     `json:"error,omitempty"`
	Input      any        `json:"input,omitempty"`
	Output     any        `json:"output,omitempty"`
}

type timelineRow struct {
	StepID        string
	Attempt       int64
	UsedCached    bool
	CachedAttempt int64
	QueuedAt      string
	StartedAt     string
	EndedAt       string
	QueueMS       int64
	WorkerMS      int64
}

type runStateResp struct {
	Ok     bool       `json:"ok"`
	Run    any        `json:"run"`
	Nodes  []nodeExec `json:"nodes"`
	Events any        `json:"events"`
	Timers any        `json:"timers"`
}

type debugRunResp struct {
	Run   map[string]any  `json:"run"`
	Nodes []nodeStatusRow `json:"nodes"`
}

type artifactMetaSummary struct {
	ContentType string
	Sha256      string
	SizeBytes   int64
}

func cmdRuns(baseURL string, timeout time.Duration, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("runs: missing subcommand (use: list|ls|status|step|events|state|diff|cancel|demo|artifacts|artifact-meta|artifact-get)")
	}

	switch args[0] {
	case "artifacts":
		return runsArtifacts(baseURL, timeout, args[1:])

	case "artifact-meta":
		return runsArtifactMeta(baseURL, timeout, args[1:])

	case "artifact-get":
		return runsArtifactGet(baseURL, timeout, args[1:])

	case "cancel":
		return runsCancel(baseURL, timeout, args[1:])

	case "demo":
		return runsDemo(baseURL, timeout, args[1:])

	case "diff":
		return runsDiff(baseURL, timeout, args[1:])

	case "list", "ls":
		return runsList(baseURL, timeout, args[1:])

	case "status":
		return runsStatus(baseURL, timeout, args[1:])

	case "step":
		return runsStep(baseURL, timeout, args[1:])

	case "events":
		return runsEvents(baseURL, timeout, args[1:])

	case "state":
		return runsState(baseURL, timeout, args[1:])

	case "show", "get":
		return runsStatus(baseURL, timeout, args[1:])

	case "replay":
		return runsReplay(baseURL, timeout, args[1:])

	case "timeline":
		return runsTimeline(baseURL, timeout, args[1:])

	case "promote":
		return runsPromote(baseURL, timeout, args[1:])

	case "rollback":
		return runsRollback(baseURL, timeout, args[1:])

	case "active-index":
		return runsActiveIndex(baseURL, timeout, args[1:])

	default:
		return fmt.Errorf("runs: unknown subcommand %q (use: list|status|step|events|state|diff|cancel|demo|artifacts|artifact-meta|artifact-get)", args[0])
	}
}

func runsDemo(baseURL string, timeout time.Duration, args []string) error {
	// for now ignoring it
	_ = args
	resp, err := doPOST(baseURL, timeout, "/debug/run-demo")

	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("runs demo failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		// fallback: just print raw JSON
		fmt.Println(strings.TrimSpace(string(body)))
		return nil
	}

	runID, _ := out["run_id"].(string)
	traceID, _ := out["trace_id"].(string)

	if runID == "" {
		fmt.Println(strings.TrimSpace(string(body)))
		return nil
	}

	if traceID != "" {
		fmt.Printf("run_id=%s trace_id=%s\n", runID, traceID)
	} else {
		fmt.Printf("run_id=%s\n", runID)
	}

	fmt.Printf("next: driftqctl runs status --run-id %s\n", runID)

	return nil
}

func runsCancel(baseURL string, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("runs cancel", flag.ContinueOnError)
	runID := fs.String("run-id", "", "run id (required)")
	reason := fs.String("reason", "", "optional reason")
	raw := fs.Bool("raw", false, "print raw JSON response")

	if err := fs.Parse(args); err != nil {
		return err
	}

	id := strings.TrimSpace(*runID)
	if id == "" {
		return fmt.Errorf("runs cancel: --run-id is required")
	}

	bodyObj := map[string]string{"run_id": id}
	if strings.TrimSpace(*reason) != "" {
		bodyObj["reason"] = strings.TrimSpace(*reason)
	}

	b, err := json.Marshal(bodyObj)
	if err != nil {
		return fmt.Errorf("runs cancel: marshal body: %w", err)
	}

	u := strings.TrimRight(baseURL, "/") + "/debug/run-cancel"

	client := http.DefaultClient
	if timeout > 0 {
		client = &http.Client{Timeout: timeout}
	}

	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("runs cancel failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	if *raw {
		fmt.Println(strings.TrimSpace(string(respBody)))
		return nil
	}

	fmt.Printf("canceled run_id=%s\n", id)
	return nil
}

func runsStatus(baseURL string, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("runs status", flag.ContinueOnError)
	runID := fs.String("run-id", "", "run id (required)")
	raw := fs.Bool("raw", false, "print raw JSON response")

	if err := fs.Parse(args); err != nil {
		return err
	}

	id := strings.TrimSpace(*runID)
	if id == "" {
		return fmt.Errorf("runs status: --run-id is required")
	}

	path := "/debug/run?run_id=" + url.QueryEscape(id)
	resp, err := doGET(baseURL, timeout, path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("runs status failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	if *raw {
		fmt.Println(strings.TrimSpace(string(body)))
		return nil
	}

	var out debugRunResp
	if err := json.Unmarshal(body, &out); err != nil {
		fmt.Println(strings.TrimSpace(string(body)))
		return nil
	}

	status := pickString(out.Run, "Status", "status")
	wfid := pickString(out.Run, "WorkflowID", "workflow_id", "workflowId")
	updated := pickString(out.Run, "UpdatedAt", "updated_at", "updatedAt")

	fmt.Printf("run_id=%s", id)
	if wfid != "" {
		fmt.Printf(" workflow_id=%s", wfid)
	}

	if status != "" {
		fmt.Printf(" status=%s", status)
	}

	if updated != "" {
		fmt.Printf(" updated_at=%s", updated)
	}

	fmt.Println()

	if len(out.Nodes) == 0 {
		fmt.Println("(no node executions)")
		return nil
	}

	fmt.Println("NODE\tATT\tSTATUS\tDUR\tIN\tOUT\tERR")
	for _, n := range out.Nodes {
		dur := "-"
		if n.StartedAt != nil && n.EndedAt != nil {
			dur = n.EndedAt.Sub(*n.StartedAt).String()
		}

		errStr := strings.TrimSpace(n.Error)
		if len(errStr) > 80 {
			errStr = errStr[:77] + "..."
		}

		fmt.Printf("%s\t%d\t%s\t%s\t%d\t%d\t%s\n",
			n.NodeID, n.Attempt, n.Status, dur, n.InputBytes, n.OutputBytes, errStr,
		)
	}

	return nil
}

func runsStep(baseURL string, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("runs step", flag.ContinueOnError)
	runID := fs.String("run-id", "", "run id (required)")
	nodeID := fs.String("node-id", "", "node id (required)")
	attempt := fs.Int("attempt", 0, "attempt number (optional)")
	raw := fs.Bool("raw", false, "print raw JSON for matching node execution(s)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	id := strings.TrimSpace(*runID)
	nid := strings.TrimSpace(*nodeID)
	if id == "" {
		return fmt.Errorf("runs step: --run-id is required")
	}

	if nid == "" {
		return fmt.Errorf("runs step: --node-id is required")
	}

	root, err := fetchRunState(baseURL, timeout, id)
	if err != nil {
		return err
	}

	var nodes []map[string]any
	{
		b, err := json.Marshal(root.Nodes)
		if err != nil {
			return fmt.Errorf("runs step: marshal nodes: %w", err)
		}

		if string(b) != "null" && string(b) != "" {
			if err := json.Unmarshal(b, &nodes); err != nil {
				return fmt.Errorf("runs step: decode nodes: %w", err)
			}
		}
	}

	if len(nodes) == 0 {
		fmt.Println("(no node executions)")
		return nil
	}

	type match struct {
		Attempt int
		Obj     map[string]any
	}
	var matches []match

	for _, m := range nodes {
		if pickString(m, "node_id", "nodeId", "NodeID") != nid {
			continue
		}

		att := pickInt(m, "attempt", "Attempt")
		if *attempt > 0 && att != *attempt {
			continue
		}

		matches = append(matches, match{Attempt: att, Obj: m})
	}

	if len(matches) == 0 {
		if *attempt > 0 {
			return fmt.Errorf("runs step: no executions found for node=%q attempt=%d", nid, *attempt)
		}

		return fmt.Errorf("runs step: no executions found for node=%q", nid)
	}

	sort.Slice(matches, func(i, j int) bool { return matches[i].Attempt < matches[j].Attempt })

	if *raw {
		for _, mm := range matches {
			b, _ := json.MarshalIndent(mm.Obj, "", "  ")
			fmt.Println(string(b))
			fmt.Println()
		}
		return nil
	}

	// nice summary view
	fmt.Printf("run_id=%s node_id=%s\n", id, nid)
	fmt.Println("ATT\tSTATUS\tDUR\tSTART\tEND\tERR")

	for _, mm := range matches {
		m := mm.Obj
		status := pickString(m, "status", "Status")
		startS := pickString(m, "started_at", "startedAt", "StartedAt")
		endS := pickString(m, "ended_at", "endedAt", "EndedAt")
		errStr := pickString(m, "error", "Error")

		dur := "-"
		if startS != "" && endS != "" {
			st, e1 := time.Parse(time.RFC3339Nano, startS)
			en, e2 := time.Parse(time.RFC3339Nano, endS)
			if e1 == nil && e2 == nil {
				dur = en.Sub(st).String()
			}
		}

		if len(errStr) > 90 {
			errStr = errStr[:87] + "..."
		}

		fmt.Printf("%d\t%s\t%s\t%s\t%s\t%s\n",
			mm.Attempt,
			emptyTo(status, "-"),
			dur,
			shortTime(startS),
			shortTime(endS),
			emptyTo(errStr, "-"),
		)
	}

	return nil
}

func runsEvents(baseURL string, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("runs events", flag.ContinueOnError)
	runID := fs.String("run-id", "", "run id (required)")
	raw := fs.Bool("raw", false, "print raw events JSON (one object per block)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	id := strings.TrimSpace(*runID)
	if id == "" {
		return fmt.Errorf("runs events: --run-id is required")
	}

	root, err := fetchRunState(baseURL, timeout, id)
	if err != nil {
		return err
	}

	var evs []map[string]any
	{
		b, err := json.Marshal(root.Events)
		if err != nil {
			return fmt.Errorf("runs events: marshal events: %w", err)
		}
		if string(b) != "null" && string(b) != "" {
			if err := json.Unmarshal(b, &evs); err != nil {
				return fmt.Errorf("runs events: decode events: %w", err)
			}
		}
	}

	if len(evs) == 0 {
		fmt.Println("(no events)")
		return nil
	}

	if *raw {
		for _, e := range evs {
			b, _ := json.MarshalIndent(e, "", "  ")
			fmt.Println(string(b))
			fmt.Println()
		}
		return nil
	}

	fmt.Println("TIME\tNODE\tATT\tEVENT\tDETAIL")
	for _, m := range evs {
		at := pickString(m, "at", "At")
		node := pickString(m, "node_id", "nodeId", "NodeID")
		att := pickInt(m, "attempt", "Attempt")
		typ := pickString(m, "type", "Type")

		detail := ""
		if p, ok := m["payload"]; ok && p != nil {
			detail = compactAny(p, 140)
		}

		fmt.Printf("%s\t%s\t%d\t%s\t%s\n", at, node, att, typ, detail)
	}

	return nil
}

func runsState(baseURL string, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("runs state", flag.ContinueOnError)
	runID := fs.String("run-id", "", "run id (required)")
	raw := fs.Bool("raw", false, "print raw JSON response")
	if err := fs.Parse(args); err != nil {
		return err
	}

	id := strings.TrimSpace(*runID)
	if id == "" {
		return fmt.Errorf("runs state: --run-id is required")
	}

	path := "/debug/run-state?run_id=" + url.QueryEscape(id)
	resp, err := doGET(baseURL, timeout, path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("runs state failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	if *raw {
		fmt.Println(strings.TrimSpace(string(body)))
		return nil
	}

	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		fmt.Println(strings.TrimSpace(string(body)))
		return nil
	}

	pretty, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(pretty))
	return nil
}

func runsList(baseURL string, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("runs list", flag.ContinueOnError)
	limit := fs.Int("limit", 50, "max runs to return")
	raw := fs.Bool("raw", false, "print raw JSON response")

	if err := fs.Parse(args); err != nil {
		return err
	}

	q := url.Values{}
	if *limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", *limit))
	}

	path := "/debug/runs"
	if enc := q.Encode(); enc != "" {
		path += "?" + enc
	}

	resp, err := doGET(baseURL, timeout, path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("runs list failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	if *raw {
		fmt.Println(strings.TrimSpace(string(body)))
		return nil
	}

	var out debugRunsResp
	if err := json.Unmarshal(body, &out); err != nil {
		fmt.Println(strings.TrimSpace(string(body)))
		return nil
	}

	if len(out.Runs) == 0 {
		fmt.Println("(no runs)")
		return nil
	}

	// IMPORTANT: don't sort here if you want "newest first" from the server.
	for _, id := range out.Runs {
		fmt.Println(id)
	}

	return nil
}

func runsDiff(baseURL string, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("runs diff", flag.ContinueOnError)
	runID := fs.String("run-id", "", "run id (required)")
	nodeID := fs.String("node-id", "", "node id (required)")
	from := fs.Int("from", 0, "from attempt (optional; default: previous attempt)")
	to := fs.Int("to", 0, "to attempt (optional; default: latest attempt)")
	raw := fs.Bool("raw", false, "print full raw JSON for both attempts")

	if err := fs.Parse(args); err != nil {
		return err
	}

	id := strings.TrimSpace(*runID)
	nid := strings.TrimSpace(*nodeID)
	if id == "" {
		return fmt.Errorf("runs diff: --run-id is required")
	}

	if nid == "" {
		return fmt.Errorf("runs diff: --node-id is required")
	}

	root, err := fetchRunState(baseURL, timeout, id)
	if err != nil {
		return err
	}

	nodes, err := decodeNodesAsMaps(root)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return fmt.Errorf("runs diff: no node executions found for run_id=%q", id)
	}

	// collect attempts for this node
	byAttempt := map[int]map[string]any{}
	var attempts []int

	for _, m := range nodes {
		if pickString(m, "node_id", "nodeId", "NodeID") != nid {
			continue
		}
		att := pickInt(m, "attempt", "Attempt")
		if att <= 0 {
			continue
		}
		byAttempt[att] = m
		attempts = append(attempts, att)
	}

	if len(attempts) == 0 {
		return fmt.Errorf("runs diff: no executions found for node=%q", nid)
	}

	sort.Ints(attempts)
	attempts = uniqueSortedInts(attempts)

	// pick default from/to if not provided
	f := *from
	t := *to

	if f == 0 && t == 0 {
		if len(attempts) < 2 {
			return fmt.Errorf("runs diff: need at least 2 attempts for node=%q (have %d)", nid, len(attempts))
		}
		f = attempts[len(attempts)-2]
		t = attempts[len(attempts)-1]
	} else if f == 0 && t > 0 {
		// pick the attempt immediately before 'to'
		idx := indexOfInt(attempts, t)
		if idx <= 0 {
			return fmt.Errorf("runs diff: cannot infer --from for --to=%d (available: %v)", t, attempts)
		}
		f = attempts[idx-1]
	} else if f > 0 && t == 0 {
		// pick the attempt immediately after 'from' if exists; otherwise latest
		idx := indexOfInt(attempts, f)
		if idx < 0 {
			return fmt.Errorf("runs diff: attempt --from=%d not found (available: %v)", f, attempts)
		}
		if idx+1 < len(attempts) {
			t = attempts[idx+1]
		} else {
			if len(attempts) < 2 {
				return fmt.Errorf("runs diff: need at least 2 attempts for node=%q", nid)
			}
			t = attempts[len(attempts)-1]
			if t == f {
				return fmt.Errorf("runs diff: cannot infer --to; only attempt=%d exists", f)
			}
		}
	}

	fromObj, ok := byAttempt[f]
	if !ok {
		return fmt.Errorf("runs diff: attempt --from=%d not found for node=%q (available: %v)", f, nid, attempts)
	}
	toObj, ok := byAttempt[t]
	if !ok {
		return fmt.Errorf("runs diff: attempt --to=%d not found for node=%q (available: %v)", t, nid, attempts)
	}

	if *raw {
		fmt.Printf("run_id=%s node_id=%s from=%d to=%d\n\n", id, nid, f, t)

		b1, _ := json.MarshalIndent(fromObj, "", "  ")
		fmt.Println(string(b1))
		fmt.Println()

		b2, _ := json.MarshalIndent(toObj, "", "  ")
		fmt.Println(string(b2))
		return nil
	}

	// summary
	fmt.Printf("run_id=%s node_id=%s attempts=%d->%d\n", id, nid, f, t)

	fromStatus := pickString(fromObj, "status", "Status")
	toStatus := pickString(toObj, "status", "Status")
	fmt.Printf("status: %s -> %s\n", emptyTo(fromStatus, "-"), emptyTo(toStatus, "-"))

	fromErr := pickString(fromObj, "error", "Error")
	toErr := pickString(toObj, "error", "Error")
	fmt.Printf("error:  %s -> %s\n", emptyTo(fromErr, "-"), emptyTo(toErr, "-"))

	fmt.Printf("dur:    %s -> %s\n", durFromMap(fromObj), durFromMap(toObj))

	printAnyDelta("input", fromObj["input"], toObj["input"])
	printAnyDelta("output", fromObj["output"], toObj["output"])

	return nil
}

func pickString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok {
				return strings.TrimSpace(s)
			}
		}
	}

	return ""
}

func pickInt(m map[string]any, keys ...string) int {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch vv := v.(type) {
			case float64:
				return int(vv)
			case int:
				return vv
			}
		}
	}

	return 0
}

func compactAny(v any, max int) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}

	s := strings.TrimSpace(string(b))
	if len(s) > max {
		return s[:max-3] + "..."
	}

	return s
}

func emptyTo(s, fallback string) string {
	if strings.TrimSpace(s) == "" {
		return fallback
	}

	return s
}

func shortTime(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "-"
	}

	// print just time-ish part to keep table readable
	if len(s) > 19 {
		return s[:19]
	}

	return s
}

func fetchRunState(baseURL string, timeout time.Duration, runID string) (*runStateResp, error) {
	path := "/debug/run-state?run_id=" + url.QueryEscape(runID)
	resp, err := doGET(baseURL, timeout, path)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("runs state failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var out runStateResp
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("runs state: bad json: %w", err)
	}
	return &out, nil
}

func prettyJSON(v any) string {
	if v == nil {
		return ""
	}

	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("%v", v)
	}

	return string(b)
}

func decodeNodesAsMaps(root *runStateResp) ([]map[string]any, error) {
	var nodes []map[string]any

	b, err := json.Marshal(root.Nodes)
	if err != nil {
		return nil, fmt.Errorf("runs: marshal nodes: %w", err)
	}

	if string(b) == "null" || len(b) == 0 {
		return nil, nil
	}

	if err := json.Unmarshal(b, &nodes); err != nil {
		return nil, fmt.Errorf("runs: decode nodes: %w", err)
	}

	return nodes, nil
}

func durFromMap(m map[string]any) string {
	startS := pickString(m, "started_at", "startedAt", "StartedAt")
	endS := pickString(m, "ended_at", "endedAt", "EndedAt")
	if startS == "" || endS == "" {
		return "-"
	}

	st, e1 := time.Parse(time.RFC3339Nano, startS)
	en, e2 := time.Parse(time.RFC3339Nano, endS)
	if e1 != nil || e2 != nil {
		return "-"
	}

	return en.Sub(st).String()
}

func printAnyDelta(label string, a, b any) {
	sa := canonicalJSON(a)
	sb := canonicalJSON(b)

	if sa == sb {
		fmt.Printf("%s: (unchanged)\n", label)
		return
	}

	fmt.Printf("%s: changed\n", label)

	if sa == "" {
		fmt.Printf("  - from: <empty>\n")
	} else {
		fmt.Printf("  - from:\n%s\n", indentBlock(prettyJSON(a), "    "))
	}

	if sb == "" {
		fmt.Printf("  - to:   <empty>\n")
	} else {
		fmt.Printf("  - to:\n%s\n", indentBlock(prettyJSON(b), "    "))
	}
}

func canonicalJSON(v any) string {
	if v == nil {
		return ""
	}

	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}

	// normalize whitespace by re-decoding
	var tmp any
	if err := json.Unmarshal(b, &tmp); err != nil {
		return string(bytes.TrimSpace(b))
	}

	nb, _ := json.Marshal(tmp)
	return string(nb)
}

func indentBlock(s, prefix string) string {
	if s == "" {
		return ""
	}

	lines := strings.Split(s, "\n")
	for i := range lines {
		lines[i] = prefix + lines[i]
	}

	return strings.Join(lines, "\n")
}

func uniqueSortedInts(in []int) []int {
	if len(in) == 0 {
		return in
	}

	out := []int{in[0]}
	for i := 1; i < len(in); i++ {
		if in[i] != in[i-1] {
			out = append(out, in[i])
		}
	}

	return out
}

func indexOfInt(xs []int, v int) int {
	for i := range xs {
		if xs[i] == v {
			return i
		}
	}

	return -1
}

func fetchArtifactMeta(baseURL string, timeout time.Duration, artifactID string) (*artifactMetaSummary, error) {
	path := "/debug/artifact-meta?artifact_id=" + url.QueryEscape(strings.TrimSpace(artifactID))
	resp, err := doGET(baseURL, timeout, path)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("artifact-meta failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var root map[string]any
	if err := json.Unmarshal(body, &root); err != nil {
		return nil, fmt.Errorf("artifact-meta: bad json: %w", err)
	}

	metaAny, _ := root["meta"]
	meta, _ := metaAny.(map[string]any)

	out := &artifactMetaSummary{
		ContentType: pickString(meta, "content_type", "contentType", "ContentType"),
		Sha256:      pickString(meta, "sha256", "Sha256", "SHA256"),
		SizeBytes:   pickInt64(meta, "size_bytes", "sizeBytes", "SizeBytes", "bytes", "Bytes", "size", "Size"),
	}
	return out, nil
}

func pickInt64(m map[string]any, keys ...string) int64 {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch vv := v.(type) {
			case float64:
				return int64(vv)

			case int:
				return int64(vv)

			case int64:
				return vv
			}
		}
	}

	return 0
}

func runsReplay(baseURL string, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("runs replay", flag.ContinueOnError)
	runID := fs.String("run-id", "", "run id (required)")
	fromStep := fs.String("from-step", "", "step/node id to restart from (required)")
	mode := fs.String("mode", "time-travel", "replay mode: time-travel|live")

	if err := fs.Parse(args); err != nil {
		return err
	}

	id := strings.TrimSpace(*runID)
	step := strings.TrimSpace(*fromStep)
	m := strings.ToLower(strings.TrimSpace(*mode))
	if id == "" {
		return fmt.Errorf("runs replay: --run-id is required")
	}

	if step == "" {
		return fmt.Errorf("runs replay: --from-step is required")
	}

	if m == "" {
		m = "time-travel"
	}

	switch m {
	case "time-travel", "timetravel", "tt", "time_travel":
		m = "time_travel"
	case "live":
		// ok
	default:
		return fmt.Errorf("runs replay: invalid --mode %q (use: time-travel|live)", *mode)
	}

	bodyObj := map[string]any{
		"run_id":    id,
		"from_step": step,
		"mode":      m,
	}

	b, err := json.Marshal(bodyObj)
	if err != nil {
		return fmt.Errorf("runs replay: marshal body: %w", err)
	}

	u := strings.TrimRight(baseURL, "/") + "/debug/run-replay"

	client := http.DefaultClient
	if timeout > 0 {
		client = &http.Client{Timeout: timeout}
	}

	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	rb, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("runs replay failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(rb)))
	}

	// Best-effort parse for nicer output.
	var out map[string]any
	_ = json.Unmarshal(rb, &out)

	newID := id
	if v, ok := out["run_id"].(string); ok && strings.TrimSpace(v) != "" {
		newID = strings.TrimSpace(v)
	}

	fmt.Printf("replay started run_id=%s from_step=%s mode=%s\n", newID, step, m)
	fmt.Printf("next: driftqctl runs status --run-id %s\n", newID)
	fmt.Printf("next: driftqctl runs timeline --run-id %s\n", newID)

	return nil
}

func runsTimeline(baseURL string, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("runs timeline", flag.ContinueOnError)
	runID := fs.String("run-id", "", "run id (required)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	id := strings.TrimSpace(*runID)
	if id == "" {
		return fmt.Errorf("runs timeline: --run-id is required")
	}

	root, err := fetchRunState(baseURL, timeout, id)
	if err != nil {
		return err
	}

	eventsAny := root.Events
	var events []any
	switch v := eventsAny.(type) {
	case []any:
		events = v
	case nil:
		// keep empty
	default:
		// Some servers might wrap events under an object; try common shapes.
		if m, ok := v.(map[string]any); ok {
			if arr, ok := m["events"].([]any); ok {
				events = arr
			} else if arr, ok := m["items"].([]any); ok {
				events = arr
			}
		}
	}
	if len(events) == 0 {
		return fmt.Errorf("runs timeline: no events available for run %q", id)
	}

	rows := make([]timelineRow, 0, 32)
	for _, ev := range events {
		em, ok := ev.(map[string]any)
		if !ok {
			continue
		}
		typeS := strings.ToLower(pickString(em, "type", "event_type", "kind"))
		if typeS == "" {
			continue
		}
		if !strings.Contains(typeS, "node_finished") && !strings.Contains(typeS, "nodefinished") {
			continue
		}

		step := pickString(em, "node_id", "step_id", "node", "step")
		attempt := pickInt64(em, "attempt", "try", "n")
		payload, _ := em["payload"].(map[string]any)

		row := timelineRow{
			StepID:        step,
			Attempt:       attempt,
			UsedCached:    pickBool(payload, "used_cached_output", "used_cached", "cached"),
			CachedAttempt: pickInt64(payload, "cached_attempt", "cache_attempt"),
			QueuedAt:      shortTime(pickString(payload, "queued_at")),
			StartedAt:     shortTime(pickString(payload, "started_at")),
			EndedAt:       shortTime(pickString(payload, "ended_at")),
			QueueMS:       pickInt64(payload, "queue_ms"),
			WorkerMS:      pickInt64(payload, "worker_ms"),
		}
		rows = append(rows, row)
	}

	if len(rows) == 0 {
		return fmt.Errorf("runs timeline: couldn't find node_finished events for run %q", id)
	}

	// Sort by attempt then step id for stable output.
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Attempt != rows[j].Attempt {
			return rows[i].Attempt < rows[j].Attempt
		}
		if rows[i].StepID != rows[j].StepID {
			return rows[i].StepID < rows[j].StepID
		}
		return rows[i].WorkerMS < rows[j].WorkerMS
	})

	fmt.Printf("attempt\tstep\tcached\tcache_attempt\tqueue_ms\tworker_ms\tqueued_at\tstarted_at\tended_at\n")
	for _, r := range rows {
		fmt.Printf("%d\t%s\t%t\t%d\t%d\t%d\t%s\t%s\t%s\n",
			r.Attempt,
			emptyTo(r.StepID, "-"),
			r.UsedCached,
			r.CachedAttempt,
			r.QueueMS,
			r.WorkerMS,
			emptyTo(r.QueuedAt, "-"),
			emptyTo(r.StartedAt, "-"),
			emptyTo(r.EndedAt, "-"),
		)
	}

	return nil
}

func pickBool(m map[string]any, keys ...string) bool {
	if m == nil {
		return false
	}
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch t := v.(type) {
			case bool:
				return t
			case string:
				s := strings.ToLower(strings.TrimSpace(t))
				return s == "true" || s == "1" || s == "yes" || s == "y"
			case float64:
				return t != 0
			}
		}
	}
	return false
}

func runsActiveIndex(baseURL string, timeout time.Duration, args []string) error {
	// no flags for now
	_ = args

	client := &http.Client{Timeout: timeout}
	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(baseURL, "/")+"/debug/index/active", nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("runs active-index: %s", strings.TrimSpace(string(b)))
	}

	var root map[string]any
	if err := json.Unmarshal(b, &root); err != nil {
		return fmt.Errorf("runs active-index: decode json: %w", err)
	}

	ver := ""
	if v, ok := root["active_version"].(string); ok {
		ver = v
	}
	if ver == "" {
		fmt.Println("(no active index version set)")
		return nil
	}

	fmt.Printf("active_index_version=%s\n", ver)
	return nil
}

func runsPromote(baseURL string, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("runs promote", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	runID := fs.String("run-id", "", "Run ID to promote from (must be succeeded)")
	version := fs.String("version", "", "Index version to promote (defaults to run-id)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	id := strings.TrimSpace(*runID)
	if id == "" {
		return fmt.Errorf("runs promote: --run-id is required")
	}

	ver := strings.TrimSpace(*version)
	if ver == "" {
		ver = id
	}

	client := &http.Client{Timeout: timeout}
	url := strings.TrimRight(baseURL, "/") + "/debug/index/promote"
	body, _ := json.Marshal(map[string]any{"run_id": id, "version": ver})
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("runs promote: %s", strings.TrimSpace(string(b)))
	}

	var root map[string]any
	if err := json.Unmarshal(b, &root); err != nil {
		return fmt.Errorf("runs promote: decode json: %w", err)
	}

	active := ver
	if v, ok := root["active_version"].(string); ok && strings.TrimSpace(v) != "" {
		active = v
	}

	fmt.Printf("promoted active_index_version=%s\n", active)
	fmt.Printf("next: driftqctl runs active-index\n")
	return nil
}

func runsRollback(baseURL string, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("runs rollback", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	to := fs.String("to", "", "Index version to rollback to")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ver := strings.TrimSpace(*to)
	if ver == "" {
		return fmt.Errorf("runs rollback: --to is required")
	}

	client := &http.Client{Timeout: timeout}
	url := strings.TrimRight(baseURL, "/") + "/debug/index/rollback"
	body, _ := json.Marshal(map[string]any{"version": ver})
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("runs rollback: %s", strings.TrimSpace(string(b)))
	}

	fmt.Printf("rolled back active_index_version=%s\n", ver)
	fmt.Printf("next: driftqctl runs active-index\n")
	return nil
}
