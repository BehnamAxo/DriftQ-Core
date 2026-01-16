package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"time"
)

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

func cmdRuns(baseURL string, timeout time.Duration, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("runs: missing subcommand (use: status|step|events|state)")
	}

	switch args[0] {
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

	case "diff":
		return runsDiff(baseURL, timeout, args[1:])

	default:
		return fmt.Errorf("runs: unknown subcommand %q (use: status|step|events|state)", args[0])
	}
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

func runsDiff(baseURL string, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("runs diff", flag.ContinueOnError)
	runID := fs.String("run-id", "", "run id (required)")
	nodeID := fs.String("node-id", "", "node id (required)")
	from := fs.Int("from", 0, "from attempt (optional)")
	to := fs.Int("to", 0, "to attempt (optional)")
	raw := fs.Bool("raw", false, "print raw JSON for both attempts")

	if err := fs.Parse(args); err != nil {
		return err
	}

	rid := strings.TrimSpace(*runID)
	nid := strings.TrimSpace(*nodeID)
	if rid == "" {
		return fmt.Errorf("runs diff: --run-id is required")
	}

	if nid == "" {
		return fmt.Errorf("runs diff: --node-id is required")
	}

	state, err := fetchRunState(baseURL, timeout, rid)
	if err != nil {
		return err
	}

	var steps []nodeExec
	for _, ne := range state.Nodes {
		if ne.NodeID == nid {
			steps = append(steps, ne)
		}
	}

	if len(steps) == 0 {
		return fmt.Errorf("runs diff: no executions found for node %q in run %q", nid, rid)
	}

	sort.Slice(steps, func(i, j int) bool { return steps[i].Attempt < steps[j].Attempt })

	var a, b *nodeExec

	// pick attempts
	if *from > 0 || *to > 0 {
		if *from <= 0 || *to <= 0 {
			return fmt.Errorf("runs diff: if you set one of --from/--to, you must set both")
		}

		for i := range steps {
			if steps[i].Attempt == *from {
				a = &steps[i]
			}

			if steps[i].Attempt == *to {
				b = &steps[i]
			}
		}

		if a == nil || b == nil {
			return fmt.Errorf("runs diff: attempt not found (have attempts: %s)", attemptList(steps))
		}
	} else {
		if len(steps) < 2 {
			return fmt.Errorf("runs diff: need at least 2 attempts to diff (have attempts: %s)", attemptList(steps))
		}

		a = &steps[len(steps)-2]
		b = &steps[len(steps)-1]
	}

	// header
	fmt.Printf("run_id=%s node_id=%s diff attempt %d -> %d\n", rid, nid, a.Attempt, b.Attempt)

	printIfChanged("status", a.Status, b.Status)
	printIfChanged("error", strings.TrimSpace(a.Error), strings.TrimSpace(b.Error))

	adur := durationStr(a.StartedAt, a.EndedAt)
	bdur := durationStr(b.StartedAt, b.EndedAt)
	if adur != bdur {
		fmt.Printf("dur: %s -> %s\n", adur, bdur)
	}

	printJSONDiff("input", a.Input, b.Input)
	printJSONDiff("output", a.Output, b.Output)

	if *raw {
		fmt.Println("\n--- attempt", a.Attempt, "raw ---")
		fmt.Println(prettyJSON(*a))
		fmt.Println("\n--- attempt", b.Attempt, "raw ---")
		fmt.Println(prettyJSON(*b))
	}

	return nil
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

func attemptList(xs []nodeExec) string {
	var a []string
	for _, x := range xs {
		a = append(a, fmt.Sprintf("%d", x.Attempt))
	}

	return strings.Join(a, ",")
}

func durationStr(s, e *time.Time) string {
	if s == nil || e == nil {
		return "-"
	}

	return e.Sub(*s).String()
}

func printIfChanged(label, a, b string) {
	if a == b {
		return
	}

	// keep it obvious for CLI users
	if a == "" {
		a = "(empty)"
	}

	if b == "" {
		b = "(empty)"
	}
	fmt.Printf("%s: %s -> %s\n", label, a, b)
}

func printJSONDiff(label string, a, b any) {
	aj := normalizeJSON(a)
	bj := normalizeJSON(b)

	if bytes.Equal(aj, bj) {
		return
	}

	var am map[string]any
	var bm map[string]any
	if json.Unmarshal(aj, &am) == nil && json.Unmarshal(bj, &bm) == nil {
		lines := diffMaps(am, bm, label)
		if len(lines) == 0 {
			return
		}
		for _, ln := range lines {
			fmt.Println(ln)
		}
		return
	}

	fmt.Printf("%s: %s -> %s\n", label, strings.TrimSpace(string(aj)), strings.TrimSpace(string(bj)))
}

func normalizeJSON(v any) []byte {
	if v == nil {
		return []byte("null")
	}

	b, err := json.Marshal(v)
	if err != nil {
		// fallback to fmt
		return []byte(fmt.Sprintf("%v", v))
	}

	return b
}

func diffMaps(a, b map[string]any, prefix string) []string {
	keys := map[string]struct{}{}
	for k := range a {
		keys[k] = struct{}{}
	}

	for k := range b {
		keys[k] = struct{}{}
	}

	var all []string
	for k := range keys {
		av, aok := a[k]
		bv, bok := b[k]

		switch {
		case aok && !bok:
			all = append(all, fmt.Sprintf("%s.%s: %s -> (missing)", prefix, k, strings.TrimSpace(string(normalizeJSON(av)))))
		case !aok && bok:
			all = append(all, fmt.Sprintf("%s.%s: (missing) -> %s", prefix, k, strings.TrimSpace(string(normalizeJSON(bv)))))
		default:
			aj := strings.TrimSpace(string(normalizeJSON(av)))
			bj := strings.TrimSpace(string(normalizeJSON(bv)))
			if aj != bj {
				all = append(all, fmt.Sprintf("%s.%s: %s -> %s", prefix, k, aj, bj))
			}
		}
	}

	sort.Strings(all)
	return all
}

func prettyJSON(v any) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}
