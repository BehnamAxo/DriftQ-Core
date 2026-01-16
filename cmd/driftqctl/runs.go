package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strconv"
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

func cmdRuns(baseURL string, timeout time.Duration, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf(`runs: missing subcommand
use:
  runs show   --run-id <id>      (raw JSON summary)
  runs status --run-id <id>      (compact table)
  runs events --run-id <id>      (timeline from /debug/run-state)
`)
	}

	switch args[0] {
	case "show", "get":
		fs := flag.NewFlagSet("runs show", flag.ContinueOnError)
		runID := fs.String("run-id", "", "run id (required)")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if strings.TrimSpace(*runID) == "" {
			return fmt.Errorf("runs show: --run-id is required")
		}
		return runsShow(baseURL, timeout, *runID)

	case "status":
		return runsStatus(baseURL, timeout, args[1:])

	case "events", "timeline", "tl":
		return runsEvents(baseURL, timeout, args[1:])

	default:
		return fmt.Errorf(`runs: unknown subcommand %q
use:
  runs show|get
  runs status
  runs events|timeline|tl
`, args[0])
	}
}

func runsShow(baseURL string, timeout time.Duration, runID string) error {
	path := "/debug/run?run_id=" + url.QueryEscape(strings.TrimSpace(runID))

	resp, err := doGET(baseURL, timeout, path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("runs show failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
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

	// status uses the smaller /debug/run endpoint (summary nodes)
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

	var out struct {
		Run   map[string]any  `json:"run"`
		Nodes []nodeStatusRow `json:"nodes"`
	}
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
		if len(errStr) > 60 {
			errStr = errStr[:57] + "..."
		}

		fmt.Printf("%s\t%d\t%s\t%s\t%d\t%d\t%s\n",
			n.NodeID, n.Attempt, n.Status, dur, n.InputBytes, n.OutputBytes, errStr,
		)
	}

	return nil
}

func runsEvents(baseURL string, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("runs events", flag.ContinueOnError)
	runID := fs.String("run-id", "", "run id (required)")
	raw := fs.Bool("raw", false, "print raw events JSON")
	limit := fs.Int("n", 0, "limit number of events printed (0 = no limit)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	id := strings.TrimSpace(*runID)
	if id == "" {
		return fmt.Errorf("runs events: --run-id is required")
	}

	// events come from the big dump endpoint
	path := "/debug/run-state?run_id=" + url.QueryEscape(id)
	resp, err := doGET(baseURL, timeout, path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("runs events failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var state map[string]any
	if err := json.Unmarshal(body, &state); err != nil {
		fmt.Println(strings.TrimSpace(string(body)))
		return nil
	}

	evAny, _ := state["events"]
	events, _ := evAny.([]any)
	if len(events) == 0 {
		fmt.Println("(no events)")
		return nil
	}

	if *raw {
		pretty, _ := json.MarshalIndent(events, "", "  ")
		fmt.Println(string(pretty))
		return nil
	}

	rows := make([]eventRow, 0, len(events))
	for _, e := range events {
		r, ok := toEventRow(e)
		if !ok {
			continue
		}
		rows = append(rows, r)
	}

	// If we got parseable timestamps, sort by time; otherwise keep insertion order.
	sort.SliceStable(rows, func(i, j int) bool {
		ti := rows[i].t
		tj := rows[j].t
		if ti.IsZero() || tj.IsZero() {
			return false
		}
		return ti.Before(tj)
	})

	if *limit > 0 && *limit < len(rows) {
		rows = rows[:*limit]
	}

	fmt.Println("TIME\tTYPE\tNODE\tATT\tDETAIL")
	for _, r := range rows {
		ts := r.timeStr
		if !r.t.IsZero() {
			ts = r.t.Format(time.RFC3339Nano)
		}
		if ts == "" {
			ts = "-"
		}
		node := r.node
		if node == "" {
			node = "-"
		}
		att := "-"
		if r.attempt >= 0 {
			att = strconv.Itoa(r.attempt)
		}
		typ := r.typ
		if typ == "" {
			typ = "-"
		}
		detail := strings.TrimSpace(r.detail)
		if len(detail) > 120 {
			detail = detail[:117] + "..."
		}
		fmt.Printf("%s\t%s\t%s\t%s\t%s\n", ts, typ, node, att, detail)
	}

	return nil
}

type eventRow struct {
	t       time.Time
	timeStr string
	typ     string
	node    string
	attempt int // -1 if unknown
	detail  string
}

func toEventRow(e any) (eventRow, bool) {
	m, ok := e.(map[string]any)
	if !ok {
		return eventRow{}, false
	}

	// common-ish keys (we don’t know your exact event struct, so we’re flexible)
	timeStr := pickString(m, "ts", "time", "at", "created_at", "occurred_at", "timestamp")
	typ := pickString(m, "type", "event_type", "kind", "name")
	node := pickString(m, "node_id", "node", "step_id", "step", "task")
	attempt := pickInt(m, "attempt", "try", "step_attempt", "node_attempt")

	detail := pickString(m, "message", "reason", "status", "state", "error", "note")
	if detail == "" {
		// if the event has a nested blob, try to keep it short but useful
		if d, ok := m["data"]; ok {
			b, _ := json.Marshal(d)
			detail = string(b)
		} else {
			// last resort: encode entire event
			b, _ := json.Marshal(m)
			detail = string(b)
		}
	}

	t := parseAnyTime(m, timeStr)

	return eventRow{
		t:       t,
		timeStr: timeStr,
		typ:     typ,
		node:    node,
		attempt: attempt,
		detail:  detail,
	}, true
}

func parseAnyTime(m map[string]any, s string) time.Time {
	s = strings.TrimSpace(s)
	if s != "" {
		// try common RFC formats
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t
		}
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			return t
		}
	}

	// maybe the JSON used a numeric timestamp field
	for _, k := range []string{"ts", "timestamp", "time"} {
		v, ok := m[k]
		if !ok {
			continue
		}
		switch vv := v.(type) {
		case float64:
			// seconds vs ms heuristic
			if vv > 1e12 {
				return time.UnixMilli(int64(vv))
			}
			if vv > 1e9 {
				return time.Unix(int64(vv), 0)
			}
		case int64:
			if vv > 1e12 {
				return time.UnixMilli(vv)
			}
			if vv > 1e9 {
				return time.Unix(vv, 0)
			}
		}
	}

	return time.Time{}
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
		v, ok := m[k]
		if !ok {
			continue
		}
		switch vv := v.(type) {
		case float64:
			return int(vv)
		case int:
			return vv
		case int64:
			return int(vv)
		case string:
			n, err := strconv.Atoi(strings.TrimSpace(vv))
			if err == nil {
				return n
			}
		}
	}
	return -1
}
