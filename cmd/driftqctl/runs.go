package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
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

type debugRunResp struct {
	Run   map[string]any  `json:"run"`
	Nodes []nodeStatusRow `json:"nodes"`
}

func cmdRuns(baseURL string, timeout time.Duration, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("runs: missing subcommand (use: show|get|status)")
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

	default:
		return fmt.Errorf("runs: unknown subcommand %q (use: show|get|status)", args[0])
	}
}

func runsShow(baseURL string, timeout time.Duration, runID string) error {
	path := "/debug/run?run_id=" + url.QueryEscape(runID)

	resp, err := doGET(baseURL, timeout, path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("runs show failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	// pretty print JSON
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
		if len(errStr) > 60 {
			errStr = errStr[:57] + "..."
		}

		fmt.Printf("%s\t%d\t%s\t%s\t%d\t%d\t%s\n",
			n.NodeID, n.Attempt, n.Status, dur, n.InputBytes, n.OutputBytes, errStr,
		)
	}

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
