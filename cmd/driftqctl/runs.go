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

func cmdRuns(baseURL string, timeout time.Duration, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("runs: missing subcommand (use: runs show)")
	}

	switch args[0] {
	case "show", "get", "status":
		fs := flag.NewFlagSet("runs show", flag.ContinueOnError)
		runID := fs.String("run-id", "", "run id")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if strings.TrimSpace(*runID) == "" {
			return fmt.Errorf("runs show: --run-id is required")
		}
		return runsShow(baseURL, timeout, *runID)

	default:
		return fmt.Errorf("runs: unknown subcommand %q (use: runs show)", args[0])
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

	// pretty print JSON stuff
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		// fallback raw
		fmt.Println(string(body))
		return nil
	}
	pretty, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(pretty))
	return nil
}
