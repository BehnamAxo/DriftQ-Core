package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const defaultBaseURL = "http://localhost:8080"

func main() {
	baseURL := flag.String("base-url", defaultBaseURL, "DriftQ base URL (e.g. http://localhost:8080)")
	timeout := flag.Duration("timeout", 5*time.Second, "HTTP timeout (e.g. 2s, 500ms)")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	switch args[0] {
	case "topics":
		if err := cmdTopics(*baseURL, *timeout, args[1:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}

	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Println(`driftqctl [--base-url URL] <command> [args]

	Commands:
		topics list|ls   List topics (GET /v1/topics)

	Examples:
		driftqctl topics list
		driftqctl --base-url http://localhost:8080 topics ls
	`)
}

func cmdTopics(baseURL string, timeout time.Duration, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("topics: missing subcommand (use: topics list|ls)")
	}

	switch args[0] {
	case "list", "ls":
		return topicsList(baseURL, timeout)
	default:
		return fmt.Errorf("topics: unknown subcommand %q (use: list|ls)", args[0])
	}
}

func topicsList(baseURL string, timeout time.Duration) error {
	url := strings.TrimRight(baseURL, "/") + "/v1/topics"

	c := &http.Client{Timeout: timeout}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// This is super useful for debugging
		return fmt.Errorf("GET %s: %s\n%s", url, resp.Status, strings.TrimSpace(string(body)))
	}

	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		fmt.Println(strings.TrimSpace(string(body)))
		return nil
	}

	printTopics(v)
	return nil
}

func printTopics(v any) {
	var items []any

	switch vv := v.(type) {
	case []any:
		items = vv
	case map[string]any:
		if t, ok := vv["topics"].([]any); ok {
			items = t
		} else {
			b, _ := json.MarshalIndent(vv, "", "  ")
			fmt.Println(string(b))
			return
		}
	default:
		b, _ := json.MarshalIndent(v, "", "  ")
		fmt.Println(string(b))
		return
	}

	if len(items) == 0 {
		fmt.Println("(no topics)")
		return
	}

	allStrings := true
	for _, it := range items {
		if _, ok := it.(string); !ok {
			allStrings = false
			break
		}
	}
	if allStrings {
		for _, it := range items {
			fmt.Println(it.(string))
		}
		return
	}

	b, _ := json.MarshalIndent(items, "", "  ")
	fmt.Println(string(b))
}
