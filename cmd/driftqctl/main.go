package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
		return fmt.Errorf("topics: missing subcommand (use: list|ls|create)")
	}

	switch args[0] {
	case "list", "ls":
		return topicsList(baseURL, timeout)

	case "create":
		fs := flag.NewFlagSet("topics create", flag.ContinueOnError)
		name := fs.String("name", "", "topic name")
		partitions := fs.Int("partitions", 1, "number of partitions")

		if err := fs.Parse(args[1:]); err != nil {
			return err
		}

		if strings.TrimSpace(*name) == "" {
			return fmt.Errorf("topics create: --name is required")
		}

		if *partitions < 1 {
			return fmt.Errorf("topics create: --partitions must be >= 1")
		}

		path := fmt.Sprintf("/v1/topics?name=%s&partitions=%d", url.QueryEscape(*name), *partitions)
		resp, err := doPOST(baseURL, timeout, path)

		if err != nil {
			return err
		}

		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("topics create failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		}

		fmt.Printf("created topic %q partitions=%d\n", *name, *partitions)
		return nil

	default:
		return fmt.Errorf("topics: unknown subcommand %q (use: list|ls|create)", args[0])
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

func doPOST(baseURL string, timeout time.Duration, path string) (*http.Response, error) {
	req, err := http.NewRequest(
		http.MethodPost,
		strings.TrimRight(baseURL, "/")+path,
		nil,
	)

	if err != nil {
		return nil, err
	}

	// if CLI timeout provided
	client := http.DefaultClient
	if timeout > 0 {
		client = &http.Client{Timeout: timeout}
	}

	return client.Do(req)
}
