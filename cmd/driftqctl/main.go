package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
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

	case "runs":
		if err := cmdRuns(*baseURL, *timeout, args[1:]); err != nil {
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
		topics list|ls                 List topics (GET /v1/topics)
		topics create --name T [--partitions N]   Create topic (POST /v1/topics?...)
		topics peek --topic T [--n N]             Peek messages (GET /v1/consume ...)
		runs status --run-id ID [--raw]          Show run + step status (GET /debug/run)
		runs list|ls [--limit N]            List recent runs (GET /debug/runs)

	Examples:
		driftqctl topics list
		driftqctl --base-url http://localhost:8080 topics create --name demo --partitions 1
		driftqctl --base-url http://localhost:8080 runs status --run-id demo-20260101T000000Z
	`)
}

func cmdTopics(baseURL string, timeout time.Duration, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("topics: missing subcommand (use: list|ls|create|peek)")
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

	case "peek":
		return topicsPeek(baseURL, timeout, args[1:])

	// case "runs":
	// 	// leaving this wiring as-is for now
	// 	return cmdRuns(baseURL, timeout, args[1:])

	default:
		return fmt.Errorf("topics: unknown subcommand %q (use: list|ls|create|peek)", args[0])
	}
}

func topicsList(baseURL string, timeout time.Duration) error {
	u := strings.TrimRight(baseURL, "/") + "/v1/topics"

	c := &http.Client{Timeout: timeout}
	req, err := http.NewRequest(http.MethodGet, u, nil)
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
		return fmt.Errorf("GET %s: %s\n%s", u, resp.Status, strings.TrimSpace(string(body)))
	}

	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		fmt.Println(strings.TrimSpace(string(body)))
		return nil
	}

	printTopics(v)
	return nil
}

func topicsPeek(baseURL string, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("topics peek", flag.ContinueOnError)
	topic := fs.String("topic", "", "topic name (required)")
	partition := fs.Int("partition", -1, "partition (default: all partitions)")
	n := fs.Int("n", 5, "max unique messages to print")
	leaseMS := fs.Int("lease-ms", 2000, "lease duration in ms (avoid very small values for peek)")
	waitMS := fs.Int("wait-ms", 750, "how long to wait for data before exiting as empty (ms)")
	group := fs.String("group", "_peek", "consumer group (default: _peek)")
	owner := fs.String("owner", "", "owner id (default: auto)")
	pretty := fs.Bool("pretty", false, "pretty-print each JSON line")
	allowRedelivery := fs.Bool("allow-redelivery", false, "print redelivered messages too (default: dedupe by offset)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	t := strings.TrimSpace(*topic)
	if t == "" {
		return fmt.Errorf("topics peek: --topic is required")
	}

	if *n < 1 {
		return fmt.Errorf("topics peek: --n must be >= 1")
	}

	if *leaseMS < 0 || *waitMS < 0 {
		return fmt.Errorf("topics peek: --lease-ms/--wait-ms must be >= 0")
	}

	o := strings.TrimSpace(*owner)
	if o == "" {
		o = defaultOwner()
	}

	q := url.Values{}
	q.Set("topic", t)
	q.Set("group", strings.TrimSpace(*group))
	q.Set("owner", o)

	if *leaseMS > 0 {
		q.Set("lease_ms", fmt.Sprintf("%d", *leaseMS))
	}

	if *partition >= 0 {
		q.Set("partition", fmt.Sprintf("%d", *partition))
	}

	path := "/v1/consume?" + q.Encode()
	fullURL := strings.TrimRight(baseURL, "/") + path

	// For peek: bail quickly if nothing shows up.
	clientTimeout := timeout
	if *waitMS > 0 {
		w := time.Duration(*waitMS) * time.Millisecond
		if clientTimeout <= 0 || w < clientTimeout {
			clientTimeout = w
		}
	}

	if clientTimeout <= 0 {
		clientTimeout = 5 * time.Second
	}

	client := &http.Client{Timeout: clientTimeout}
	req, err := http.NewRequest(http.MethodGet, fullURL, nil)

	if err != nil {
		return err
	}

	req.Header.Set("Accept", "application/x-ndjson")

	resp, err := client.Do(req)
	if err != nil {
		// treat timeout as empty
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			fmt.Println("(empty)")
			return nil
		}
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("GET %s: %s\n%s", fullURL, resp.Status, strings.TrimSpace(string(b)))
	}

	type consumeLine struct {
		Partition int   `json:"partition"`
		Offset    int64 `json:"offset"`
	}

	type key struct {
		p int
		o int64
	}

	seen := map[key]struct{}{}
	unique := 0

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}

		// Dedupe by (partition, offset) so we do not print redeliveries in the same peek
		if !*allowRedelivery {
			var m consumeLine
			if err := json.Unmarshal([]byte(line), &m); err == nil {
				k := key{p: m.Partition, o: m.Offset}
				if _, ok := seen[k]; ok {
					break
				}

				seen[k] = struct{}{}
			}
		}

		unique++

		if *pretty {
			var v any
			if err := json.Unmarshal([]byte(line), &v); err != nil {
				fmt.Println(line)
			} else {
				b, _ := json.MarshalIndent(v, "", "  ")
				fmt.Println(string(b))
			}
		} else {
			fmt.Println(line)
		}

		if unique >= *n {
			break
		}
	}

	if unique == 0 {
		fmt.Println("(empty)")
		return nil
	}

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

	client := http.DefaultClient
	if timeout > 0 {
		client = &http.Client{Timeout: timeout}
	}

	return client.Do(req)
}

func doGET(baseURL string, timeout time.Duration, path string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(baseURL, "/")+path, nil)
	if err != nil {
		return nil, err
	}

	client := http.DefaultClient
	if timeout > 0 {
		client = &http.Client{Timeout: timeout}
	}

	return client.Do(req)
}

func defaultOwner() string {
	host, _ := os.Hostname()
	host = strings.TrimSpace(host)

	if host == "" {
		host = "host"
	}

	return fmt.Sprintf("%s-%d-%d", host, os.Getpid(), time.Now().UnixNano())
}
