package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
)

type runArtifactsResp struct {
	Ok        bool          `json:"ok"`
	RunID     string        `json:"run_id"`
	Count     int           `json:"count"`
	Artifacts []runArtifact `json:"artifacts"`
}

type runArtifact struct {
	ArtifactID   string            `json:"artifact_id"`
	Sha256       string            `json:"sha256"`
	ContentType  string            `json:"content_type,omitempty"`
	Size         int64             `json:"size"`
	CreatedAt    time.Time         `json:"created_at"`
	RunID        string            `json:"run_id,omitempty"`
	WorkflowID   string            `json:"workflow_id,omitempty"`
	NodeID       string            `json:"node_id,omitempty"`
	Attempt      int               `json:"attempt,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
	Description  string            `json:"description,omitempty"`
	OriginalName string            `json:"original_name,omitempty"`
}

func runsArtifacts(baseURL string, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("runs artifacts", flag.ContinueOnError)
	runID := fs.String("run-id", "", "run id (required)")
	nodeID := fs.String("node-id", "", "optional node id filter")
	limit := fs.Int("limit", 50, "max artifacts to return")
	raw := fs.Bool("raw", false, "print raw JSON response")

	if err := fs.Parse(args); err != nil {
		return err
	}

	id := strings.TrimSpace(*runID)
	if id == "" {
		return fmt.Errorf("runs artifacts: --run-id is required")
	}

	if *limit <= 0 {
		*limit = 50
	}

	path := "/debug/run-artifacts?run_id=" + url.QueryEscape(id) + "&limit=" + url.QueryEscape(fmt.Sprintf("%d", *limit))
	resp, err := doGET(baseURL, timeout, path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("runs artifacts failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	if *raw {
		fmt.Println(strings.TrimSpace(string(body)))
		return nil
	}

	var out runArtifactsResp
	if err := json.Unmarshal(body, &out); err != nil {
		// fallback: print body
		fmt.Println(strings.TrimSpace(string(body)))
		return nil
	}

	wantNode := strings.TrimSpace(*nodeID)

	arts := out.Artifacts
	if wantNode != "" {
		filtered := make([]runArtifact, 0, len(arts))
		for _, a := range arts {
			if strings.TrimSpace(a.NodeID) == wantNode {
				filtered = append(filtered, a)
			}
		}
		arts = filtered
	}

	if len(arts) == 0 {
		fmt.Println("(no artifacts found)")
		return nil
	}

	// stable-ish ordering: node, attempt, created_at, artifact_id
	sort.Slice(arts, func(i, j int) bool {
		if arts[i].NodeID != arts[j].NodeID {
			return arts[i].NodeID < arts[j].NodeID
		}
		if arts[i].Attempt != arts[j].Attempt {
			return arts[i].Attempt < arts[j].Attempt
		}
		if !arts[i].CreatedAt.Equal(arts[j].CreatedAt) {
			return arts[i].CreatedAt.Before(arts[j].CreatedAt)
		}
		return arts[i].ArtifactID < arts[j].ArtifactID
	})

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ARTIFACT_ID\tSIZE\tTYPE\tNODE\tATT\tCREATED\tNAME\tDESC")
	for _, a := range arts {
		ct := strings.TrimSpace(a.ContentType)
		if ct == "" {
			ct = "-"
		}

		name := strings.TrimSpace(a.OriginalName)
		if name == "" {
			name = "-"
		}

		desc := strings.TrimSpace(a.Description)
		if desc == "" {
			desc = "-"
		}

		created := "-"
		if !a.CreatedAt.IsZero() {
			created = a.CreatedAt.UTC().Format(time.RFC3339Nano)
		}

		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%d\t%s\t%s\t%s\n",
			a.ArtifactID, a.Size, ct, a.NodeID, a.Attempt, created, name, desc,
		)
	}
	_ = tw.Flush()

	return nil
}

func runsArtifactMeta(baseURL string, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("runs artifact-meta", flag.ContinueOnError)
	artifactID := fs.String("artifact-id", "", "artifact id (required)")
	raw := fs.Bool("raw", false, "print raw JSON response")

	if err := fs.Parse(args); err != nil {
		return err
	}

	id := strings.TrimSpace(*artifactID)
	if id == "" {
		return fmt.Errorf("runs artifact-meta: --artifact-id is required")
	}

	path := "/debug/artifact-meta?artifact_id=" + url.QueryEscape(id)
	resp, err := doGET(baseURL, timeout, path)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("artifact-meta failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
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

	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
	return nil
}

func runsArtifactGet(baseURL string, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("runs artifact-get", flag.ContinueOnError)
	artifactID := fs.String("artifact-id", "", "artifact id (required)")
	outPath := fs.String("out", "", "output file path (optional; default: sanitized artifact id)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	id := strings.TrimSpace(*artifactID)
	if id == "" {
		return fmt.Errorf("runs artifact-get: --artifact-id is required")
	}

	path := "/debug/artifact-get?artifact_id=" + url.QueryEscape(id)
	resp, err := doGET(baseURL, timeout, path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("artifact-get failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	dst := strings.TrimSpace(*outPath)
	if dst == "" {
		dst = defaultArtifactFilename(id)
	}

	f, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("cannot create %q: %w", dst, err)
	}

	defer f.Close()

	n, err := io.Copy(f, resp.Body)
	if err != nil {
		return fmt.Errorf("write %q failed: %w", dst, err)
	}

	ct := strings.TrimSpace(resp.Header.Get("Content-Type"))
	sha := strings.TrimSpace(resp.Header.Get("X-Artifact-Sha256"))
	fmt.Printf("wrote %d bytes -> %s", n, dst)
	if ct != "" {
		fmt.Printf(" content_type=%s", ct)
	}

	if sha != "" {
		fmt.Printf(" sha256=%s", sha)
	}

	fmt.Println()

	return nil
}

func defaultArtifactFilename(id string) string {
	s := strings.TrimSpace(id)
	if s == "" {
		return "artifact.bin"
	}

	repl := func(r rune) rune {
		switch r {
		case '\\', '/', ':', '*', '?', '"', '<', '>', '|':
			return '_'
		default:
			return r
		}
	}

	s = strings.Map(repl, s)
	return s
}
