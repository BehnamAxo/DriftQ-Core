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
	"time"
)

func runsArtifacts(baseURL string, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("runs artifacts", flag.ContinueOnError)
	runID := fs.String("run-id", "", "run id (required)")
	nodeID := fs.String("node-id", "", "optional node id filter")
	raw := fs.Bool("raw", false, "print raw JSON list of artifact IDs")

	if err := fs.Parse(args); err != nil {
		return err
	}

	id := strings.TrimSpace(*runID)
	if id == "" {
		return fmt.Errorf("runs artifacts: --run-id is required")
	}

	root, err := fetchRunState(baseURL, timeout, id)
	if err != nil {
		return err
	}

	nodes, err := decodeNodesAsMaps(root)
	if err != nil {
		return err
	}

	wantNode := strings.TrimSpace(*nodeID)

	idsSet := map[string]struct{}{}
	for _, n := range nodes {
		if wantNode != "" {
			if pickString(n, "node_id", "nodeId", "NodeID") != wantNode {
				continue
			}
		}
		collectArtifactIDs(n, idsSet)
	}

	ids := make([]string, 0, len(idsSet))
	for k := range idsSet {
		ids = append(ids, k)
	}

	sort.Strings(ids)

	if *raw {
		b, _ := json.MarshalIndent(map[string]any{
			"ok":        true,
			"run_id":    id,
			"node_id":   wantNode,
			"count":     len(ids),
			"artifacts": ids,
		}, "", "  ")
		fmt.Println(string(b))
		return nil
	}

	if len(ids) == 0 {
		fmt.Println("(no artifacts found)")
		return nil
	}

	for _, a := range ids {
		fmt.Println(a)
	}
	return nil
}

func collectArtifactIDs(v any, out map[string]struct{}) {
	switch t := v.(type) {
	case map[string]any:
		for k, vv := range t {
			switch k {
			case "artifact_id", "artifactId":
				if s, ok := vv.(string); ok {
					s = strings.TrimSpace(s)
					if s != "" {
						out[s] = struct{}{}
					}
				}
				continue

			case "artifact_ids", "artifactIds":
				if arr, ok := vv.([]any); ok {
					for _, it := range arr {
						if s, ok := it.(string); ok {
							s = strings.TrimSpace(s)
							if s != "" {
								out[s] = struct{}{}
							}
						}
					}
				}
				// still recurse in case it is a weird shape
				collectArtifactIDs(vv, out)
				continue

			case "artifacts", "Artifacts":
				collectArtifactIDs(vv, out)
				continue
			}

			// general recursion
			collectArtifactIDs(vv, out)
		}

	case []any:
		for _, it := range t {
			collectArtifactIDs(it, out)
		}

	case string:
		// only capture strings when they are inside an artifacts key path, we don't know context here, so do nothing
		return

	default:
		return
	}
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
