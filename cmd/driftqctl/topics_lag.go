package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"time"
)

type lagRow struct {
	Group           string `json:"group"`
	Topic           string `json:"topic"`
	Partition       int    `json:"partition"`
	HeadOffset      int64  `json:"head_offset"`
	CommittedOffset int64  `json:"committed_offset"`
	Inflight        int64  `json:"inflight"`
	Lag             int64  `json:"lag"`
}

type lagResp struct {
	Ok    bool     `json:"ok"`
	Group string   `json:"group"`
	Topic string   `json:"topic"`
	Rows  []lagRow `json:"rows"`
}

func topicsLag(baseURL string, timeout time.Duration, args []string) error {
	fs := flag.NewFlagSet("topics lag", flag.ContinueOnError)
	group := fs.String("group", "", "consumer group (required)")
	topic := fs.String("topic", "", "topic (optional)")
	partition := fs.Int("partition", -1, "partition (optional)")
	raw := fs.Bool("raw", false, "print raw JSON response")

	if err := fs.Parse(args); err != nil {
		return err
	}

	g := strings.TrimSpace(*group)
	if g == "" {
		return fmt.Errorf("topics lag: --group is required")
	}

	q := url.Values{}
	q.Set("group", g)

	t := strings.TrimSpace(*topic)
	if t != "" {
		q.Set("topic", t)
	}

	if *partition >= 0 {
		q.Set("partition", fmt.Sprintf("%d", *partition))
	}

	path := "/debug/topics/lag?" + q.Encode()
	resp, err := doGET(baseURL, timeout, path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("topics lag failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	if *raw {
		fmt.Println(strings.TrimSpace(string(body)))
		return nil
	}

	var out lagResp
	if err := json.Unmarshal(body, &out); err != nil {
		fmt.Println(strings.TrimSpace(string(body)))
		return nil
	}

	if len(out.Rows) == 0 {
		fmt.Println("(no lag rows)")
		return nil
	}

	sort.Slice(out.Rows, func(i, j int) bool {
		if out.Rows[i].Topic != out.Rows[j].Topic {
			return out.Rows[i].Topic < out.Rows[j].Topic
		}
		return out.Rows[i].Partition < out.Rows[j].Partition
	})

	fmt.Println("TOPIC\tPART\tHEAD\tCOMMIT\tINFLIGHT\tLAG")
	for _, r := range out.Rows {
		fmt.Printf("%s\t%d\t%d\t%d\t%d\t%d\n",
			r.Topic, r.Partition, r.HeadOffset, r.CommittedOffset, r.Inflight, r.Lag,
		)
	}

	return nil
}
