package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type safeBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

type driftqdProc struct {
	cmd     *exec.Cmd
	baseURL string
	logs    *safeBuffer
}

func TestDriftqdMainHelperProcess(t *testing.T) {
	if os.Getenv("DRIFTQD_MAIN_HELPER") != "1" {
		t.Skip("helper process")
	}

	sep := -1
	for i, a := range os.Args {
		if a == "--" {
			sep = i
			break
		}
	}

	if sep < 0 || sep+1 >= len(os.Args) {
		os.Exit(2)
	}

	os.Args = append([]string{os.Args[0]}, os.Args[sep+1:]...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	main()
	os.Exit(0)
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func (p *driftqdProc) stop() {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}

	_ = p.cmd.Process.Kill()
	_, _ = p.cmd.Process.Wait()
}

func pickFreeAddr(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	defer ln.Close()

	return ln.Addr().String()
}

func lastFlagValue(args []string, name string, fallback string) string {
	for i := len(args) - 2; i >= 0; i-- {
		if args[i] == name {
			return args[i+1]
		}
	}
	return fallback
}

func startDriftqdProc(t *testing.T, extraArgs ...string) *driftqdProc {
	t.Helper()

	addr := pickFreeAddr(t)
	defaultWAL := filepath.Join(t.TempDir(), "driftq.broker.wal")

	allArgs := append([]string{"-addr", addr, "-wal", defaultWAL}, extraArgs...)
	finalAddr := lastFlagValue(allArgs, "-addr", addr)
	if strings.HasPrefix(finalAddr, ":") {
		finalAddr = "127.0.0.1" + finalAddr
	}

	logs := &safeBuffer{}
	cmdArgs := []string{"-test.run=^TestDriftqdMainHelperProcess$", "--"}
	cmdArgs = append(cmdArgs, allArgs...)

	cmd := exec.Command(os.Args[0], cmdArgs...)
	cmd.Env = append(os.Environ(), "DRIFTQD_MAIN_HELPER=1")
	cmd.Stdout = logs
	cmd.Stderr = logs

	if err := cmd.Start(); err != nil {
		t.Fatalf("start driftqd helper: %v", err)
	}

	p := &driftqdProc{
		cmd:     cmd,
		baseURL: "http://" + finalAddr,
		logs:    logs,
	}

	t.Cleanup(func() { p.stop() })

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(p.baseURL + "/v1/healthz")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return p
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	p.stop()
	t.Fatalf("driftqd did not become healthy at %s\nlogs:\n%s", p.baseURL, p.logs.String())
	return nil
}

func postURL(t *testing.T, rawURL string) (int, []byte, http.Header) {
	t.Helper()

	resp, err := http.Post(rawURL, "", nil)
	if err != nil {
		t.Fatalf("POST %s: %v", rawURL, err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, body, resp.Header
}

func mustCreateTopic(t *testing.T, baseURL, topic string, partitions int) {
	t.Helper()

	u := fmt.Sprintf("%s/v1/topics?name=%s&partitions=%d", baseURL, topic, partitions)
	status, body, _ := postURL(t, u)
	if status != http.StatusCreated {
		t.Fatalf("create topic status=%d body=%s", status, string(body))
	}
}

func mustProduceStatus(t *testing.T, baseURL, topic, value string, want int) (int, string) {
	t.Helper()

	u := fmt.Sprintf("%s/v1/produce?topic=%s&value=%s", baseURL, topic, value)
	status, body, _ := postURL(t, u)
	if status != want {
		t.Fatalf("produce status=%d want=%d body=%s", status, want, string(body))
	}
	return status, string(body)
}

func postJSON(t *testing.T, rawURL string, body string) (int, []byte, http.Header) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, rawURL, strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest POST %s: %v", rawURL, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", rawURL, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b, resp.Header
}

func consumeOneNDJSONLine(t *testing.T, baseURL, topic, group, owner string, leaseMs int, timeout time.Duration) (string, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	u := fmt.Sprintf("%s/v1/consume?topic=%s&group=%s&owner=%s&lease_ms=%d", baseURL, topic, group, owner, leaseMs)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)

	if err != nil {
		t.Fatalf("NewRequest consume: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/consume: %v", err)
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("consume status=%d body=%s", resp.StatusCode, string(b))
	}

	sc := bufio.NewScanner(resp.Body)
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			t.Fatalf("scan consume: %v", err)
		}
		t.Fatalf("expected one consumed line from topic=%s group=%s", topic, group)
	}

	return sc.Text(), resp.StatusCode
}

func assertNoConsumeLineWithin(t *testing.T, baseURL, topic, group, owner string, leaseMs int, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	defer cancel()
	u := fmt.Sprintf("%s/v1/consume?topic=%s&group=%s&owner=%s&lease_ms=%d", baseURL, topic, group, owner, leaseMs)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)

	if err != nil {
		t.Fatalf("NewRequest consume: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// Expected when no message arrives before timeout.
		if errors.Is(err, context.DeadlineExceeded) {
			return
		}
		t.Fatalf("GET /v1/consume: %v", err)
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("consume status=%d body=%s", resp.StatusCode, string(b))
	}

	sc := bufio.NewScanner(resp.Body)
	if sc.Scan() {
		t.Fatalf("expected no message, got line=%s", sc.Text())
	}

	if err := sc.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		// Scanner typically returns nil when the request context cancels and the body closes.
		t.Fatalf("unexpected scanner error: %v", err)
	}
}

func TestMainFlags_ResetWAL_CreatesBackupAndNewWAL(t *testing.T) {
	tmp := t.TempDir()
	walPath := filepath.Join(tmp, "broker.wal")

	if err := os.WriteFile(walPath, []byte("old-wal"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_ = startDriftqdProc(t, "-wal", walPath, "-reset-wal")

	matches, err := filepath.Glob(walPath + ".bak.*")
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}

	if len(matches) != 1 {
		t.Fatalf("expected 1 wal backup, got %d (%v)", len(matches), matches)
	}

	bakBytes, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("ReadFile backup: %v", err)
	}

	if string(bakBytes) != "old-wal" {
		t.Fatalf("backup content mismatch: got %q", string(bakBytes))
	}

	if _, err := os.Stat(walPath); err != nil {
		t.Fatalf("new wal should exist at %s: %v", walPath, err)
	}
}

func TestMainFlags_EngineStoreFile_EngineWAL_ArtifactsDir(t *testing.T) {
	tmp := t.TempDir()
	engineWAL := filepath.Join(tmp, "engine.wal")
	artifactsDir := filepath.Join(tmp, "artifacts")

	_ = startDriftqdProc(t,
		"-engine-store", "file",
		"-engine-wal", engineWAL,
		"-artifacts-dir", artifactsDir,
	)

	if st, err := os.Stat(engineWAL); err != nil || st.IsDir() {
		t.Fatalf("engine WAL file missing at %s (err=%v)", engineWAL, err)
	}

	if st, err := os.Stat(artifactsDir); err != nil || !st.IsDir() {
		t.Fatalf("artifacts dir missing at %s (err=%v)", artifactsDir, err)
	}
}

func TestMainFlags_LogFormatJSON_AndLogLevelError(t *testing.T) {
	t.Run("json format emits structured logs", func(t *testing.T) {
		p := startDriftqdProc(t, "-log-format", "json", "-log-level", "info")

		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			out := p.logs.String()
			if strings.Contains(out, `"service":"driftqd"`) && strings.Contains(out, `"msg":"broker starting"`) {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}

		t.Fatalf("did not observe expected json startup logs\nlogs:\n%s", p.logs.String())
	})

	t.Run("error level suppresses info logs", func(t *testing.T) {
		p := startDriftqdProc(t, "-log-format", "json", "-log-level", "error")

		// Give it a short moment to emit anything it wants.
		time.Sleep(200 * time.Millisecond)

		out := p.logs.String()
		if strings.Contains(out, `"msg":"broker starting"`) {
			t.Fatalf("unexpected info log at error level\nlogs:\n%s", out)
		}
	})
}

func TestMainFlags_MaxPartitionMsgs_Enforced(t *testing.T) {
	p := startDriftqdProc(t, "-max-partition-msgs", "2")

	mustCreateTopic(t, p.baseURL, "msg-limit", 1)
	mustProduceStatus(t, p.baseURL, "msg-limit", "a", http.StatusOK)
	mustProduceStatus(t, p.baseURL, "msg-limit", "b", http.StatusOK)

	status, body := mustProduceStatus(t, p.baseURL, "msg-limit", "c", http.StatusTooManyRequests)
	if status != http.StatusTooManyRequests || !strings.Contains(body, "RESOURCE_EXHAUSTED") {
		t.Fatalf("expected RESOURCE_EXHAUSTED 429, got status=%d body=%s", status, body)
	}
}

func TestMainFlags_MaxPartitionBytes_Enforced(t *testing.T) {
	p := startDriftqdProc(t, "-max-partition-bytes", "10")

	mustCreateTopic(t, p.baseURL, "byte-limit", 1)
	mustProduceStatus(t, p.baseURL, "byte-limit", "12345", http.StatusOK)

	status, body := mustProduceStatus(t, p.baseURL, "byte-limit", "67890", http.StatusTooManyRequests)
	if status != http.StatusTooManyRequests || !strings.Contains(body, "RESOURCE_EXHAUSTED") {
		t.Fatalf("expected RESOURCE_EXHAUSTED 429, got status=%d body=%s", status, body)
	}
}

func TestMainFlags_MaxInFlight_EnforcedUntilAck(t *testing.T) {
	p := startDriftqdProc(t, "-max-inflight", "1")
	mustCreateTopic(t, p.baseURL, "if-limit", 1)

	mustProduceStatus(t, p.baseURL, "if-limit", "m1", http.StatusOK)
	mustProduceStatus(t, p.baseURL, "if-limit", "m2", http.StatusOK)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	consumeURL := p.baseURL + "/v1/consume?topic=if-limit&group=g1&owner=o1&lease_ms=5000"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, consumeURL, nil)
	if err != nil {
		t.Fatalf("NewRequest consume: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/consume: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("consume status=%d body=%s", resp.StatusCode, string(body))
	}

	type consumeItem struct {
		Partition int   `json:"partition"`
		Offset    int64 `json:"offset"`
		Value     string
	}

	sc := bufio.NewScanner(resp.Body)
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			t.Fatalf("scan first: %v", err)
		}
		t.Fatal("expected first message")
	}

	var first consumeItem
	if err := json.Unmarshal(sc.Bytes(), &first); err != nil {
		t.Fatalf("unmarshal first: %v line=%s", err, string(sc.Bytes()))
	}

	if first.Value != "m1" {
		t.Fatalf("expected first value m1, got %q", first.Value)
	}

	type scanRes struct {
		line string
		err  error
		ok   bool
	}

	readCh := make(chan scanRes, 1)
	go func() {
		if sc.Scan() {
			readCh <- scanRes{line: sc.Text(), ok: true}
			return
		}
		readCh <- scanRes{err: sc.Err(), ok: false}
	}()

	select {
	case r := <-readCh:
		if r.ok {
			t.Fatalf("received second message before ack (max-inflight broken): %s", r.line)
		}
		if r.err != nil {
			t.Fatalf("scan second before ack err: %v", r.err)
		}
		t.Fatal("stream ended before ack")
	case <-time.After(300 * time.Millisecond):
		// expected: second message should be blocked until ack
	}

	ackURL := fmt.Sprintf("%s/v1/ack?topic=if-limit&group=g1&owner=o1&partition=%d&offset=%d", p.baseURL, first.Partition, first.Offset)
	status, body, _ := postURL(t, ackURL)
	if status != http.StatusNoContent {
		t.Fatalf("ack status=%d body=%s", status, string(body))
	}

	select {
	case r := <-readCh:
		if !r.ok {
			t.Fatalf("expected second message after ack, err=%v", r.err)
		}

		var second consumeItem
		if err := json.Unmarshal([]byte(r.line), &second); err != nil {
			t.Fatalf("unmarshal second: %v line=%s", err, r.line)
		}

		if second.Value != "m2" {
			t.Fatalf("expected second value m2, got %q", second.Value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for second message after ack")
	}
}

func TestMainFlags_Addr_UsesRequestedBindAddress(t *testing.T) {
	addr := pickFreeAddr(t)
	p := startDriftqdProc(t, "-addr", addr)

	resp, err := http.Get("http://" + addr + "/v1/healthz")
	if err != nil {
		t.Fatalf("health at requested addr: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, string(body))
	}

	if p.baseURL != "http://"+addr {
		t.Fatalf("baseURL mismatch: got %s want %s", p.baseURL, "http://"+addr)
	}
}

func TestMainFlags_MultiagentConfig_BootstrapAndRouter(t *testing.T) {
	cfgJSON := `{
  "agents": ["planner"],
  "teams": ["core"],
  "capabilities": { "coding": ["coder-a"] },
  "topic_partitions": 1,
  "router_strict": false,
  "source_topics": ["agent-ingress"]
}`
	cfgPath := filepath.Join(t.TempDir(), "multiagent.json")
	if err := os.WriteFile(cfgPath, []byte(cfgJSON), 0o644); err != nil {
		t.Fatalf("WriteFile config: %v", err)
	}

	p := startDriftqdProc(t,
		"-multiagent-config", cfgPath,
		"-bootstrap-multiagent-topics",
	)

	// Bootstrapped topics should exist (union of explicit agents + capability agents + teams)
	resp, err := http.Get(p.baseURL + "/v1/topics")
	if err != nil {
		t.Fatalf("GET /v1/topics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("topics status=%d body=%s", resp.StatusCode, string(body))
	}

	var list struct {
		Topics []string `json:"topics"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("decode topics: %v", err)
	}

	wantTopics := map[string]bool{
		"agent.planner.inbox":  true,
		"agent.planner.outbox": true,
		"agent.coder-a.inbox":  true,
		"agent.coder-a.outbox": true,
		"team.core.broadcast":  true,
	}
	for _, tname := range list.Topics {
		delete(wantTopics, tname)
	}
	if len(wantTopics) != 0 {
		t.Fatalf("missing bootstrapped topics: %v (all=%v)", wantTopics, list.Topics)
	}

	// Router source topic itself is not auto-created; create it and verify capability routing.
	mustCreateTopic(t, p.baseURL, "agent-ingress", 1)

	agentMsg := `{"sender":"planner","capability":"coding","intent":"implement_pr","payload":{"pr":3}}`
	reqBody := fmt.Sprintf(`{"topic":"agent-ingress","value":%q}`, agentMsg)

	req, err := http.NewRequest(http.MethodPost, p.baseURL+"/v1/produce", strings.NewReader(reqBody))
	if err != nil {
		t.Fatalf("NewRequest produce: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	prodResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/produce: %v", err)
	}
	defer prodResp.Body.Close()

	if prodResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(prodResp.Body)
		t.Fatalf("produce status=%d body=%s", prodResp.StatusCode, string(body))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	consumeURL := p.baseURL + "/v1/consume?topic=agent.coder-a.inbox&group=g1&owner=o1&lease_ms=5000"
	creq, err := http.NewRequestWithContext(ctx, http.MethodGet, consumeURL, nil)
	if err != nil {
		t.Fatalf("NewRequest consume: %v", err)
	}
	cresp, err := http.DefaultClient.Do(creq)
	if err != nil {
		t.Fatalf("GET /v1/consume: %v", err)
	}
	defer cresp.Body.Close()

	if cresp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(cresp.Body)
		t.Fatalf("consume status=%d body=%s", cresp.StatusCode, string(body))
	}

	sc := bufio.NewScanner(cresp.Body)
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			t.Fatalf("scan consume: %v", err)
		}
		t.Fatal("expected one consumed line")
	}

	var item struct {
		Value   string `json:"value"`
		Routing struct {
			Label string            `json:"label"`
			Meta  map[string]string `json:"meta"`
		} `json:"routing"`
	}
	if err := json.Unmarshal(sc.Bytes(), &item); err != nil {
		t.Fatalf("unmarshal consume item: %v line=%s", err, string(sc.Bytes()))
	}

	if item.Value != agentMsg {
		t.Fatalf("consumed value mismatch got=%q want=%q", item.Value, agentMsg)
	}
	if got := item.Routing.Meta["selected_agent"]; got != "coder-a" {
		t.Fatalf("routing selected_agent=%q want coder-a (routing=%+v)", got, item.Routing)
	}
	if got := item.Routing.Meta["route_kind"]; got != "capability" {
		t.Fatalf("routing route_kind=%q want capability", got)
	}
}

func TestMainFlags_MultiagentRouter_Direct_IdempotentAckFlow(t *testing.T) {
	cfgJSON := `{
  "agents": ["coder-a"],
  "teams": ["core"],
  "capabilities": {"coding": ["coder-a"]},
  "router_strict": false,
  "source_topics": ["agent-ingress"]
}`

	cfgPath := filepath.Join(t.TempDir(), "multiagent.json")
	if err := os.WriteFile(cfgPath, []byte(cfgJSON), 0o644); err != nil {
		t.Fatalf("WriteFile config: %v", err)
	}

	p := startDriftqdProc(t, "-multiagent-config", cfgPath, "-bootstrap-multiagent-topics")
	mustCreateTopic(t, p.baseURL, "agent-ingress", 1)

	agentMsg := `{"sender":"planner","receiver":"coder-a","intent":"implement","payload":{"task":"pr5"}}`
	produceBody := `{"topic":"agent-ingress","value":` + strconv.Quote(agentMsg) + `,"envelope":{"tenant_id":"t1","idempotency_key":"idem-1"}}`
	status, body, _ := postJSON(t, p.baseURL+"/v1/produce", produceBody)

	if status != http.StatusOK {
		t.Fatalf("first produce status=%d body=%s", status, string(body))
	}

	status, body, _ = postJSON(t, p.baseURL+"/v1/produce", produceBody)
	if status != http.StatusOK {
		t.Fatalf("second produce (idempotent duplicate) status=%d body=%s", status, string(body))
	}

	line, _ := consumeOneNDJSONLine(t, p.baseURL, "agent.coder-a.inbox", "g1", "o1", 5000, 3*time.Second)
	var item struct {
		Partition int    `json:"partition"`
		Offset    int64  `json:"offset"`
		Value     string `json:"value"`
		Routing   struct {
			Label string            `json:"label"`
			Meta  map[string]string `json:"meta"`
		} `json:"routing"`
		Envelope struct {
			TenantID       string `json:"tenant_id"`
			IdempotencyKey string `json:"idempotency_key"`
		} `json:"envelope"`
	}

	if err := json.Unmarshal([]byte(line), &item); err != nil {
		t.Fatalf("unmarshal consume item: %v line=%s", err, line)
	}

	if item.Value != agentMsg {
		t.Fatalf("value mismatch got=%q want=%q", item.Value, agentMsg)
	}

	if got := item.Routing.Meta["route_kind"]; got != "direct" {
		t.Fatalf("route_kind=%q want direct (routing=%+v)", got, item.Routing)
	}

	if got := item.Routing.Meta["receiver"]; got != "coder-a" {
		t.Fatalf("receiver meta=%q want coder-a", got)
	}

	if item.Envelope.TenantID != "t1" || item.Envelope.IdempotencyKey != "idem-1" {
		t.Fatalf("envelope mismatch: %+v", item.Envelope)
	}

	ackURL := fmt.Sprintf("%s/v1/ack?topic=agent.coder-a.inbox&group=g1&owner=o1&partition=%d&offset=%d", p.baseURL, item.Partition, item.Offset)
	status, body, _ = postURL(t, ackURL)
	if status != http.StatusNoContent {
		t.Fatalf("ack status=%d body=%s", status, string(body))
	}

	assertNoConsumeLineWithin(t, p.baseURL, "agent.coder-a.inbox", "g1", "o1", 500, 300*time.Millisecond)
}

func TestMainFlags_MultiagentRouter_Broadcast_ReachesMultipleGroups(t *testing.T) {
	cfgJSON := `{
  "agents": ["planner"],
  "teams": ["core"],
  "router_strict": false,
  "source_topics": ["agent-ingress"]
}`

	cfgPath := filepath.Join(t.TempDir(), "multiagent.json")
	if err := os.WriteFile(cfgPath, []byte(cfgJSON), 0o644); err != nil {
		t.Fatalf("WriteFile config: %v", err)
	}

	p := startDriftqdProc(t, "-multiagent-config", cfgPath, "-bootstrap-multiagent-topics")
	mustCreateTopic(t, p.baseURL, "agent-ingress", 1)

	bmsg := `{"sender":"planner","team":"core","intent":"announce","payload":{"msg":"hello team"}}`
	body := `{"topic":"agent-ingress","value":` + strconv.Quote(bmsg) + `}`
	status, respBody, _ := postJSON(t, p.baseURL+"/v1/produce", body)
	if status != http.StatusOK {
		t.Fatalf("produce status=%d body=%s", status, string(respBody))
	}

	lineA, _ := consumeOneNDJSONLine(t, p.baseURL, "team.core.broadcast", "agentA", "oa", 5000, 3*time.Second)
	lineB, _ := consumeOneNDJSONLine(t, p.baseURL, "team.core.broadcast", "agentB", "ob", 5000, 3*time.Second)

	for name, line := range map[string]string{"A": lineA, "B": lineB} {
		var item struct {
			Value   string `json:"value"`
			Routing struct {
				Meta map[string]string `json:"meta"`
			} `json:"routing"`
		}

		if err := json.Unmarshal([]byte(line), &item); err != nil {
			t.Fatalf("unmarshal consume item %s: %v line=%s", name, err, line)
		}

		if item.Value != bmsg {
			t.Fatalf("group %s value mismatch got=%q want=%q", name, item.Value, bmsg)
		}

		if got := item.Routing.Meta["route_kind"]; got != "broadcast" {
			t.Fatalf("group %s route_kind=%q want broadcast", name, got)
		}

		if got := item.Routing.Meta["team"]; got != "core" {
			t.Fatalf("group %s team meta=%q want core", name, got)
		}
	}
}

func TestMainFlags_MultiagentRouter_OutboxSource_RoutesDirect(t *testing.T) {
	cfgJSON := `{
  "agents": ["planner", "coder-a"],
  "router_strict": false,
  "source_topics": ["agent-ingress"]
}`

	cfgPath := filepath.Join(t.TempDir(), "multiagent.json")
	if err := os.WriteFile(cfgPath, []byte(cfgJSON), 0o644); err != nil {
		t.Fatalf("WriteFile config: %v", err)
	}

	p := startDriftqdProc(t, "-multiagent-config", cfgPath, "-bootstrap-multiagent-topics")

	agentMsg := `{"sender":"planner","receiver":"coder-a","intent":"implement","payload":{"task":"outbox-route"}}`
	produceBody := `{"topic":"agent.planner.outbox","value":` + strconv.Quote(agentMsg) + `}`
	status, body, _ := postJSON(t, p.baseURL+"/v1/produce", produceBody)
	if status != http.StatusOK {
		t.Fatalf("produce status=%d body=%s", status, string(body))
	}

	line, _ := consumeOneNDJSONLine(t, p.baseURL, "agent.coder-a.inbox", "g1", "o1", 5000, 3*time.Second)
	var item struct {
		Value   string `json:"value"`
		Routing struct {
			Meta map[string]string `json:"meta"`
		} `json:"routing"`
	}

	if err := json.Unmarshal([]byte(line), &item); err != nil {
		t.Fatalf("unmarshal consume item: %v line=%s", err, line)
	}

	if item.Value != agentMsg {
		t.Fatalf("value mismatch got=%q want=%q", item.Value, agentMsg)
	}

	if got := item.Routing.Meta["route_kind"]; got != "direct" {
		t.Fatalf("route_kind=%q want direct", got)
	}

	if got := item.Routing.Meta["source_topic"]; got != "agent.planner.outbox" {
		t.Fatalf("source_topic=%q want agent.planner.outbox", got)
	}

	if got := item.Routing.Meta["source_agent"]; got != "planner" {
		t.Fatalf("source_agent=%q want planner", got)
	}

	assertNoConsumeLineWithin(t, p.baseURL, "agent.planner.outbox", "g2", "o2", 500, 300*time.Millisecond)
}

func TestMainFlags_MultiagentRouter_OutboxSource_RejectsSenderMismatch(t *testing.T) {
	cfgJSON := `{
  "agents": ["planner", "coder-a"],
  "router_strict": false,
  "source_topics": ["agent-ingress"]
}`

	cfgPath := filepath.Join(t.TempDir(), "multiagent.json")
	if err := os.WriteFile(cfgPath, []byte(cfgJSON), 0o644); err != nil {
		t.Fatalf("WriteFile config: %v", err)
	}

	p := startDriftqdProc(t, "-multiagent-config", cfgPath, "-bootstrap-multiagent-topics")

	agentMsg := `{"sender":"reviewer","receiver":"coder-a","intent":"implement","payload":{"task":"bad-outbox"}}`
	produceBody := `{"topic":"agent.planner.outbox","value":` + strconv.Quote(agentMsg) + `}`
	status, body, _ := postJSON(t, p.baseURL+"/v1/produce", produceBody)
	if status != http.StatusBadRequest {
		t.Fatalf("produce status=%d want=%d body=%s", status, http.StatusBadRequest, string(body))
	}

	if !strings.Contains(string(body), "sender does not match source outbox topic") {
		t.Fatalf("expected sender mismatch error, body=%s", string(body))
	}

	assertNoConsumeLineWithin(t, p.baseURL, "agent.planner.outbox", "g1", "o1", 500, 300*time.Millisecond)
	assertNoConsumeLineWithin(t, p.baseURL, "agent.coder-a.inbox", "g2", "o2", 500, 300*time.Millisecond)
}

func TestMainFlags_MultiagentRouter_StrictInvalidPayload_ReturnsBadRequest(t *testing.T) {
	cfgJSON := `{
  "agents": ["coder-a"],
  "router_strict": true,
  "source_topics": ["agent-ingress"]
}`

	cfgPath := filepath.Join(t.TempDir(), "multiagent.json")
	if err := os.WriteFile(cfgPath, []byte(cfgJSON), 0o644); err != nil {
		t.Fatalf("WriteFile config: %v", err)
	}

	p := startDriftqdProc(t, "-multiagent-config", cfgPath)
	mustCreateTopic(t, p.baseURL, "agent-ingress", 1)
	mustCreateTopic(t, p.baseURL, "agent.coder-a.inbox", 1)

	status, body := mustProduceStatus(t, p.baseURL, "agent-ingress", "not-json", http.StatusBadRequest)
	if status != http.StatusBadRequest || !strings.Contains(body, "parse agent message json") {
		t.Fatalf("expected parse failure 400, got status=%d body=%s", status, body)
	}

	assertNoConsumeLineWithin(t, p.baseURL, "agent-ingress", "g1", "o1", 500, 300*time.Millisecond)
	assertNoConsumeLineWithin(t, p.baseURL, "agent.coder-a.inbox", "g2", "o2", 500, 300*time.Millisecond)
}
