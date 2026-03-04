package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/broker"
	v1 "github.com/driftq-org/DriftQ-Core/internal/httpapi/v1"
)

func newV1TestServer(t *testing.T) (*httptest.Server, *broker.InMemoryBroker) {
	t.Helper()

	b := broker.NewInMemoryBroker()
	s := &server{broker: b}

	rootMux := http.NewServeMux()
	v1Mux := http.NewServeMux()

	v1Mux.HandleFunc("/healthz", s.requireMethod(http.MethodGet)(s.handleHealthz))
	v1Mux.HandleFunc("/produce", s.requireMethod(http.MethodPost)(s.handleProduce))
	v1Mux.HandleFunc("/consume", s.requireMethod(http.MethodGet)(s.handleConsume))
	v1Mux.HandleFunc("/ack", s.requireMethod(http.MethodPost)(s.handleAck))
	v1Mux.HandleFunc("/ack-cumulative", s.requireMethod(http.MethodPost)(s.handleAckCumulative))
	v1Mux.HandleFunc("/nack", s.requireMethod(http.MethodPost)(s.handleNack))
	v1Mux.HandleFunc("/topics", s.method(s.handleTopicsList, s.handleTopicsCreate))
	v1Mux.HandleFunc("/version", s.requireMethod(http.MethodGet)(s.handleVersion))

	rootMux.Handle("/v1/", http.StripPrefix("/v1", v1Mux))
	rootMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		v1.WriteError(w, http.StatusNotFound, "NOT_FOUND", "use /v1/* endpoints")
	})

	ts := httptest.NewServer(rootMux)
	t.Cleanup(ts.Close)

	return ts, b
}

func mustDecodeJSON[T any](t *testing.T, r io.Reader) T {
	t.Helper()
	var out T
	if err := json.NewDecoder(r).Decode(&out); err != nil {
		t.Fatalf("decode json: %v", err)
	}

	return out
}

func TestV1_RootIsBlocked(t *testing.T) {
	ts, _ := newV1TestServer(t)

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d want=%d", resp.StatusCode, http.StatusNotFound)
	}

	got := mustDecodeJSON[v1.ErrorResponse](t, resp.Body)
	if got.Error != "NOT_FOUND" {
		t.Fatalf("error=%q want=%q", got.Error, "NOT_FOUND")
	}

	if !strings.Contains(got.Message, "/v1/") {
		t.Fatalf("message=%q should mention /v1/", got.Message)
	}
}

func TestV1_Healthz_And_MethodNotAllowed(t *testing.T) {
	ts, _ := newV1TestServer(t)

	resp, err := http.Get(ts.URL + "/v1/healthz")
	if err != nil {
		t.Fatalf("GET /v1/healthz: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d want=%d", resp.StatusCode, http.StatusOK)
	}

	got := mustDecodeJSON[v1.HealthzResponse](t, resp.Body)
	if got.Status != "ok" {
		t.Fatalf("status=%q want=%q", got.Status, "ok")
	}

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/healthz", nil)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/healthz: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d want=%d", resp2.StatusCode, http.StatusMethodNotAllowed)
	}

	if allow := resp2.Header.Get("Allow"); allow != http.MethodGet {
		t.Fatalf("Allow=%q want=%q", allow, http.MethodGet)
	}

	errBody := mustDecodeJSON[v1.ErrorResponse](t, resp2.Body)
	if errBody.Error != "METHOD_NOT_ALLOWED" {
		t.Fatalf("error=%q want=%q", errBody.Error, "METHOD_NOT_ALLOWED")
	}
}

func TestV1_Topics_CreateAndList_JSONAndQuery(t *testing.T) {
	ts, b := newV1TestServer(t)

	// List empty
	resp, err := http.Get(ts.URL + "/v1/topics")
	if err != nil {
		t.Fatalf("GET /v1/topics: %v", err)
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d want=%d", resp.StatusCode, http.StatusOK)
	}

	list := mustDecodeJSON[v1.TopicsListResponse](t, resp.Body)
	if len(list.Topics) != 0 {
		t.Fatalf("topics=%v want empty", list.Topics)
	}

	// Create JSON
	body := bytes.NewBufferString(`{"name":"t1","partitions":2}`)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/topics", body)
	req.Header.Set("Content-Type", "application/json")
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/topics: %v", err)
	}

	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusCreated {
		t.Fatalf("status=%d want=%d", resp2.StatusCode, http.StatusCreated)
	}

	created := mustDecodeJSON[v1.TopicsCreateResponse](t, resp2.Body)
	if created.Status != "created" || created.Name != "t1" || created.Partitions != 2 {
		t.Fatalf("created=%+v unexpected", created)
	}

	// Create via query (no JSON body)
	req3, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/topics?name=t2&partitions=1", nil)
	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatalf("POST /v1/topics query: %v", err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusCreated {
		t.Fatalf("status=%d want=%d", resp3.StatusCode, http.StatusCreated)
	}

	// List should include both
	resp4, err := http.Get(ts.URL + "/v1/topics")
	if err != nil {
		t.Fatalf("GET /v1/topics: %v", err)
	}

	defer resp4.Body.Close()
	list2 := mustDecodeJSON[v1.TopicsListResponse](t, resp4.Body)
	if len(list2.Topics) != 2 {
		t.Fatalf("topics=%v want 2", list2.Topics)
	}

	// Sanity: broker sees same topics
	topics, err := b.ListTopics(context.Background())
	if err != nil {
		t.Fatalf("broker topics: %v", err)
	}

	if len(topics) != 2 {
		t.Fatalf("broker topics=%v want 2", topics)
	}
}

func TestV1_Produce_JSONAndQuery(t *testing.T) {
	ts, b := newV1TestServer(t)
	if err := b.CreateTopic(context.Background(), "t1", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// JSON body
	reqBody := bytes.NewBufferString(`{"topic":"t1","value":"hello"}`)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/produce", reqBody)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/produce: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d want=%d", resp.StatusCode, http.StatusOK)
	}

	got := mustDecodeJSON[v1.ProduceResponse](t, resp.Body)
	if got.Status != "produced" || got.Topic != "t1" {
		t.Fatalf("produce response=%+v unexpected", got)
	}

	// Query params
	req2, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/produce?topic=t1&value=hello2", nil)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("POST /v1/produce query: %v", err)
	}

	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("status=%d want=%d", resp2.StatusCode, http.StatusOK)
	}
}

func TestV1_Produce_BackpressureMapsTo429(t *testing.T) {
	ts, b := newV1TestServer(t)
	ctx := context.Background()
	if err := b.CreateTopic(ctx, "t1", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	limit := b.MaxPartitionMsgs()
	if limit <= 0 {
		t.Fatalf("expected positive default maxPartitionMsgs, got %d", limit)
	}

	// Fill partition to the broker's default maxPartitionMsgs.
	for i := 0; i < limit; i++ {
		if err := b.Produce(ctx, "t1", broker.Message{Value: []byte("x")}); err != nil {
			t.Fatalf("pre-fill produce %d: %v", i, err)
		}
	}

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/produce?topic=t1&value=overflow", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/produce overflow: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status=%d want=%d", resp.StatusCode, http.StatusTooManyRequests)
	}

	if ra := resp.Header.Get("Retry-After"); ra == "" {
		t.Fatalf("Retry-After header missing")
	}

	got := mustDecodeJSON[v1.ResourceExhaustedResponse](t, resp.Body)
	if got.Error != "RESOURCE_EXHAUSTED" {
		t.Fatalf("error=%q want=%q", got.Error, "RESOURCE_EXHAUSTED")
	}

	if got.Reason == "" || got.RetryAfterMs <= 0 {
		t.Fatalf("reason/retry_after_ms not set: %+v", got)
	}
}

func TestV1_Consume_NDJSON(t *testing.T) {
	ts, b := newV1TestServer(t)
	ctx := context.Background()

	if err := b.CreateTopic(ctx, "t1", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	if err := b.Produce(ctx, "t1", broker.Message{Value: []byte("a")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	cctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// lease_ms doesn't get echoed back. It's just behavior (300ms is safe)
	req, _ := http.NewRequestWithContext(
		cctx,
		http.MethodGet,
		ts.URL+"/v1/consume?topic=t1&group=g1&owner=o1&lease_ms=300",
		nil,
	)

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/consume: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d want=%d body=%s", resp.StatusCode, http.StatusOK, string(body))
	}

	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/x-ndjson") {
		t.Fatalf("Content-Type=%q expected ndjson", ct)
	}

	br := bufio.NewReader(resp.Body)
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read ndjson line: %v", err)
	}

	var item v1.ConsumeItem
	if err := json.Unmarshal([]byte(line), &item); err != nil {
		t.Fatalf("unmarshal ndjson line: %v line=%q", err, line)
	}

	// ConsumeItem does NOT include Topic/Group. Those are request params, not echoed in NDJSON.
	if item.Partition != 0 || item.Offset != 0 || item.Value != "a" {
		t.Fatalf("consume item unexpected: %+v", item)
	}

	// optional: this is usually true on first delivery
	if item.Attempts != 1 {
		t.Fatalf("expected Attempts=1, got %+v", item)
	}

	// Stop stream
	cancel()
}

func TestV1_Consume_GroupOffsetsAreIndependent(t *testing.T) {
	ts, b := newV1TestServer(t)
	ctx := context.Background()

	if err := b.CreateTopic(ctx, "t1", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	leaseMs := 300 * time.Millisecond

	// Produce first message
	if err := b.Produce(ctx, "t1", broker.Message{Value: []byte("a")}); err != nil {
		t.Fatalf("Produce(a): %v", err)
	}

	// ---- Consume with group g1 (should get offset 0 / "a") ----
	cctx1, cancel1 := context.WithTimeout(context.Background(), 4*time.Second)
	req1, _ := http.NewRequestWithContext(cctx1, http.MethodGet,
		ts.URL+"/v1/consume?topic=t1&group=g1&owner=o1&lease_ms=300", nil)

	resp1, err := client.Do(req1)
	if err != nil {
		cancel1()
		t.Fatalf("GET /v1/consume (g1): %v", err)
	}

	if resp1.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp1.Body)
		_ = resp1.Body.Close()
		cancel1()
		t.Fatalf("status=%d want=%d body=%s", resp1.StatusCode, http.StatusOK, string(body))
	}

	br1 := bufio.NewReader(resp1.Body)
	line1, err := br1.ReadString('\n')
	if err != nil {
		_ = resp1.Body.Close()
		cancel1()
		t.Fatalf("read ndjson line (g1): %v", err)
	}

	var it1 v1.ConsumeItem
	if err := json.Unmarshal([]byte(line1), &it1); err != nil {
		_ = resp1.Body.Close()
		cancel1()
		t.Fatalf("unmarshal ndjson line (g1): %v line=%q", err, line1)
	}

	if it1.Partition != 0 || it1.Offset != 0 || it1.Value != "a" {
		_ = resp1.Body.Close()
		cancel1()
		t.Fatalf("g1 first item unexpected: %+v", it1)
	}

	// Ack it so g1 advances.
	ack := map[string]any{
		"topic":     "t1",
		"group":     "g1",
		"partition": it1.Partition,
		"offset":    it1.Offset,
		"owner":     "o1",
	}
	ackBody, _ := json.Marshal(ack)
	ackReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/ack", bytes.NewReader(ackBody))
	ackReq.Header.Set("Content-Type", "application/json")

	ackResp, err := client.Do(ackReq)
	if err != nil {
		_ = resp1.Body.Close()
		cancel1()
		t.Fatalf("POST /v1/ack: %v", err)
	}
	_ = ackResp.Body.Close()

	if ackResp.StatusCode != http.StatusNoContent {
		_ = resp1.Body.Close()
		cancel1()
		t.Fatalf("ack status=%d want=%d", ackResp.StatusCode, http.StatusNoContent)
	}

	// IMPORTANT:
	// Close the stream and give the broker time for the lease to lapse. Otherwise a new
	// consume for the same group can block waiting for ownership to clear.
	cancel1()
	_ = resp1.Body.Close()
	time.Sleep(leaseMs + 50*time.Millisecond)

	// Produce second message
	if err := b.Produce(ctx, "t1", broker.Message{Value: []byte("b")}); err != nil {
		t.Fatalf("Produce(b): %v", err)
	}

	// ---- Consume again with SAME group g1 (should get "b" at offset 1) ----
	cctx2, cancel2 := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel2()

	req2, _ := http.NewRequestWithContext(cctx2, http.MethodGet,
		ts.URL+"/v1/consume?topic=t1&group=g1&owner=o1&lease_ms=300", nil)

	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatalf("GET /v1/consume (g1 again): %v", err)
	}
	defer resp2.Body.Close()

	br2 := bufio.NewReader(resp2.Body)
	line2, err := br2.ReadString('\n')
	if err != nil {
		t.Fatalf("read ndjson line (g1 again): %v", err)
	}

	var it2 v1.ConsumeItem
	if err := json.Unmarshal([]byte(line2), &it2); err != nil {
		t.Fatalf("unmarshal ndjson line (g1 again): %v line=%q", err, line2)
	}

	if it2.Partition != 0 || it2.Offset != 1 || it2.Value != "b" {
		t.Fatalf("g1 second item unexpected (expected offset=1 value=b): %+v", it2)
	}

	// ---- Consume with DIFFERENT group g2 (fresh group should start at offset 0 and see "a") ----
	cctx3, cancel3 := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel3()

	req3, _ := http.NewRequestWithContext(cctx3, http.MethodGet,
		ts.URL+"/v1/consume?topic=t1&group=g2&owner=o2&lease_ms=300", nil)

	resp3, err := client.Do(req3)
	if err != nil {
		t.Fatalf("GET /v1/consume (g2): %v", err)
	}
	defer resp3.Body.Close()

	br3 := bufio.NewReader(resp3.Body)
	line3, err := br3.ReadString('\n')
	if err != nil {
		t.Fatalf("read ndjson line (g2): %v", err)
	}

	var it3 v1.ConsumeItem
	if err := json.Unmarshal([]byte(line3), &it3); err != nil {
		t.Fatalf("unmarshal ndjson line (g2): %v line=%q", err, line3)
	}

	if it3.Partition != 0 || it3.Offset != 0 || it3.Value != "a" {
		t.Fatalf("g2 first item unexpected (expected offset=0 value=a): %+v", it3)
	}
}

func TestV1_AckCumulative(t *testing.T) {
	ts, b := newV1TestServer(t)
	ctx := context.Background()

	if err := b.CreateTopic(ctx, "t1", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ch, err := b.ConsumeWithLease(ctx, "t1", "g1", "o1", 5*time.Second)
	if err != nil {
		t.Fatalf("ConsumeWithLease: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := b.Produce(ctx, "t1", broker.Message{Value: []byte{byte('a' + i)}}); err != nil {
			t.Fatalf("Produce %d: %v", i, err)
		}
	}

	var last broker.Message
	for i := 0; i < 3; i++ {
		select {
		case last = <-ch:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for message")
		}
	}

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/ack-cumulative?topic=t1&group=g1&owner=o1&partition=0&offset=2", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/ack-cumulative: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d want=%d body=%s", resp.StatusCode, http.StatusNoContent, string(body))
	}

	if err := b.Produce(ctx, "t1", broker.Message{Value: []byte("d")}); err != nil {
		t.Fatalf("Produce after cumulative ack: %v", err)
	}

	select {
	case next := <-ch:
		if next.Offset != last.Offset+1 || string(next.Value) != "d" {
			t.Fatalf("next item after cumulative ack unexpected: %+v", next)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for next message after cumulative ack")
	}
}
