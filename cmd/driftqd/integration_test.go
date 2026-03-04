//go:build integration

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/broker"
	v1 "github.com/driftq-org/DriftQ-Core/internal/httpapi/v1"
	"github.com/driftq-org/DriftQ-Core/internal/storage"
)

// Test Server Setup
type testServer struct {
	*httptest.Server
	broker *broker.InMemoryBroker
	cancel context.CancelFunc
}

func startTestServer(t *testing.T) *testServer {
	t.Helper()

	tmpDir := t.TempDir()

	walPath := filepath.Join(tmpDir, "broker.wal")
	wal, err := storage.OpenFileWAL(walPath)
	if err != nil {
		t.Fatalf("OpenFileWAL: %v", err)
	}

	// IMPORTANT: keeping this concrete so we can call StartRedeliveryLoop / SetRouter
	b := broker.NewInMemoryBrokerWithWAL(wal)
	b.SetRouter(TestRouter{})

	ctx, cancel := context.WithCancel(context.Background())
	b.StartRedeliveryLoop(ctx)

	s := &server{broker: b} // server wants the interface; *InMemoryBroker implements it

	rootMux := http.NewServeMux()
	v1Mux := http.NewServeMux()

	// v1 routes (same wiring pattern as main.go)
	v1Mux.HandleFunc("/healthz", s.requireMethod(http.MethodGet)(s.handleHealthz))
	v1Mux.HandleFunc("/produce", s.requireMethod(http.MethodPost)(s.handleProduce))
	v1Mux.HandleFunc("/consume", s.requireMethod(http.MethodGet)(s.handleConsume))
	v1Mux.HandleFunc("/ack", s.requireMethod(http.MethodPost)(s.handleAck))
	v1Mux.HandleFunc("/nack", s.requireMethod(http.MethodPost)(s.handleNack))
	v1Mux.HandleFunc("/topics", s.method(s.handleTopicsList, s.handleTopicsCreate))
	v1Mux.HandleFunc("/version", s.requireMethod(http.MethodGet)(s.handleVersion))

	// mount v1 under /v1/*
	rootMux.Handle("/v1/", http.StripPrefix("/v1", v1Mux))

	// block unversioned routes (same behavior as main.go)
	rootMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		v1.WriteError(w, http.StatusNotFound, "NOT_FOUND", "use /v1/* endpoints")
	})

	srv := httptest.NewServer(rootMux)

	t.Cleanup(func() {
		cancel()
		srv.Close()
		_ = b.Close()
		_ = wal.Close()
	})

	return &testServer{
		Server: srv,
		broker: b,
		cancel: cancel,
	}
}

func TestIntegration_Healthz(t *testing.T) {
	srv := startTestServer(t)

	resp, err := http.Get(srv.URL + "/v1/healthz")
	if err != nil {
		t.Fatalf("GET /v1/healthz: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", resp.StatusCode, mustReadAll(resp.Body))
	}

	var got v1.HealthzResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Status != "ok" {
		t.Fatalf("expected status=ok, got %+v", got)
	}
}

func TestIntegration_CreateTopic(t *testing.T) {
	srv := startTestServer(t)

	resp, err := http.Post(srv.URL+"/v1/topics?name=test-topic&partitions=3", "", nil)
	if err != nil {
		t.Fatalf("POST /v1/topics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", resp.StatusCode, mustReadAll(resp.Body))
	}

	var created v1.TopicsCreateResponse
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode: %v body=%s", err, mustReadAll(resp.Body))
	}

	if created.Name != "test-topic" || created.Partitions != 3 {
		t.Fatalf("unexpected create response: %+v", created)
	}

	// List topics
	resp2, err := http.Get(srv.URL + "/v1/topics")
	if err != nil {
		t.Fatalf("GET /v1/topics: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", resp2.StatusCode, mustReadAll(resp2.Body))
	}

	var list v1.TopicsListResponse
	if err := json.NewDecoder(resp2.Body).Decode(&list); err != nil {
		t.Fatalf("decode: %v body=%s", err, mustReadAll(resp2.Body))
	}

	found := false
	for _, name := range list.Topics {
		if name == "test-topic" {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("topic not found in list: %+v", list.Topics)
	}
}

func TestIntegration_ProduceConsume(t *testing.T) {
	srv := startTestServer(t)

	// Create topic
	resp, err := http.Post(srv.URL+"/v1/topics?name=produce-test&partitions=1", "", nil)
	if err != nil {
		t.Fatalf("POST /v1/topics: %v", err)
	}

	resp.Body.Close()

	// Produce
	resp, err = http.Post(srv.URL+"/v1/produce?topic=produce-test&value=hello", "", nil)
	if err != nil {
		t.Fatalf("POST /v1/produce: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("produce failed: %d", resp.StatusCode)
	}

	// Consume one line
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/consume?topic=produce-test&group=g1&owner=o1&lease_ms=300", nil)

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/consume: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("consume status=%d body=%s", resp.StatusCode, mustReadAll(resp.Body))
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/x-ndjson") {
		t.Fatalf("Content-Type=%q expected ndjson", ct)
	}

	sc := bufio.NewScanner(resp.Body)
	if !sc.Scan() {
		t.Fatalf("no message received; err=%v", sc.Err())
	}

	var item v1.ConsumeItem
	if err := json.Unmarshal(sc.Bytes(), &item); err != nil {
		t.Fatalf("unmarshal: %v line=%q", err, string(sc.Bytes()))
	}

	if item.Value != "hello" {
		t.Fatalf("expected value=hello, got %+v", item)
	}
}

func TestIntegration_ProduceAck(t *testing.T) {
	srv := startTestServer(t)

	// Setup
	_, _ = http.Post(srv.URL+"/v1/topics?name=ack-test&partitions=1", "", nil)
	_, _ = http.Post(srv.URL+"/v1/produce?topic=ack-test&value=msg1", "", nil)

	// Consume one line
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/consume?topic=ack-test&group=g1&owner=o1&lease_ms=300", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/consume: %v", err)
	}

	sc := bufio.NewScanner(resp.Body)
	if !sc.Scan() {
		resp.Body.Close()
		t.Fatalf("no message; err=%v", sc.Err())
	}

	var item v1.ConsumeItem
	_ = json.Unmarshal(sc.Bytes(), &item)
	resp.Body.Close()

	// Ack requires owner in this API (AckIfOwner)
	ackURL := fmt.Sprintf("%s/v1/ack?topic=ack-test&group=g1&owner=o1&partition=%d&offset=%d",
		srv.URL, item.Partition, item.Offset)

	ackResp, err := http.Post(ackURL, "", nil)
	if err != nil {
		t.Fatalf("POST /v1/ack: %v", err)
	}
	ackResp.Body.Close()

	if ackResp.StatusCode != http.StatusNoContent {
		t.Fatalf("ack failed: %d", ackResp.StatusCode)
	}
}

func TestIntegration_Idempotency(t *testing.T) {
	srv := startTestServer(t)

	// Setup
	resp, err := http.Post(srv.URL+"/v1/topics?name=idem-test&partitions=1", "", nil)
	if err != nil {
		t.Fatalf("POST /v1/topics: %v", err)
	}
	resp.Body.Close()

	// Produce twice with same idempotency key
	resp, err = http.Post(srv.URL+"/v1/produce?topic=idem-test&value=first&idem_key=unique123", "", nil)
	if err != nil {
		t.Fatalf("POST /v1/produce first: %v", err)
	}
	resp.Body.Close()

	resp, err = http.Post(srv.URL+"/v1/produce?topic=idem-test&value=second&idem_key=unique123", "", nil)
	if err != nil {
		t.Fatalf("POST /v1/produce second: %v", err)
	}
	resp.Body.Close()

	// Consume one line
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()

	req1, _ := http.NewRequestWithContext(ctx1, http.MethodGet,
		srv.URL+"/v1/consume?topic=idem-test&group=g1&owner=o1&lease_ms=300", nil)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatalf("GET /v1/consume: %v", err)
	}

	sc := bufio.NewScanner(resp1.Body)
	if !sc.Scan() {
		resp1.Body.Close()
		t.Fatalf("no message; err=%v", sc.Err())
	}

	var item v1.ConsumeItem
	_ = json.Unmarshal(sc.Bytes(), &item)
	resp1.Body.Close()

	// Ack so group offset advances
	ackURL := fmt.Sprintf("%s/v1/ack?topic=idem-test&group=g1&owner=o1&partition=%d&offset=%d",
		srv.URL, item.Partition, item.Offset)
	ackResp, err := http.Post(ackURL, "", nil)
	if err != nil {
		t.Fatalf("POST /v1/ack: %v", err)
	}
	ackResp.Body.Close()

	// Consume again: should NOT receive a second message (should block until timeout/cancel)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel2()

	req2, _ := http.NewRequestWithContext(ctx2, http.MethodGet,
		srv.URL+"/v1/consume?topic=idem-test&group=g1&owner=o1&lease_ms=300", nil)

	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		// also acceptable: request can fail fast due to context timing on some platforms
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return
		}
		t.Fatalf("GET /v1/consume (2): %v", err)
	}
	defer resp2.Body.Close()

	sc2 := bufio.NewScanner(resp2.Body)
	if sc2.Scan() {
		t.Fatalf("expected no second message due to idempotency, but got: %s", string(sc2.Bytes()))
	}

	if err := sc2.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		// often ends up as a read error when ctx cancels; treat ctx errors as OK
		t.Fatalf("scanner err: %v", err)
	}
}

func TestIntegration_MultipleConsumerGroups(t *testing.T) {
	srv := startTestServer(t)

	_, _ = http.Post(srv.URL+"/v1/topics?name=multi-group&partitions=1", "", nil)
	_, _ = http.Post(srv.URL+"/v1/produce?topic=multi-group&value=broadcast", "", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	received := make(chan string, 2)

	for _, group := range []string{"groupA", "groupB"} {
		go func(g string) {
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
				fmt.Sprintf("%s/v1/consume?topic=multi-group&group=%s&owner=o1&lease_ms=300", srv.URL, g), nil)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			sc := bufio.NewScanner(resp.Body)
			if sc.Scan() {
				received <- g
			}
		}(group)
	}

	timeout := time.After(3 * time.Second)
	got := 0
	for got < 2 {
		select {
		case <-received:
			got++
		case <-timeout:
			t.Fatalf("timeout: only received %d/2 messages", got)
		}
	}
}

func TestIntegration_ErrorHandling_InvalidTopic(t *testing.T) {
	srv := startTestServer(t)

	resp, err := http.Post(srv.URL+"/v1/produce?topic=nonexistent&value=test", "", nil)
	if err != nil {
		t.Fatalf("POST /v1/produce: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		t.Fatalf("expected error for non-existent topic, got 200: %s", mustReadAll(resp.Body))
	}
}

func TestIntegration_ErrorHandling_InvalidMethod(t *testing.T) {
	srv := startTestServer(t)

	req, err := http.NewRequest(http.MethodDelete, srv.URL+"/v1/topics", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d: %s", resp.StatusCode, mustReadAll(resp.Body))
	}
}

// Load Test (lightweight-ish)
func TestIntegration_Load_ProduceThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	srv := startTestServer(t)
	_, _ = http.Post(srv.URL+"/v1/topics?name=load-test&partitions=4", "", nil)

	start := time.Now()
	count := 500

	for i := 0; i < count; i++ {
		resp, err := http.Post(fmt.Sprintf("%s/v1/produce?topic=load-test&value=msg%d", srv.URL, i), "", nil)
		if err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
		resp.Body.Close()
	}

	elapsed := time.Since(start)
	rate := float64(count) / elapsed.Seconds()
	t.Logf("Produced %d messages in %v (%.0f msg/s)", count, elapsed, rate)
}

func mustReadAll(r io.Reader) string {
	b, _ := io.ReadAll(r)
	return string(b)
}
