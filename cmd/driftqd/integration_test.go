//go:build integration

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/broker"
	"github.com/driftq-org/DriftQ-Core/internal/engine"
	"github.com/driftq-org/DriftQ-Core/internal/storage"
)

// Test Server Setup
type testServer struct {
	*httptest.Server
	broker broker.Broker
	runner *engine.Runner
	walDir string
}

func startTestServer(t *testing.T) *testServer {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "driftq-integration-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}

	walPath := filepath.Join(tmpDir, "broker.wal")
	wal, err := storage.OpenFileWAL(walPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("OpenFileWAL: %v", err)
	}

	b := broker.NewInMemoryBrokerWithWAL(wal)
	ctx, cancel := context.WithCancel(context.Background())
	b.StartRedeliveryLoop(ctx)

	store := engine.NewMemoryStore()
	runner := engine.NewRunner(store)

	mux := http.NewServeMux()

	// TODO: Wire up actual HTTP handlers from main.go
	// Ok for now though, create minimal handlers for testing

	mux.HandleFunc("/v1/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	})

	mux.HandleFunc("/v1/topics", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			topics, _ := b.ListTopics(r.Context())
			json.NewEncoder(w).Encode(map[string]any{"topics": topics})
		case http.MethodPost:
			name := r.URL.Query().Get("name")
			partitions := 1

			if p := r.URL.Query().Get("partitions"); p != "" {
				fmt.Sscanf(p, "%d", &partitions)
			}

			if err := b.CreateTopic(r.Context(), name, partitions); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]any{"ok": true, "topic": name})

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/v1/produce", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		topic := r.URL.Query().Get("topic")
		key := r.URL.Query().Get("key")
		value := r.URL.Query().Get("value")

		msg := broker.Message{
			Key:   []byte(key),
			Value: []byte(value),
		}

		if idemKey := r.URL.Query().Get("idem_key"); idemKey != "" {
			msg.Envelope = &broker.Envelope{IdempotencyKey: idemKey}
		}

		if err := b.Produce(r.Context(), topic, msg); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{"ok": true})
	})

	mux.HandleFunc("/v1/consume", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		topic := r.URL.Query().Get("topic")
		group := r.URL.Query().Get("group")
		owner := r.URL.Query().Get("owner")

		ch, err := b.ConsumeWithLease(r.Context(), topic, group, owner, 5*time.Second)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/x-ndjson")
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		encoder := json.NewEncoder(w)
		timeout := time.After(10 * time.Second)

		for {
			select {
			case msg := <-ch:
				encoder.Encode(map[string]any{
					"partition": msg.Partition,
					"offset":    msg.Offset,
					"key":       string(msg.Key),
					"value":     string(msg.Value),
				})
				flusher.Flush()
			case <-timeout:
				return
			case <-r.Context().Done():
				return
			}
		}
	})

	mux.HandleFunc("/v1/ack", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		topic := r.URL.Query().Get("topic")
		group := r.URL.Query().Get("group")

		var partition int
		var offset int64

		fmt.Sscanf(r.URL.Query().Get("partition"), "%d", &partition)
		fmt.Sscanf(r.URL.Query().Get("offset"), "%d", &offset)

		if err := b.Ack(r.Context(), topic, group, partition, offset); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	srv := httptest.NewServer(mux)

	t.Cleanup(func() {
		cancel()
		srv.Close()
		os.RemoveAll(tmpDir)
	})

	return &testServer{
		Server: srv,
		broker: b,
		runner: runner,
		walDir: tmpDir,
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
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body map[string]any
	json.NewDecoder(resp.Body).Decode(&body)
	if body["ok"] != true {
		t.Fatalf("expected ok=true, got %v", body)
	}
}

func TestIntegration_CreateTopic(t *testing.T) {
	srv := startTestServer(t)

	resp, err := http.Post(srv.URL+"/v1/topics?name=test-topic&partitions=3", "", nil)
	if err != nil {
		t.Fatalf("POST /v1/topics: %v", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, body)
	}

	// List topics
	resp, err = http.Get(srv.URL + "/v1/topics")
	if err != nil {
		t.Fatalf("GET /v1/topics: %v", err)
	}
	defer resp.Body.Close()

	var list map[string]any
	json.NewDecoder(resp.Body).Decode(&list)
	topics := list["topics"].([]any)

	found := false
	for _, topic := range topics {
		if topic == "test-topic" {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("topic not found in list: %v", topics)
	}
}

func TestIntegration_ProduceConsume(t *testing.T) {
	srv := startTestServer(t)

	// Create topic
	resp, _ := http.Post(srv.URL+"/v1/topics?name=produce-test&partitions=1", "", nil)
	resp.Body.Close()

	// Produce
	resp, err := http.Post(srv.URL+"/v1/produce?topic=produce-test&key=k1&value=hello", "", nil)
	if err != nil {
		t.Fatalf("POST /v1/produce: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("produce failed: %d", resp.StatusCode)
	}

	// Consume (with short timeout)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/consume?topic=produce-test&group=g1&owner=o1", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/consume: %v", err)
	}

	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	if !scanner.Scan() {
		t.Fatal("no message received")
	}

	var msg map[string]any
	json.Unmarshal(scanner.Bytes(), &msg)

	if msg["value"] != "hello" {
		t.Fatalf("expected 'hello', got %v", msg["value"])
	}
}

func TestIntegration_ProduceAck(t *testing.T) {
	srv := startTestServer(t)

	// Setup
	http.Post(srv.URL+"/v1/topics?name=ack-test&partitions=1", "", nil)
	http.Post(srv.URL+"/v1/produce?topic=ack-test&value=msg1", "", nil)

	// Consume
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		srv.URL+"/v1/consume?topic=ack-test&group=g1&owner=o1", nil)
	resp, _ := http.DefaultClient.Do(req)

	scanner := bufio.NewScanner(resp.Body)
	if !scanner.Scan() {
		resp.Body.Close()
		t.Fatal("no message")
	}

	var msg map[string]any
	json.Unmarshal(scanner.Bytes(), &msg)
	resp.Body.Close()

	partition := int(msg["partition"].(float64))
	offset := int64(msg["offset"].(float64))

	// Ack
	resp, err := http.Post(fmt.Sprintf("%s/v1/ack?topic=ack-test&group=g1&owner=o1&partition=%d&offset=%d", srv.URL, partition, offset), "", nil)
	if err != nil {
		t.Fatalf("POST /v1/ack: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("ack failed: %d", resp.StatusCode)
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

	// Produce with same idempotency key twice
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

	// Consume - should only get one message
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/consume?topic=idem-test&group=g1&owner=o1", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/consume: %v", err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	count := 0
	for scanner.Scan() {
		count++
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner: %v", err)
	}

	if count > 1 {
		t.Fatalf("expected 1 message due to idempotency, got %d", count)
	}
}

func TestIntegration_MultipleConsumerGroups(t *testing.T) {
	srv := startTestServer(t)

	// Setup
	http.Post(srv.URL+"/v1/topics?name=multi-group&partitions=1", "", nil)
	http.Post(srv.URL+"/v1/produce?topic=multi-group&value=broadcast", "", nil)

	// Two consumer groups should both receive the message
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	received := make(chan string, 2)

	for _, group := range []string{"groupA", "groupB"} {
		go func(g string) {
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/v1/consume?topic=multi-group&group=%s&owner=o1", srv.URL, g), nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			scanner := bufio.NewScanner(resp.Body)
			if scanner.Scan() {
				received <- g
			}
		}(group)
	}

	// Wait for both
	timeout := time.After(3 * time.Second)
	count := 0
	for count < 2 {
		select {
		case <-received:
			count++
		case <-timeout:
			t.Fatalf("timeout: only received %d/2 messages", count)
		}
	}
}

func TestIntegration_ErrorHandling_InvalidTopic(t *testing.T) {
	srv := startTestServer(t)

	// Produce to non-existent topic
	resp, err := http.Post(srv.URL+"/v1/produce?topic=nonexistent&value=test", "", nil)
	if err != nil {
		t.Fatalf("POST /v1/produce: %v", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected error for non-existent topic, got 200: %s", body)
	}
}

func TestIntegration_ErrorHandling_InvalidMethod(t *testing.T) {
	srv := startTestServer(t)

	// DELETE to topics endpoint (invalid method for this handler)
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
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 405, got %d: %s", resp.StatusCode, body)
	}
}

// Load Test (lightweight-ish)

func TestIntegration_Load_ProduceThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	srv := startTestServer(t)
	http.Post(srv.URL+"/v1/topics?name=load-test&partitions=4", "", nil)

	start := time.Now()
	count := 1000

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
