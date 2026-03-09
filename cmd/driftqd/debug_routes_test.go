package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/driftq-org/DriftQ-Core/internal/broker"
	"github.com/driftq-org/DriftQ-Core/internal/engine"
)

func TestTopicDebugAdapter_MessageStatesRoute(t *testing.T) {
	t.Parallel()

	b := broker.NewInMemoryBroker()
	ctx := context.Background()

	if err := b.CreateTopic(ctx, "demo", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	if err := b.Produce(ctx, "demo", broker.Message{Value: []byte("hello")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	mux := http.NewServeMux()
	engine.AttachTopicDebugRoutes(mux, topicDebugAdapter{b: b})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/debug/messages/state?group=bench&limit=10")
	if err != nil {
		t.Fatalf("GET /debug/messages/state: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d want=%d", resp.StatusCode, http.StatusOK)
	}

	var payload struct {
		OK    bool `json:"ok"`
		Rows []struct {
			Topic string `json:"topic"`
			State string `json:"state"`
		} `json:"rows"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if !payload.OK {
		t.Fatalf("expected ok=true, got %+v", payload)
	}

	if len(payload.Rows) != 1 {
		t.Fatalf("rows=%d want=1", len(payload.Rows))
	}

	if payload.Rows[0].Topic != "demo" || payload.Rows[0].State != "queued" {
		t.Fatalf("unexpected row: %+v", payload.Rows[0])
	}
}
