package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/driftq-org/DriftQ-Core/internal/broker"
	"github.com/driftq-org/DriftQ-Core/internal/engine"
	"github.com/driftq-org/DriftQ-Core/internal/storage"
)

type topicDebugAdapter struct {
	b broker.Broker
}

func (a topicDebugAdapter) ListTopics() ([]string, error) {
	return a.b.ListTopics(context.Background())
}

func TestDebug_TopicsList_EmptyIsArray_ThenShowsTopic(t *testing.T) {
	tmp := t.TempDir()

	wal, err := storage.OpenFileWAL(filepath.Join(tmp, "driftq.wal"))
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() { _ = wal.Close() })

	b, err := broker.NewInMemoryBrokerFromWAL(wal)
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	mux := http.NewServeMux()
	engine.AttachTopicDebugRoutes(mux, topicDebugAdapter{b: b})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	type respShape struct {
		OK     bool            `json:"ok"`
		Topics json.RawMessage `json:"topics"`
	}

	// 1) empty => []
	{
		res, err := http.Get(srv.URL + "/debug/topics")
		if err != nil {
			t.Fatalf("get /debug/topics: %v", err)
		}

		defer res.Body.Close()

		if res.StatusCode != http.StatusOK {
			t.Fatalf("status=%d", res.StatusCode)
		}

		var got respShape
		if err := json.NewDecoder(res.Body).Decode(&got); err != nil {
			t.Fatalf("decode: %v", err)
		}

		if string(got.Topics) != "[]" {
			t.Fatalf("expected topics=[], got %s", string(got.Topics))
		}
	}

	// create topic
	if err := b.CreateTopic(context.Background(), "test", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// 2) now includes "test"
	{
		res, err := http.Get(srv.URL + "/debug/topics")
		if err != nil {
			t.Fatalf("get /debug/topics: %v", err)
		}
		defer res.Body.Close()

		var got struct {
			OK     bool             `json:"ok"`
			Topics []map[string]any `json:"topics"`
		}

		if err := json.NewDecoder(res.Body).Decode(&got); err != nil {
			t.Fatalf("decode: %v", err)
		}

		if len(got.Topics) != 1 || got.Topics[0]["topic"] != "test" {
			t.Fatalf("unexpected topics: %#v", got.Topics)
		}
	}
}
