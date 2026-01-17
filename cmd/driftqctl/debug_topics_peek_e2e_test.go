package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/driftq-org/DriftQ-Core/internal/engine"
)

type brokerNoPeek struct{}

type brokerWithPeek struct {
	lastTopic string
	lastLimit int
	ret       []any
}

func (b *brokerWithPeek) Peek(topic string, limit int) ([]any, error) {
	b.lastTopic = topic
	b.lastLimit = limit

	if b.ret == nil {
		return []any{}, nil
	}

	return b.ret, nil
}

func newDebugServerForPeek(t *testing.T, broker any) *httptest.Server {
	t.Helper()

	store := engine.NewMemoryStore()
	runner := engine.NewRunner(store)

	mux := http.NewServeMux()
	engine.AttachDebugRoutes(mux, runner)
	engine.AttachTopicDebugRoutes(mux, broker)

	return httptest.NewServer(mux)
}

func TestDebug_TopicsPeek_NotImplemented_Returns501(t *testing.T) {
	ts := newDebugServerForPeek(t, brokerNoPeek{})
	defer ts.Close()

	res, err := http.Get(ts.URL + "/debug/topics/peek?topic=test")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status=%d want=%d", res.StatusCode, http.StatusNotImplemented)
	}
}

func TestDebug_TopicsPeek_Limit_Default_Parse_Clamp(t *testing.T) {
	b := &brokerWithPeek{}
	ts := newDebugServerForPeek(t, b)
	defer ts.Close()

	// default (no limit) => 10
	res, err := http.Get(ts.URL + "/debug/topics/peek?topic=t1")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}

	res.Body.Close()
	if b.lastLimit != 10 {
		t.Fatalf("default limit=%d want=10", b.lastLimit)
	}

	// invalid => keep default 10
	res, err = http.Get(ts.URL + "/debug/topics/peek?topic=t1&limit=abc")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	res.Body.Close()
	if b.lastLimit != 10 {
		t.Fatalf("invalid limit=%d want=10", b.lastLimit)
	}

	// clamp low => 1
	res, err = http.Get(ts.URL + "/debug/topics/peek?topic=t1&limit=0")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}

	res.Body.Close()
	if b.lastLimit != 1 {
		t.Fatalf("clamp low=%d want=1", b.lastLimit)
	}

	// clamp high => 100
	res, err = http.Get(ts.URL + "/debug/topics/peek?topic=t1&limit=999")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}

	res.Body.Close()
	if b.lastLimit != 100 {
		t.Fatalf("clamp high=%d want=100", b.lastLimit)
	}
}

func TestDebug_TopicsPeek_Success_JSONShape(t *testing.T) {
	b := &brokerWithPeek{
		ret: []any{
			map[string]any{"k": "v"},
			"raw",
			float64(123), // json decodes numbers as float64 into interface{}
		},
	}

	ts := newDebugServerForPeek(t, b)
	defer ts.Close()

	res, err := http.Get(ts.URL + "/debug/topics/peek?topic=test&limit=2")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("status=%d want=%d", res.StatusCode, http.StatusOK)
	}

	var got struct {
		OK       bool   `json:"ok"`
		Topic    string `json:"topic"`
		Limit    int    `json:"limit"`
		Messages []any  `json:"messages"`
	}

	if err := json.NewDecoder(res.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if !got.OK || got.Topic != "test" || got.Limit != 2 {
		t.Fatalf("header got=%+v", got)
	}

	if len(got.Messages) != 3 {
		t.Fatalf("messages len=%d want=3", len(got.Messages))
	}
}
