package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/broker"
	v1 "github.com/driftq-org/DriftQ-Core/internal/httpapi/v1"
)

func TestV1_AckAndNack_OwnerEnforced(t *testing.T) {
	ts, b := newV1TestServer(t)
	ctx := context.Background()

	if err := b.CreateTopic(ctx, "t1", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// ACK
	if err := b.Produce(ctx, "t1", broker.Message{Value: []byte("x")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	consumeCtx1, cancel1 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel1()

	ch1, err := b.ConsumeWithLease(consumeCtx1, "t1", "g1", "ownerA", 2*time.Second)
	if err != nil {
		t.Fatalf("ConsumeWithLease: %v", err)
	}

	var m broker.Message
	select {
	case m = <-ch1:
	case <-consumeCtx1.Done():
		t.Fatalf("timed out waiting for consumed message")
	}

	// Ack with wrong owner => 409
	ackWrong := map[string]any{
		"topic":     "t1",
		"group":     "g1",
		"partition": m.Partition,
		"offset":    m.Offset,
		"owner":     "ownerB",
	}

	ackBody, _ := json.Marshal(ackWrong)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/ack", bytes.NewReader(ackBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		t.Fatalf("POST /v1/ack: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		bb, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d want=%d body=%s", resp.StatusCode, http.StatusConflict, string(bb))
	}

	errResp := mustDecodeJSON[v1.ErrorResponse](t, resp.Body)
	if errResp.Error != "FAILED_PRECONDITION" {
		t.Fatalf("error=%q want=%q", errResp.Error, "FAILED_PRECONDITION")
	}

	// Ack with correct owner => 204
	ackRight := map[string]any{
		"topic":     "t1",
		"group":     "g1",
		"partition": m.Partition,
		"offset":    m.Offset,
		"owner":     "ownerA",
	}

	ackBody2, _ := json.Marshal(ackRight)
	req2, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/ack", bytes.NewReader(ackBody2))
	req2.Header.Set("Content-Type", "application/json")
	resp2, err := http.DefaultClient.Do(req2)

	if err != nil {
		t.Fatalf("POST /v1/ack (right owner): %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusNoContent {
		bb, _ := io.ReadAll(resp2.Body)
		t.Fatalf("status=%d want=%d body=%s", resp2.StatusCode, http.StatusNoContent, string(bb))
	}

	// Cancel consumer stream so it doesn't interfere with the next part
	cancel1()

	// NACK
	// Use a DIFFERENT consumer group so the canceled/old stream from g1 can't "steal" the message
	const nackGroup = "g2"

	if err := b.Produce(ctx, "t1", broker.Message{Value: []byte("y")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	consumeCtx2, cancel2 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel2()

	ch2, err := b.ConsumeWithLease(consumeCtx2, "t1", nackGroup, "ownerA", 2*time.Second)
	if err != nil {
		t.Fatalf("ConsumeWithLease: %v", err)
	}

	var m2 broker.Message
	select {
	case m2 = <-ch2:
	case <-consumeCtx2.Done():
		t.Fatalf("timed out waiting for consumed message 2")
	}

	// Nack wrong owner => 409
	nackWrong := map[string]any{
		"topic":     "t1",
		"group":     nackGroup,
		"partition": m2.Partition,
		"offset":    m2.Offset,
		"owner":     "ownerB",
		"reason":    "test",
	}

	nackBody, _ := json.Marshal(nackWrong)
	req3, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/nack", bytes.NewReader(nackBody))
	req3.Header.Set("Content-Type", "application/json")
	resp3, err := http.DefaultClient.Do(req3)

	if err != nil {
		t.Fatalf("POST /v1/nack: %v", err)
	}
	defer resp3.Body.Close()

	if resp3.StatusCode != http.StatusConflict {
		bb, _ := io.ReadAll(resp3.Body)
		t.Fatalf("status=%d want=%d body=%s", resp3.StatusCode, http.StatusConflict, string(bb))
	}

	// Nack correct owner => 204
	nackRight := map[string]any{
		"topic":     "t1",
		"group":     nackGroup,
		"partition": m2.Partition,
		"offset":    m2.Offset,
		"owner":     "ownerA",
		"reason":    "test",
	}

	nackBody2, _ := json.Marshal(nackRight)
	req4, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/nack", bytes.NewReader(nackBody2))
	req4.Header.Set("Content-Type", "application/json")
	resp4, err := http.DefaultClient.Do(req4)

	if err != nil {
		t.Fatalf("POST /v1/nack (right owner): %v", err)
	}
	defer resp4.Body.Close()

	if resp4.StatusCode != http.StatusNoContent {
		bb, _ := io.ReadAll(resp4.Body)
		t.Fatalf("status=%d want=%d body=%s", resp4.StatusCode, http.StatusNoContent, string(bb))
	}
}

func TestV1_Ack_RejectsUnknownFields(t *testing.T) {
	ts, b := newV1TestServer(t)
	ctx := context.Background()

	if err := b.CreateTopic(ctx, "t1", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	if err := b.Produce(ctx, "t1", broker.Message{Value: []byte("x")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	consumeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	ch, err := b.ConsumeWithLease(consumeCtx, "t1", "g1", "ownerA", 2*time.Second)
	if err != nil {
		t.Fatalf("ConsumeWithLease: %v", err)
	}

	var m broker.Message
	select {
	case m = <-ch:
	case <-consumeCtx.Done():
		t.Fatalf("timed out waiting for consumed message")
	}

	// Unknown field "wat" should be rejected by strict decoder
	body := bytes.NewBufferString(`{"topic":"t1","group":"g1","partition":` + strconv.Itoa(m.Partition) + `,"offset":` + strconv.FormatInt(m.Offset, 10) + `,"owner":"ownerA","wat":123}`)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/ack", body)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/ack: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		bb, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d want=%d body=%s", resp.StatusCode, http.StatusBadRequest, string(bb))
	}
}
