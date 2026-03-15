package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPDebug_SideEffectsRoutes(t *testing.T) {
	t.Parallel()

	runner := NewRunner(NewMemoryStore())
	reg := NewHandlerRegistry()
	runner.SetHandlerRegistry(reg)

	reg.Register("email.send", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		runtime, _ := SideEffectRuntimeFrom(ctx)
		switch runtime.Mode {
		case SideEffectModeStage:
			return json.RawMessage(`{"preview":"prepared"}`), nil
		case SideEffectModeCommit:
			return json.RawMessage(`{"sent":true}`), nil
		default:
			return nil, nil
		}
	})

	reg.Register("email.undo", func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"undone":true}`), nil
	})

	if err := runner.SaveToolGatewayBundle(ToolGatewayBundle{
		Tools: []ToolPolicy{{
			ID:       "email-send",
			Tool:     "email.send",
			Approved: true,
			SideEffect: &SideEffectPolicy{
				Enabled:          true,
				StageRequired:    true,
				CompensationTool: "email.undo",
			},
		}},
	}); err != nil {
		t.Fatalf("SaveToolGatewayBundle: %v", err)
	}

	mux := http.NewServeMux()
	AttachDebugRoutes(mux, runner)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	spec := []byte(`{"id":"wf_email_http","nodes":[{"id":"send","topic":"email.send"}]}`)
	if err := runner.RunSpecJSON(WithTenantID(context.Background(), "tenant-a"), "run-http-side-effect", spec, reg, json.RawMessage(`{"to":"a@example.com"}`)); err != nil {
		t.Fatalf("RunSpecJSON: %v", err)
	}

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/debug/side-effects?run_id=run-http-side-effect", nil)
	if err != nil {
		t.Fatalf("NewRequest GET /debug/side-effects: %v", err)
	}

	req.Header.Set("X-Tenant-Id", "tenant-a")
	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		t.Fatalf("GET /debug/side-effects: %v", err)
	}

	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/side-effects status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var listResp struct {
		Receipts []SideEffectReceipt `json:"receipts"`
	}

	if err := json.Unmarshal(body, &listResp); err != nil {
		t.Fatalf("decode /debug/side-effects: %v body=%s", err, string(body))
	}

	if len(listResp.Receipts) != 1 {
		t.Fatalf("expected one staged receipt, got %+v", listResp)
	}

	req, err = http.NewRequest(http.MethodPost, srv.URL+"/debug/side-effects/commit", bytes.NewBufferString(`{"receipt_id":"`+listResp.Receipts[0].ID+`"}`))
	if err != nil {
		t.Fatalf("NewRequest POST /debug/side-effects/commit: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-Id", "tenant-a")
	resp, err = http.DefaultClient.Do(req)

	if err != nil {
		t.Fatalf("POST /debug/side-effects/commit: %v", err)
	}

	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /debug/side-effects/commit status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	req, err = http.NewRequest(http.MethodPost, srv.URL+"/debug/side-effects/compensate", bytes.NewBufferString(`{"receipt_id":"`+listResp.Receipts[0].ID+`"}`))
	if err != nil {
		t.Fatalf("NewRequest POST /debug/side-effects/compensate: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-Id", "tenant-a")
	resp, err = http.DefaultClient.Do(req)

	if err != nil {
		t.Fatalf("POST /debug/side-effects/compensate: %v", err)
	}

	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /debug/side-effects/compensate status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
}
