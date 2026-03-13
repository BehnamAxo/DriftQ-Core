package engine

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestHTTPDebug_TenantIsolationAndAudit(t *testing.T) {
	t.Parallel()

	runStore := NewMemoryStore()
	runner := NewRunner(runStore)
	runner.SetArtifactStore(NewMemoryArtifactStore())

	mux := http.NewServeMux()
	AttachDebugRoutes(mux, runner)

	srv := httptest.NewServer(mux)
	defer srv.Close()

	runID := postRunDemoWithTenant(t, srv.URL, "tenant-a")

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/debug/run?run_id="+url.QueryEscape(runID), nil)
	if err != nil {
		t.Fatalf("NewRequest missing tenant: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /debug/run missing tenant: %v", err)
	}

	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 without tenant, got %d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	req, err = http.NewRequest(http.MethodGet, srv.URL+"/debug/run?run_id="+url.QueryEscape(runID), nil)
	if err != nil {
		t.Fatalf("NewRequest same tenant: %v", err)
	}

	req.Header.Set("X-Tenant-Id", "tenant-a")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /debug/run same tenant: %v", err)
	}

	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for same tenant, got %d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	req, err = http.NewRequest(http.MethodGet, srv.URL+"/debug/run?run_id="+url.QueryEscape(runID), nil)
	if err != nil {
		t.Fatalf("NewRequest other tenant: %v", err)
	}

	req.Header.Set("X-Tenant-Id", "tenant-b")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /debug/run other tenant: %v", err)
	}

	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for other tenant, got %d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	arts := getRunArtifactsWithTenant(t, srv.URL, runID, "tenant-a")
	if len(arts) == 0 {
		t.Fatal("expected artifacts for tenant-scoped run")
	}

	req, err = http.NewRequest(http.MethodGet, srv.URL+"/debug/audit?tenant_id=tenant-a&run_id="+url.QueryEscape(runID), nil)
	if err != nil {
		t.Fatalf("NewRequest audit: %v", err)
	}

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /debug/audit: %v", err)
	}

	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for audit list, got %d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var auditResp struct {
		OK      bool          `json:"ok"`
		Records []AuditRecord `json:"records"`
	}

	if err := json.Unmarshal(body, &auditResp); err != nil {
		t.Fatalf("decode audit response: %v body=%s", err, string(body))
	}

	if !auditResp.OK || len(auditResp.Records) == 0 {
		t.Fatalf("expected audit records, got %+v", auditResp)
	}
}

func postRunDemoWithTenant(t *testing.T, base, tenantID string) string {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, base+"/debug/run-demo", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	req.Header.Set("X-Tenant-Id", tenantID)
	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		t.Fatalf("post run-demo: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("run-demo failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var out struct {
		OK    bool   `json:"ok"`
		RunID string `json:"run_id"`
	}

	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal run-demo: %v body=%s", err, strings.TrimSpace(string(body)))
	}
	return strings.TrimSpace(out.RunID)
}

func getRunArtifactsWithTenant(t *testing.T, base, runID, tenantID string) []artifactMetaJSON {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, base+"/debug/run-artifacts?run_id="+url.QueryEscape(runID), nil)
	if err != nil {
		t.Fatalf("new request artifacts: %v", err)
	}

	req.Header.Set("X-Tenant-Id", tenantID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get run-artifacts: %v", err)
	}

	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("run-artifacts failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var out struct {
		OK        bool               `json:"ok"`
		Artifacts []artifactMetaJSON `json:"artifacts"`
	}

	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal run-artifacts: %v body=%s", err, strings.TrimSpace(string(body)))
	}

	return out.Artifacts
}
