package engine

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPDebug_ForensicsRoutes(t *testing.T) {
	t.Parallel()

	fixture := newForensicFixture(t)

	mux := http.NewServeMux()
	AttachDebugRoutes(mux, fixture.runner)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	lineageResp := mustGetStatus(t, srv.URL+"/debug/forensics/lineage?run_id="+fixture.failedRunID, http.StatusOK)
	var lineageBody struct {
		OK    bool                   `json:"ok"`
		Graph ForensicExecutionGraph `json:"graph"`
	}

	if err := json.Unmarshal(lineageResp, &lineageBody); err != nil {
		t.Fatalf("decode lineage response: %v body=%s", err, string(lineageResp))
	}

	if !lineageBody.OK || lineageBody.Graph.Run.RunID != fixture.failedRunID {
		t.Fatalf("unexpected lineage response: %+v", lineageBody)
	}

	rootResp := mustGetStatus(t, srv.URL+"/debug/forensics/root-cause?run_id="+fixture.failedRunID, http.StatusOK)
	var rootBody struct {
		OK        bool                  `json:"ok"`
		RootCause ForensicRootCauseView `json:"root_cause"`
	}

	if err := json.Unmarshal(rootResp, &rootBody); err != nil {
		t.Fatalf("decode root-cause response: %v body=%s", err, string(rootResp))
	}

	if !rootBody.OK || rootBody.RootCause.PrimaryFailureNode != "B" {
		t.Fatalf("unexpected root-cause response: %+v", rootBody)
	}

	diffURL := srv.URL + "/debug/forensics/run-diff?from_run_id=" + fixture.baseRunID + "&to_run_id=" + fixture.failedRunID
	diffResp := mustGetStatus(t, diffURL, http.StatusOK)
	var diffBody struct {
		OK   bool            `json:"ok"`
		Diff ForensicRunDiff `json:"diff"`
	}

	if err := json.Unmarshal(diffResp, &diffBody); err != nil {
		t.Fatalf("decode run-diff response: %v body=%s", err, string(diffResp))
	}

	if !diffBody.OK || !diffBody.Diff.WorkflowSpec.Changed {
		t.Fatalf("unexpected run-diff response: %+v", diffBody)
	}

	workflowDiffResp := mustGetStatus(t, srv.URL+"/debug/forensics/workflow-diff?from_run_id="+fixture.baseRunID+"&to_run_id="+fixture.failedRunID, http.StatusOK)
	if !strings.Contains(string(workflowDiffResp), `"changed":true`) {
		t.Fatalf("expected changed workflow diff, got %s", string(workflowDiffResp))
	}

	whatChangedResp := mustGetStatus(t, srv.URL+"/debug/forensics/what-changed?from_run_id="+fixture.baseRunID+"&to_run_id="+fixture.failedRunID, http.StatusOK)
	var whatChangedBody struct {
		OK          bool                    `json:"ok"`
		WhatChanged ForensicWhatChangedView `json:"what_changed"`
	}

	if err := json.Unmarshal(whatChangedResp, &whatChangedBody); err != nil {
		t.Fatalf("decode what-changed response: %v body=%s", err, string(whatChangedResp))
	}

	if !whatChangedBody.OK || whatChangedBody.WhatChanged.RootCause == nil {
		t.Fatalf("unexpected what-changed response: %+v", whatChangedBody)
	}
}

func mustGetStatus(t *testing.T, url string, wantStatus int) []byte {
	t.Helper()

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}

	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("GET %s status=%d want=%d body=%s", url, resp.StatusCode, wantStatus, strings.TrimSpace(string(body)))
	}

	return body
}
