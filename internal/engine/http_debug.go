package engine

import (
	"encoding/json"
	"net/http"
)

// Adds minimal observability endpoints
func AttachDebugRoutes(mux *http.ServeMux, runner *Runner) {
	mux.HandleFunc("/debug/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// snap := runner.metrics.Snapshot()
		snap := runner.MetricsSnapshot()

		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(snap)
	})
}
