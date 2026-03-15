package engine

import (
	"encoding/json"
	"errors"
	"net/http"
)

func attachBrainRoutes(mux *http.ServeMux, runner *Runner) {
	mux.HandleFunc("/debug/brain-policy", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodPost:
		default:
			w.Header().Set("Allow", http.MethodGet+", "+http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		switch r.Method {
		case http.MethodGet:
			policy, ok, err := runner.GetBrainPolicy()

			if err != nil {
				http.Error(w, "get brain policy failed: "+err.Error(), http.StatusInternalServerError)
				return
			}

			if !ok {
				policy = defaultBrainPolicy()
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":         true,
				"configured": ok,
				"policy":     policy,
				"trace_id":   traceID,
			})
		case http.MethodPost:
			var body BrainPolicy
			dec := json.NewDecoder(r.Body)
			dec.DisallowUnknownFields()

			if err := dec.Decode(&body); err != nil {
				http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
				return
			}

			if err := runner.SaveBrainPolicy(body); err != nil {
				http.Error(w, "save brain policy failed: "+err.Error(), http.StatusBadRequest)
				return
			}

			policy, _, _ := runner.GetBrainPolicy()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":       true,
				"policy":   policy,
				"trace_id": traceID,
			})
		}
	})

	mux.HandleFunc("/debug/brain/route", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		traceID := traceIDFromRequest(r)
		ctx := debugContextFromRequest(r, traceID)
		var body BrainRouteRequest
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()

		if err := dec.Decode(&body); err != nil {
			http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
			return
		}

		decision, err := runner.ExplainBrainRoute(ctx, body)
		if err != nil {
			switch {
			case errors.Is(err, ErrRunNotFound):
				http.Error(w, err.Error(), http.StatusNotFound)
			case errors.Is(err, ErrTenantAccessDenied), errors.Is(err, ErrAuthorizationDenied):
				http.Error(w, err.Error(), http.StatusForbidden)
			default:
				http.Error(w, "brain route explain failed: "+err.Error(), http.StatusBadRequest)
			}
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":       true,
			"decision": decision,
			"trace_id": traceID,
		})
	})
}
