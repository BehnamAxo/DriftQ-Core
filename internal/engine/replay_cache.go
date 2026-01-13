package engine

import "encoding/json"

type replayCacheEntry struct {
	Attempt int
	Output  json.RawMessage
}

// pick the latest succeeded attempt per node
func (r *Runner) buildReplayCacheFromRun(srcRunID string) map[string]replayCacheEntry {
	cache := map[string]replayCacheEntry{}
	prev := r.store.ListNodeExecutions(srcRunID)

	for _, ne := range prev {
		if ne.Status != NodeStatusSucceeded {
			continue
		}
		if len(ne.Output) == 0 {
			continue
		}
		cur, ok := cache[ne.NodeID]
		if !ok || ne.Attempt > cur.Attempt {
			cache[ne.NodeID] = replayCacheEntry{
				Attempt: ne.Attempt,
				Output:  cloneRaw(ne.Output),
			}
		}
	}

	return cache
}
