package engine

import (
	"fmt"
	"strings"
)

const MetaKeyActiveIndexVersion = "active_index_version"

// GetActiveIndexVersion returns the currently active index version (if set).
func GetActiveIndexVersion(s Store) (string, bool) {
	return s.GetKV(MetaKeyActiveIndexVersion)
}

// PromoteIndexVersion flips the active pointer to the given version ONLY if the run succeeded.
// If version is empty, it defaults to runID.
func PromoteIndexVersion(s Store, runID string, version string) (string, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return "", fmt.Errorf("promote: run_id required")
	}

	version = strings.TrimSpace(version)
	if version == "" {
		version = runID
	}

	run, ok := s.GetRun(runID)
	if !ok {
		return "", ErrRunNotFound
	}

	if run.Status != RunStatusSucceeded {
		return "", fmt.Errorf("promote: run %s status=%s (need %s)", runID, run.Status, RunStatusSucceeded)
	}

	if err := s.PutKV(MetaKeyActiveIndexVersion, version); err != nil {
		return "", err
	}
	return version, nil
}

// RollbackIndexVersion flips the active pointer to a previous version.
func RollbackIndexVersion(s Store, version string) error {
	version = strings.TrimSpace(version)
	if version == "" {
		return fmt.Errorf("rollback: version required")
	}

	return s.PutKV(MetaKeyActiveIndexVersion, version)
}
