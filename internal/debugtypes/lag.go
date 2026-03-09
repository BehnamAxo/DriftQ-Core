package debugtypes

// ConsumerLagRow is a snapshot row for consumer lag inspection
// Convention:
// - HeadOffset: next offset to be produced (high watermark)
// - CommittedOffset: group's committed offset (next to deliver / last ack+1)
// - Inflight: currently leased but not yet acked
// - Lag: HeadOffset - CommittedOffset (computed/clamped in handler)
type ConsumerLagRow struct {
	Group           string `json:"group"`
	Topic           string `json:"topic"`
	Partition       int    `json:"partition"`
	HeadOffset      int64  `json:"head_offset"`
	CommittedOffset int64  `json:"committed_offset"`
	Inflight        int64  `json:"inflight"`
	Lag             int64  `json:"lag"`
	LeaseOwners     []string `json:"lease_owners,omitempty"`
	LastOwner       string   `json:"last_owner,omitempty"`
	LastDeliveredAt int64    `json:"last_delivered_at_ms,omitempty"`
	OldestLeaseAge  int64    `json:"oldest_lease_age_ms,omitempty"`
	LeaseDurationMs int64    `json:"lease_duration_ms,omitempty"`
	LeaseExpiresAt  int64    `json:"lease_expires_at_ms,omitempty"`
	Stalled         bool     `json:"stalled,omitempty"`
}
