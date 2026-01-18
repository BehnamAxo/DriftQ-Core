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
}
