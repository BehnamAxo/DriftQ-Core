package debugtypes

type MessageStateRow struct {
	Group            string `json:"group"`
	Topic            string `json:"topic"`
	Partition        int    `json:"partition"`
	Offset           int64  `json:"offset"`
	State            string `json:"state"`
	Owner            string `json:"owner,omitempty"`
	Key              string `json:"key,omitempty"`
	Value            string `json:"value,omitempty"`
	Attempts         int    `json:"attempts"`
	LastError        string `json:"last_error,omitempty"`
	LastDeliveredAt  int64  `json:"last_delivered_at_ms,omitempty"`
	LeaseAgeMs       int64  `json:"lease_age_ms,omitempty"`
	LeaseDurationMs  int64  `json:"lease_duration_ms,omitempty"`
	LeaseExpiresAtMs int64  `json:"lease_expires_at_ms,omitempty"`
	Stalled          bool   `json:"stalled,omitempty"`
	Envelope         any    `json:"envelope,omitempty"`
	Routing          any    `json:"routing,omitempty"`
}
