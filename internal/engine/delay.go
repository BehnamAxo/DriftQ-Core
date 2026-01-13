package engine

import (
	"fmt"
	"time"
)

type DelayError struct {
	After  time.Duration
	Reason string
}

func (e *DelayError) Error() string {
	if e == nil {
		return "delay"
	}

	if e.Reason != "" {
		return fmt.Sprintf("delay: %s (%s)", e.After.String(), e.Reason)
	}

	return fmt.Sprintf("delay: %s", e.After.String())
}

func Delay(after time.Duration, reason string) error {
	if after < 0 {
		after = 0
	}

	return &DelayError{After: after, Reason: reason}
}
