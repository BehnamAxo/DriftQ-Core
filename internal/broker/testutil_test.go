package broker

import (
	"testing"
	"time"
)

func recvMsg(t *testing.T, ch <-chan Message, timeout time.Duration) Message {
	t.Helper()

	select {
	case m, ok := <-ch:
		if !ok {
			t.Fatalf("channel closed while waiting for message")
		}
		return m
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for message")
		return Message{}
	}
}

func assertNoMsg(t *testing.T, ch <-chan Message, timeout time.Duration) {
	t.Helper()

	select {
	case m, ok := <-ch:
		if ok {
			t.Fatalf("unexpected message received: %+v", m)
		}
		// closed channel is also unexpected here
		t.Fatalf("channel closed unexpectedly while asserting no message")
	case <-time.After(timeout):
		// ok
	}
}

func waitClosed(t *testing.T, ch <-chan Message, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)

	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timed out waiting for channel to close")
		}

		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-time.After(remaining):
			t.Fatalf("timed out waiting for channel to close")
		}
	}
}
