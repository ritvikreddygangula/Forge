package eventlog

import (
	"context"
	"fmt"
	"time"
)

// retryTransient retries fn, up to a bounded time window, for the class of
// transient connection/timing errors observed in practice when multiple
// coordinator replicas start up or operate concurrently against one broker
// — dial failures during startup bursts, "unknown topic" propagation races
// right after creation, and plain i/o timeouts under concurrent load on a
// single-shard dev broker. fn must be safe to call more than once.
func retryTransient(ctx context.Context, fn func() error) error {
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for {
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return fmt.Errorf("giving up after repeated transient errors: %w", lastErr)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("context done while retrying: %w", lastErr)
		case <-time.After(100 * time.Millisecond):
		}
	}
}
