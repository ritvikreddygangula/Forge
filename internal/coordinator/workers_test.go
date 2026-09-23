package coordinator_test

import (
	"testing"
	"time"

	"github.com/ritvikreddygangula/forge/internal/coordinator"
)

func TestWorkerRegistry_DeadWorkers_OnlyFlagsStaleKnownWorkers(t *testing.T) {
	r := coordinator.NewWorkerRegistry()
	now := time.Now()
	r.Heartbeat("fresh", now)
	r.Heartbeat("stale", now.Add(-1*time.Minute))

	dead := r.DeadWorkers(10*time.Second, now)
	if len(dead) != 1 || dead[0] != "stale" {
		t.Fatalf("expected only 'stale' reported dead, got %v", dead)
	}
}

func TestWorkerRegistry_DeadWorkers_NeverFlagsUnknownWorkers(t *testing.T) {
	r := coordinator.NewWorkerRegistry()
	// No heartbeats recorded at all — simulates a freshly-elected raft
	// leader with no history yet. Must report nothing, not everything.
	dead := r.DeadWorkers(10*time.Second, time.Now())
	if len(dead) != 0 {
		t.Fatalf("expected no dead workers with empty history, got %v", dead)
	}
}

func TestWorkerRegistry_Forget(t *testing.T) {
	r := coordinator.NewWorkerRegistry()
	now := time.Now()
	r.Heartbeat("gone", now.Add(-1*time.Minute))
	r.Forget("gone")

	dead := r.DeadWorkers(10*time.Second, now)
	if len(dead) != 0 {
		t.Fatalf("expected forgotten worker to no longer be tracked, got %v", dead)
	}
}
