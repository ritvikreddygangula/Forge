package coordinator_test

import (
	"testing"
	"time"

	"github.com/ritvikreddygangula/forge/internal/coordinator"
	"github.com/ritvikreddygangula/forge/internal/job"
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

func TestReaper_Tick_RequeuesDeadWorkersJobs(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	if _, err := store.ClaimNext("dead-worker"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}

	registry := coordinator.NewWorkerRegistry()
	now := time.Now()
	registry.Heartbeat("dead-worker", now.Add(-1*time.Minute))

	reaper := coordinator.NewReaper(registry, store, nil, 10*time.Second) // nil gate == single-node, always leader
	reaper.Tick(now)

	got, err := store.Get(created.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusQueued {
		t.Fatalf("expected job requeued after reaping dead worker, got %+v", got)
	}
}

func TestReaper_Tick_LeavesFreshWorkersAlone(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	if _, err := store.ClaimNext("busy-worker"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}

	registry := coordinator.NewWorkerRegistry()
	now := time.Now()
	registry.Heartbeat("busy-worker", now) // just heard from it

	reaper := coordinator.NewReaper(registry, store, nil, 10*time.Second)
	reaper.Tick(now)

	got, err := store.Get(created.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusRunning {
		t.Fatalf("expected fresh worker's job untouched, got %+v", got)
	}
}

func TestReaper_Tick_ForgetsWorkerAfterReaping(t *testing.T) {
	store := job.NewMemoryStore()
	if _, err := store.Create("alpine", []string{"true"}, 10); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if _, err := store.ClaimNext("dead-worker"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}

	registry := coordinator.NewWorkerRegistry()
	now := time.Now()
	registry.Heartbeat("dead-worker", now.Add(-1*time.Minute))

	reaper := coordinator.NewReaper(registry, store, nil, 10*time.Second)
	reaper.Tick(now)

	// A zero timeout flags any KNOWN worker regardless of freshness — if
	// dead-worker is still tracked, it'll show up here.
	if dead := registry.DeadWorkers(0, now); len(dead) != 0 {
		t.Fatalf("expected dead-worker forgotten after reaping, still tracked: %v", dead)
	}
}

// TestReaper_Tick_OnlyActsOnLeader uses a real 2-node in-memory raft cluster
// (same helpers as Task 4.2a's failover test) to prove the follower's Reaper
// genuinely does nothing — not just trusting RaftGate.IsLeader()'s nil-safe
// default, which single-node tests can't distinguish from "correctly checked
// and passed." Without this check, every replica in a cluster would
// independently reassign the same dead worker's jobs.
func TestReaper_Tick_OnlyActsOnLeader(t *testing.T) {
	nodes := newInMemRaftCluster(t, 2)
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.raft.Shutdown()
		}
	})
	leaderNode := waitForLeader(t, nodes, 2*time.Second)

	var followerNode *raftTestNode
	for _, n := range nodes {
		if n.id != leaderNode.id {
			followerNode = n
		}
	}

	store := job.NewMemoryStore()
	if _, err := store.Create("alpine", []string{"true"}, 10); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if _, err := store.ClaimNext("dead-worker"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}

	registry := coordinator.NewWorkerRegistry()
	now := time.Now()
	registry.Heartbeat("dead-worker", now.Add(-1*time.Minute))

	followerGate := coordinator.NewRaftGate(followerNode.raft, nil)
	reaper := coordinator.NewReaper(registry, store, followerGate, 10*time.Second)
	reaper.Tick(now)

	jobs, err := store.RequeueRunning("dead-worker")
	if err != nil {
		t.Fatalf("RequeueRunning returned error: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected the follower's Reaper to have left dead-worker's job untouched, but it was already requeued")
	}
}
