package coordinator

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/ritvikreddygangula/forge/internal/job"
)

// WorkerRegistry tracks the last time each worker was heard from — every
// PollJob call doubles as a heartbeat, so no separate heartbeat RPC exists.
type WorkerRegistry struct {
	mu       sync.Mutex
	lastSeen map[string]time.Time
}

func NewWorkerRegistry() *WorkerRegistry {
	return &WorkerRegistry{lastSeen: make(map[string]time.Time)}
}

func (r *WorkerRegistry) Heartbeat(workerID string, at time.Time) {
	if workerID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastSeen[workerID] = at
}

// DeadWorkers returns IDs of workers whose last heartbeat is older than
// timeout. A worker never heard from at all is never included — absence
// isn't staleness. This is what keeps a freshly-elected raft leader (whose
// registry starts empty) from reaping every worker on its first tick.
func (r *WorkerRegistry) DeadWorkers(timeout time.Duration, now time.Time) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var dead []string
	for id, seen := range r.lastSeen {
		if now.Sub(seen) > timeout {
			dead = append(dead, id)
		}
	}
	return dead
}

// Forget stops tracking a worker — called once its in-flight jobs have been
// reassigned, so it isn't reported dead on every subsequent tick forever.
func (r *WorkerRegistry) Forget(workerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.lastSeen, workerID)
}

// Reaper periodically requeues jobs held by workers that have stopped
// polling. Only acts when this replica is the raft leader (a nil gate — see
// RaftGate — means single-node mode, always "leader").
type Reaper struct {
	registry *WorkerRegistry
	store    job.Store
	gate     *RaftGate
	timeout  time.Duration
}

func NewReaper(registry *WorkerRegistry, store job.Store, gate *RaftGate, timeout time.Duration) *Reaper {
	return &Reaper{registry: registry, store: store, gate: gate, timeout: timeout}
}

// Tick checks once for dead workers and requeues their jobs. Exported and
// separate from Run so tests can drive it with an explicit clock instead of
// waiting on a real ticker.
func (r *Reaper) Tick(now time.Time) {
	if !r.gate.IsLeader() {
		return
	}
	for _, workerID := range r.registry.DeadWorkers(r.timeout, now) {
		requeued, err := r.store.RequeueRunning(workerID)
		if err != nil {
			slog.Error("reaper: failed to requeue jobs from dead worker", "worker_id", workerID, "error", err)
			continue // leave it tracked, try again next tick
		}
		if len(requeued) > 0 {
			slog.Info("reaper: reassigned jobs from dead worker", "worker_id", workerID, "count", len(requeued))
		}
		r.registry.Forget(workerID)
	}
}

func (r *Reaper) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.Tick(time.Now())
		}
	}
}
