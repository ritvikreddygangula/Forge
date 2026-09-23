package coordinator

import (
	"sync"
	"time"
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
