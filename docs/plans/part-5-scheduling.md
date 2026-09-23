# Part 5 — Multiple workers + real scheduling — Implementation Plan

**Branch:** `part-5-scheduling`, cut from latest `main`.

**Goal:** Multiple workers running concurrently, identified by a stable ID. The coordinator tracks
which worker holds which job via the heartbeat every poll already implies (no new RPC needed), detects
a worker that's stopped polling, and reassigns its in-flight job so another worker can pick it up.
Closed out with a real load test — actual `docker run` execution across N workers — that reports
whatever throughput and latency numbers it actually measures, not a target.

**Spec:** `docs/spec.md`. **Roadmap:** `docs/plans/roadmap.md` (Part 5 section). **Prior plan:**
`docs/plans/part-4-raft.md`.

## Design

### "Least-loaded" already falls out of the existing pull model — worth being honest about

`worker.Loop.RunOnce` is synchronous: poll → if a job, execute it (blocking) → report → loop back to
poll again. A worker is structurally incapable of polling *while* it's busy — it's still inside the
blocking `Execute` call. So under the current one-job-at-a-time-per-worker design, "give the job to the
least-loaded worker" and "give the job to whichever worker happens to be free enough to be asking right
now" are the same thing — every worker that's currently polling has exactly zero jobs in flight. This
Part's real new capability isn't a scheduling algorithm; it's **knowing which worker holds which job at
all**, which today the coordinator has no idea of. That tracking is what makes failure-detection and
reassignment possible — the "least-loaded" framing is satisfied for free once identity exists, not
because of new assignment logic layered on top. Stated plainly rather than building unneeded machinery
to make a trivial case look sophisticated.

**On throughput:** per prior discussion, real per-job `docker run` overhead (roughly 200ms-1s through
Colima) times strictly sequential execution per worker means a grounded expectation is low tens of
jobs/sec with a modest worker count, not hundreds — this Part's load test measures the real number
instead of assuming one. No per-worker concurrent execution is being added in this Part (that would be
a deliberate, separate scope decision, not implied by "multiple workers").

### Worker identity and heartbeat

`PollJobRequest` gains a `worker_id` field. `worker.Loop` generates a UUID once at construction
(`google/uuid`, already a dependency) and sends it on every poll. The coordinator treats **every poll as
an implicit heartbeat** — a worker already polls every `PollInterval` (2s default) whether or not
there's a job, so a separate heartbeat RPC would just be the same signal through a second door.

**Revised after a manual smoke test caught a real bug (Task 5.4):** the above reasoning holds only
*between* jobs. `Loop.RunOnce` blocks on `Execute` for the full job duration, so a worker executing a job
sends no poll — and thus no heartbeat — until that job finishes. A real `sleep 30` job proved this:
the reaper falsely reassigned the job at its 6s dead-worker timeout while the worker was still alive and
actively running it, and (worse) once requeued, a second worker could have claimed and duplicate-executed
the same job, with `MemoryStore.Complete` writing the final result by ID with no ownership check. Fixed by
adding a real `Heartbeat(worker_id)` RPC that `Loop` calls on its own ticker from a goroutine running
alongside `Execute`, so a worker keeps proving liveness independent of whether it's currently polling. The
"no separate RPC needed" framing above was the original (wrong) assumption; it's kept here so the reasoning
and its correction are both visible.

### Requeuing, not a new "assignment" concept

`job.Job` gains a `WorkerID` field, set on claim. A new `job.Store.RequeueRunning(workerID)` finds any
job still `Running` under a given worker ID and moves it back to `Queued` — the exact same state a fresh
`Create` leaves a job in, just re-entering the pool instead of starting fresh. A new event type,
`job_requeued`, replicates this the same way every other transition does, so every replica's tailed copy
stays consistent.

**Known replay-ordering simplification, stated plainly:** live, a requeued job goes to the *back* of the
FIFO queue (fair — it re-enters like a new arrival). A **replayed** store (fresh coordinator rebuilding
from the log) places it at its *original creation-time* position instead, since `Rebuild` folds events in
creation order and only derives final queue position from final status. This is a real, minor behavioral
difference between "live" and "replayed" queue order for a job that was once requeued — acceptable for a
learning project's fairness guarantee (nothing violates correctness, a job is never lost or duplicated),
not worth the added complexity of encoding explicit queue-position metadata in the event log to fix.

### The reaper only acts on the raft leader, and never punishes a worker it's never met

A background `Reaper` ticks periodically, asking a `WorkerRegistry` (last-heartbeat-per-worker-ID) for
workers who've gone quiet longer than a timeout, and requeues their in-flight jobs. Two safety properties,
both essentially free given how the pieces are built:

- **Leader-only.** `Reaper.Tick` checks `raftGate.IsLeader()` (already nil-safe from Part 4 — single-node
  mode just always passes) before doing anything. Without this, every replica in a raft cluster would
  independently try to requeue the same dead worker's jobs.
- **Never reaps a worker it has no history for.** `WorkerRegistry.DeadWorkers` only ever considers
  workers already present in its map — a worker ID it's never seen literally cannot appear in the
  "stale" set. This matters right after a raft failover: the newly-elected leader's registry starts
  empty, so a naive "anyone I haven't heard from recently" check would immediately (and wrongly) flag
  every worker as dead. Because absence isn't staleness here, the new leader simply reaps nothing until
  workers have polled it at least once — safe by construction, not by an added special case.

### Everything else about the existing architecture is untouched

`eventlog.Store` still wraps `job.Store` the same way; `RaftGate`/forwarding from Part 4 don't change;
`Rebuild`/`ApplyEvent` gain one more case each, not a redesign. Single-instance mode (no
`COORDINATOR_REPLICA_ID`, no raft) still works exactly as before, just now supports more than one worker
polling it — nothing about the raft cluster is required to get multi-worker scheduling.

## File structure (new/changed)

```
api/proto/jobv1/job.proto              # MODIFIED — PollJobRequest gains worker_id; new Heartbeat RPC
api/proto/gen/jobv1/                     # regenerated
internal/
  job/
    job.go                                 # MODIFIED — Job.WorkerID field
    store.go                                # MODIFIED — ClaimNext(workerID), RequeueRunning, ApplyClaimed/ApplyRequeued additions
    store_test.go                            # MODIFIED
  eventlog/
    event.go                                  # MODIFIED — Event.WorkerID, EventJobRequeued, Rebuild/ApplyEvent cases
    event_test.go                              # MODIFIED
    store.go                                    # MODIFIED — ClaimNext/RequeueRunning wrapping + publish
    store_test.go                                # MODIFIED
  coordinator/
    workers.go                                    # NEW — WorkerRegistry, Reaper
    workers_test.go                                # NEW
    grpc_server.go                                  # MODIFIED — PollJob records heartbeat, passes worker_id; new Heartbeat handler (forwards to leader)
    grpc_server_test.go                              # MODIFIED
    scheduling_test.go                                # NEW — multi-worker + failure-reassignment test
    loadtest_test.go                                   # NEW — integration-tagged real load test
internal/worker/
  loop.go                                              # MODIFIED — Loop.ID, sent on every poll; heartbeatWhileExecuting goroutine during Execute
  loop_test.go                                          # MODIFIED
cmd/coordinator/main.go                                  # MODIFIED — wire WorkerRegistry + Reaper
```

---

### Task 5.1: Worker identity — proto, `Job.WorkerID`, `ClaimNext(workerID)`

**Files:** Modify `api/proto/jobv1/job.proto` (+ regenerate), `internal/job/job.go`, `internal/job/store.go`,
`internal/job/store_test.go`, `internal/worker/loop.go`, `internal/worker/loop_test.go`,
`internal/coordinator/grpc_server.go`, `internal/coordinator/grpc_server_test.go`.

- [ ] **Step 1:** Add to `PollJobRequest` in `api/proto/jobv1/job.proto`:

```proto
message PollJobRequest {
  string worker_id = 1;
}
```

  Run `make proto` to regenerate (buf + plugins already installed from Part 2).

- [ ] **Step 2: Write the failing test** — append to `internal/job/store_test.go`:

```go
func TestMemoryStore_ClaimNext_RecordsWorkerID(t *testing.T) {
	s := job.NewMemoryStore()
	if _, err := s.Create("alpine", []string{"true"}, 10); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	claimed, err := s.ClaimNext("worker-1")
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if claimed.WorkerID != "worker-1" {
		t.Fatalf("expected worker ID worker-1, got %q", claimed.WorkerID)
	}
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `go test ./internal/job/... -run TestMemoryStore_ClaimNext_RecordsWorkerID`
Expected: FAIL — `ClaimNext` doesn't take an argument yet.

- [ ] **Step 4: Implement**

`internal/job/job.go` — add `WorkerID string` field to `Job` (after `Status`).

`internal/job/store.go` — change the `Store` interface and `MemoryStore.ClaimNext`:
```go
type Store interface {
	Create(image string, command []string, timeoutSeconds int) (*Job, error)
	Get(id string) (*Job, error)
	ClaimNext(workerID string) (*Job, error)
	Complete(id string, status Status, stdout, stderr string, exitCode int) error
	RequeueRunning(workerID string) ([]*Job, error)
}
```
```go
func (s *MemoryStore) ClaimNext(workerID string) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, id := range s.order {
		j := s.jobs[id]
		if j.Status == StatusQueued {
			j.Status = StatusRunning
			j.WorkerID = workerID
			j.UpdatedAt = time.Now()
			cp := *j
			return &cp, nil
		}
	}
	return nil, nil
}
```
  (`RequeueRunning`'s real implementation lands in Task 5.3 — stub it returning `nil, nil` for now so
  the interface compiles, or implement both together if that reads more naturally during the session.)

- [ ] **Step 5: Update every existing `ClaimNext()` call site** to `ClaimNext("")` or a real ID —
  mechanical fixes across `internal/job/store_test.go` (existing tests), `internal/eventlog/store.go` and
  its tests, `internal/coordinator/*_test.go`, `internal/coordinator/integration_test.go`.

- [ ] **Step 6: Thread worker identity through `worker.Loop`**

Add to `Loop` struct in `internal/worker/loop.go`:
```go
type Loop struct {
	ID           string
	client       jobv1.JobServiceClient
	conn         *grpc.ClientConn
	PollInterval time.Duration
	Execute      func(ctx context.Context, image string, command []string, timeoutSeconds int) (ExecResult, error)
}
```
Set `ID: uuid.NewString()` in `newLoop`. Update `RunOnce` to send it:
```go
resp, err := l.client.PollJob(ctx, &jobv1.PollJobRequest{WorkerId: l.ID})
```

- [ ] **Step 7: Update `GRPCServer.PollJob`** to pass the incoming ID through:

```go
j, err := s.store.ClaimNext(req.WorkerId)
```

- [ ] **Step 8: Run full suite, verify, commit**

Run: `go build ./... && go test ./...`
Expected: PASS, all existing tests updated and passing, `TestMemoryStore_ClaimNext_RecordsWorkerID` new
and passing.

Commit message: `feat(coordinator,worker): add worker identity to poll requests`

*(Not in the roadmap's original 6-item list for this Part — genuine groundwork the rest of the Part
depends on, same as Part 4's Task 4.1. Called out explicitly.)*

---

### Task 5.2: `WorkerRegistry` — heartbeat tracking

**Files:** Create `internal/coordinator/workers.go`, `internal/coordinator/workers_test.go`. Modify
`internal/coordinator/grpc_server.go`.

**Interfaces:** Produces `coordinator.NewWorkerRegistry() *WorkerRegistry`,
`(*WorkerRegistry).Heartbeat(workerID string, at time.Time)`,
`(*WorkerRegistry).DeadWorkers(timeout time.Duration, now time.Time) []string`,
`(*WorkerRegistry).Forget(workerID string)`.

- [ ] **Step 1: Write the failing tests** — `internal/coordinator/workers_test.go`:

```go
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
```

- [ ] **Step 2: Run to verify failure, then implement**

`internal/coordinator/workers.go`:
```go
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
```

- [ ] **Step 3: Wire into `GRPCServer.PollJob`** — record the heartbeat before claiming:

Add `workerRegistry *WorkerRegistry` field + a `SetWorkerRegistry` setter (same nil-safe-default pattern
as `SetRaftGate` — a `nil` registry means "don't track," matching every existing test that doesn't set
one):
```go
func (s *GRPCServer) PollJob(ctx context.Context, req *jobv1.PollJobRequest) (*jobv1.PollJobResponse, error) {
	if !s.raftGate.IsLeader() {
		client, err := s.leaderClient()
		if err != nil {
			return nil, err
		}
		return client.PollJob(ctx, req)
	}

	if s.workerRegistry != nil {
		s.workerRegistry.Heartbeat(req.WorkerId, time.Now())
	}

	j, err := s.store.ClaimNext(req.WorkerId)
	// ... unchanged from here
}
```

- [ ] **Step 4: Run tests, verify, commit**

Run: `go test ./internal/coordinator/... -v`
Expected: PASS, all `TestWorkerRegistry_*` plus every existing test.

Commit message: `feat(coordinator): track worker load via heartbeats`

---

### Task 5.3: Requeue running jobs from a given worker

**Files:** Modify `internal/job/store.go`, `internal/job/store_test.go`, `internal/eventlog/event.go`,
`internal/eventlog/event_test.go`, `internal/eventlog/store.go`, `internal/eventlog/store_test.go`.

- [ ] **Step 1: Write the failing test** — append to `internal/job/store_test.go`:

```go
func TestMemoryStore_RequeueRunning(t *testing.T) {
	s := job.NewMemoryStore()
	created, _ := s.Create("alpine", []string{"true"}, 10)
	if _, err := s.ClaimNext("worker-1"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}

	requeued, err := s.RequeueRunning("worker-1")
	if err != nil {
		t.Fatalf("RequeueRunning returned error: %v", err)
	}
	if len(requeued) != 1 || requeued[0].ID != created.ID {
		t.Fatalf("expected job %s requeued, got %+v", created.ID, requeued)
	}

	got, err := s.Get(created.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusQueued || got.WorkerID != "" {
		t.Fatalf("expected job back to queued with no worker, got %+v", got)
	}

	// Genuinely claimable again, not just showing the right status.
	claimed, err := s.ClaimNext("worker-2")
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if claimed == nil || claimed.ID != created.ID {
		t.Fatalf("expected the requeued job to be claimable, got %+v", claimed)
	}
}

func TestMemoryStore_RequeueRunning_IgnoresOtherWorkersAndTerminalJobs(t *testing.T) {
	s := job.NewMemoryStore()
	a, _ := s.Create("alpine", []string{"true"}, 10)
	b, _ := s.Create("alpine", []string{"true"}, 10)
	if _, err := s.ClaimNext("worker-1"); err != nil { // claims a
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if _, err := s.ClaimNext("worker-2"); err != nil { // claims b
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if err := s.Complete(a.ID, job.StatusSucceeded, "ok\n", "", 0); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}

	requeued, err := s.RequeueRunning("worker-1")
	if err != nil {
		t.Fatalf("RequeueRunning returned error: %v", err)
	}
	if len(requeued) != 0 {
		t.Fatalf("expected nothing requeued (a is terminal, b belongs to worker-2), got %+v", requeued)
	}

	got, _ := s.Get(b.ID)
	if got.Status != job.StatusRunning {
		t.Fatalf("expected worker-2's job untouched, got %+v", got)
	}
}
```

- [ ] **Step 2: Implement** in `internal/job/store.go`:

```go
func (s *MemoryStore) RequeueRunning(workerID string) ([]*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var requeued []*Job
	for _, id := range s.order {
		_ = id // order only holds queued jobs; running jobs are found via s.jobs below
	}
	for _, j := range s.jobs {
		if j.Status == StatusRunning && j.WorkerID == workerID {
			j.Status = StatusQueued
			j.WorkerID = ""
			j.UpdatedAt = time.Now()
			s.order = append(s.order, j.ID) // back of the queue — see plan doc's ordering note
			cp := *j
			requeued = append(requeued, &cp)
		}
	}
	return requeued, nil
}
```
  (The `for _, id := range s.order` no-op line above is a placeholder reminder that `s.order` only ever
  holds queued job IDs — running jobs are found by scanning `s.jobs` directly, not `s.order` — delete it
  during implementation; iterating a live map for updates is safe here since we mutate values, not the
  map's key set, mid-range.)

- [ ] **Step 3: Run tests, verify they pass**

Run: `go test ./internal/job/... -v`

- [ ] **Step 4: Extend the event model** — `internal/eventlog/event.go`:

```go
const EventJobRequeued EventType = "job_requeued"
```
Add `WorkerID string \`json:"worker_id,omitempty"\`` to `Event` (used by `EventJobClaimed`, so replay
knows which worker held a job). Update `Rebuild`'s switch: `EventJobClaimed` now also sets
`j.WorkerID = e.WorkerID`; add a case for `EventJobRequeued` setting `j.Status = StatusQueued, j.WorkerID = ""`.
Update `ApplyEvent`/add `job.MemoryStore.ApplyRequeued(id string, at time.Time)` (mirrors `ApplyClaimed`'s
shape: no-op if job unknown or not currently `Running`).

- [ ] **Step 5: Write failing tests for the fold/apply changes** — append to
  `internal/eventlog/event_test.go`:

```go
func TestRebuild_RequeuedJobReturnsToQueued(t *testing.T) {
	now := time.Now()
	jobs := eventlog.Rebuild([]eventlog.Event{
		{Type: eventlog.EventJobCreated, JobID: "a", Timestamp: now},
		{Type: eventlog.EventJobClaimed, JobID: "a", WorkerID: "worker-1", Timestamp: now},
		{Type: eventlog.EventJobRequeued, JobID: "a", Timestamp: now},
	})
	if len(jobs) != 1 || jobs[0].Status != job.StatusQueued || jobs[0].WorkerID != "" {
		t.Fatalf("expected job back to queued with no worker, got %+v", jobs)
	}
}

func TestApplyEvent_Requeued(t *testing.T) {
	store := job.NewMemoryStore()
	now := time.Now()
	eventlog.ApplyEvent(store, eventlog.Event{Type: eventlog.EventJobCreated, JobID: "a", Timestamp: now})
	eventlog.ApplyEvent(store, eventlog.Event{Type: eventlog.EventJobClaimed, JobID: "a", WorkerID: "worker-1", Timestamp: now})
	eventlog.ApplyEvent(store, eventlog.Event{Type: eventlog.EventJobRequeued, JobID: "a", Timestamp: now})

	got, err := store.Get("a")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusQueued || got.WorkerID != "" {
		t.Fatalf("expected queued with no worker, got %+v", got)
	}
}
```

- [ ] **Step 6: Wrap in `eventlog.Store`** — `internal/eventlog/store.go`:

```go
func (s *Store) ClaimNext(workerID string) (*job.Job, error) {
	j, err := s.inner.ClaimNext(workerID)
	if err != nil || j == nil {
		return j, err
	}
	if err := s.producer.Publish(context.Background(), Event{
		Type: EventJobClaimed, JobID: j.ID, WorkerID: workerID, Timestamp: time.Now(),
	}); err != nil {
		return nil, fmt.Errorf("job claimed but failed to publish event: %w", err)
	}
	return j, nil
}

func (s *Store) RequeueRunning(workerID string) ([]*job.Job, error) {
	requeued, err := s.inner.RequeueRunning(workerID)
	if err != nil {
		return nil, err
	}
	for _, j := range requeued {
		if pubErr := s.producer.Publish(context.Background(), Event{
			Type: EventJobRequeued, JobID: j.ID, Timestamp: time.Now(),
		}); pubErr != nil {
			return requeued, fmt.Errorf("jobs requeued locally but failed to publish for %s: %w", j.ID, pubErr)
		}
	}
	return requeued, nil
}
```
  (Same known limitation as Part 3's `Create`/`ClaimNext`/`Complete`: apply-then-publish, no rollback of
  local state if a publish fails partway through — stated once there, applies identically here.)

- [ ] **Step 7: Run full suite, verify, commit**

Run: `go build ./... && go test ./...`

Commit message: `feat(job,eventlog): add job requeuing for worker-failure reassignment`

---

### Task 5.4: The `Reaper` — heartbeat-timeout detection and reassignment

**Files:** Add to `internal/coordinator/workers.go`, `internal/coordinator/workers_test.go`. Modify
`cmd/coordinator/main.go`.

**Interfaces:** Produces `coordinator.NewReaper(registry *WorkerRegistry, store job.Store, gate *RaftGate,
timeout time.Duration) *Reaper`, `(*Reaper).Tick(now time.Time)`, `(*Reaper).Run(ctx, interval time.Duration)`.

- [ ] **Step 1: Write the failing tests** — append to `internal/coordinator/workers_test.go`:

```go
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
```

- [ ] **Step 2: Implement**, appending to `internal/coordinator/workers.go`:

```go
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
```
  Needs `context`, `log/slog`, `github.com/ritvikreddygangula/forge/internal/job` added to
  `workers.go`'s imports.

- [ ] **Step 3: Wire into `cmd/coordinator/main.go`** — construct the registry, pass it to
  `GRPCServer.SetWorkerRegistry`, start the reaper:

```go
workerRegistry := coordinator.NewWorkerRegistry()
reaper := coordinator.NewReaper(workerRegistry, store, raftGate, 3*worker.DefaultPollInterval) // see note below
go reaper.Run(ctx, worker.DefaultPollInterval)
```
  (`raftGate` here is whatever this replica's gate is — `nil` in single-instance mode, a real one in
  cluster mode; `Reaper`/`RaftGate.IsLeader()` handle both identically. Timeout tuned as a multiple of
  the worker's own poll interval so a couple of missed polls, not one, trigger reassignment — avoids
  reaping a worker that's just between two 2-second polls. `worker.DefaultPollInterval` is a small new
  exported constant — Step 4 below — so main.go and the worker binary can't drift apart on this number.)

- [ ] **Step 4:** In `internal/worker/loop.go`, add `const DefaultPollInterval = 2 * time.Second` and use
  it in `newLoop` instead of the current inline `2 * time.Second` literal.

- [ ] **Step 5: Run tests, verify, commit**

Run: `go build ./... && go test ./... -v`

Commit message: `feat(coordinator): add heartbeat-timeout failure detection and reassignment`

*(Combines the roadmap's separately-listed "heartbeat-timeout failure detection" and "reassign in-flight
jobs from dead workers" items into one commit — in this design they're the same mechanism, not two
separable pieces; noted here rather than forcing an artificial split.)*

---

### Task 5.5: Multi-worker scheduling + failure-reassignment test

**Files:** Create `internal/coordinator/scheduling_test.go`.

Proves the whole mechanism end-to-end at the coordinator level: two real `worker.Loop`s (fake `Execute`,
no real Docker needed here — this test is about scheduling/reassignment, not execution) against one real
coordinator, kill one worker's polling, confirm its in-flight job gets reassigned to the survivor.

- [ ] **Step 1: Write the test**

```go
package coordinator_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
	"github.com/ritvikreddygangula/forge/internal/coordinator"
	"github.com/ritvikreddygangula/forge/internal/job"
	"github.com/ritvikreddygangula/forge/internal/worker"
)

func TestScheduling_ReassignsDeadWorkersJobToSurvivor(t *testing.T) {
	store := job.NewMemoryStore()
	registry := coordinator.NewWorkerRegistry()
	grpcServer := coordinator.NewGRPCServer(store)
	grpcServer.SetWorkerRegistry(registry)

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	jobv1.RegisterJobServiceServer(s, grpcServer)
	go func() { _ = s.Serve(lis) }()
	defer s.Stop()

	created, err := store.Create("alpine", []string{"true"}, 10)
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	deadWorker, err := worker.NewLoop(lis.Addr().String())
	if err != nil {
		t.Fatalf("failed to build worker loop: %v", err)
	}
	defer func() { _ = deadWorker.Close() }()
	deadWorker.Execute = func(ctx context.Context, image string, command []string, timeoutSeconds int) (worker.ExecResult, error) {
		return worker.ExecResult{Stdout: "should never actually run\n", ExitCode: 0}, nil
	}

	// The "dead" worker claims the job, then simply never reports back —
	// exactly what a crashed worker looks like from the coordinator's side.
	ctx := context.Background()
	pollResp, err := grpcServer_pollDirect(t, deadWorker, ctx) // see Step 2 note
	_ = pollResp

	got, err := store.Get(created.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusRunning || got.WorkerID == "" {
		t.Fatalf("expected job claimed by the dead worker, got %+v", got)
	}

	reaper := coordinator.NewReaper(registry, store, nil, 5*time.Second)
	reaper.Tick(time.Now().Add(10 * time.Second)) // simulate time passing, no real sleep

	got, err = store.Get(created.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusQueued {
		t.Fatalf("expected job requeued after reaping, got %+v", got)
	}

	survivor, err := worker.NewLoop(lis.Addr().String())
	if err != nil {
		t.Fatalf("failed to build survivor loop: %v", err)
	}
	defer func() { _ = survivor.Close() }()
	executed := false
	survivor.Execute = func(ctx context.Context, image string, command []string, timeoutSeconds int) (worker.ExecResult, error) {
		executed = true
		return worker.ExecResult{Stdout: "ok\n", ExitCode: 0}, nil
	}
	if err := survivor.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	if !executed {
		t.Fatal("expected the survivor to claim and execute the reassigned job")
	}

	got, err = store.Get(created.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusSucceeded {
		t.Fatalf("expected job completed by survivor, got %+v", got)
	}
}
```

  (`grpcServer_pollDirect` is a placeholder for "have `deadWorker` do exactly one poll-and-claim without
  the rest of `RunOnce`'s execute/report" — during implementation this is simplest as just calling
  `deadWorker`'s underlying gRPC client's `PollJob` once directly, or by giving `worker.Loop` a small
  exported seam for "claim only." Exact shape finalized during implementation, same as Part 4's plan
  flagged its own test scaffolding uncertainty ahead of time — the point of this step is the behavior it
  proves.)

- [ ] **Step 2: Run, verify, commit**

Run: `go test ./internal/coordinator/... -run TestScheduling -v`

Commit message: `test: add multi-worker scheduling and failure-reassignment test`

---

### Task 5.6: Real load test — actual numbers, not a target

**Files:** Create `internal/coordinator/loadtest_test.go` (build tag `integration` — needs real Docker
and real Redpanda, per this project's standing rule against mocking the thing a fault-tolerance story
depends on).

Starts one real coordinator (REST + gRPC, real listeners, real `eventlog.Store` against real Redpanda)
and N real `worker.Loop`s — real `Execute = worker.RunJob`, real `docker run` — submits a batch of real
jobs, measures wall-clock throughput and latency. **Decided (carried from the roadmap): no synthetic
no-op job type to inflate the number — whatever this measures with real container execution is what's
real.**

- [ ] **Step 1: Write the test**

```go
//go:build integration

package coordinator_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
	"github.com/ritvikreddygangula/forge/internal/coordinator"
	"github.com/ritvikreddygangula/forge/internal/eventlog"
	"github.com/ritvikreddygangula/forge/internal/job"
	"github.com/ritvikreddygangula/forge/internal/worker"
)

func TestLoadTest_RealWorkersRealDocker(t *testing.T) {
	const numWorkers = 25
	const numJobs = 500 // modest and real beats large and synthetic — see plan doc

	brokers := []string{"localhost:9092"}
	topic := testTopic(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := eventlog.EnsureTopic(ctx, brokers, topic); err != nil {
		t.Fatalf("EnsureTopic returned error: %v", err)
	}
	producer := eventlog.NewKafkaProducer(brokers, topic)
	defer func() { _ = producer.Close() }()
	store := eventlog.NewStore(job.NewMemoryStore(), producer)

	registry := coordinator.NewWorkerRegistry()
	grpcServer := coordinator.NewGRPCServer(store)
	grpcServer.SetWorkerRegistry(registry)
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	jobv1.RegisterJobServiceServer(grpcSrv, grpcServer)
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.Stop()

	restSrv := httptest.NewServer(coordinator.NewServer(store))
	defer restSrv.Close()

	// Submit all jobs up front.
	ids := make([]string, numJobs)
	for i := 0; i < numJobs; i++ {
		resp, err := http.Post(restSrv.URL+"/jobs", "application/json",
			strings.NewReader(`{"image":"alpine:3.19","command":["true"],"timeout_seconds":30}`))
		if err != nil {
			t.Fatalf("submit failed: %v", err)
		}
		var got map[string]any
		_ = jsonDecodeAndClose(resp, &got) // see Step 2 note: json.NewDecoder(resp.Body).Decode + resp.Body.Close
		ids[i] = got["id"].(string)
	}

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := worker.NewLoop(lis.Addr().String())
			if err != nil {
				t.Errorf("failed to build worker: %v", err)
				return
			}
			defer func() { _ = l.Close() }()
			workerCtx, workerCancel := context.WithTimeout(ctx, 4*time.Minute)
			defer workerCancel()
			for workerCtx.Err() == nil {
				if err := l.RunOnce(workerCtx); err != nil {
					return
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	completed := 0
	for _, id := range ids {
		j, err := store.Get(id)
		if err == nil && j.Status == job.StatusSucceeded {
			completed++
		}
	}

	rate := float64(completed) / elapsed.Seconds()
	t.Logf("load test: %d/%d jobs completed by %d workers in %s (%.1f jobs/sec)",
		completed, numJobs, numWorkers, elapsed, rate)
	if completed != numJobs {
		t.Fatalf("expected all %d jobs to complete, got %d", numJobs, completed)
	}
}
```

  (`jsonDecodeAndClose` is shorthand for the standard `json.NewDecoder(resp.Body).Decode(&got)` +
  `resp.Body.Close()` pair already used elsewhere in this codebase's tests — written as a placeholder
  name here only to keep this snippet shorter; write it inline during implementation, no new helper
  needed. Workers loop on `RunOnce` directly rather than `Run`, so the test controls pacing and knows
  when to stop instead of waiting on `Loop`'s internal ticker.)

- [ ] **Step 2: Run it for real** (⚠️ needs `make compose-up` and Docker/Colima running)

Run: `go test -tags=integration ./internal/coordinator/... -run TestLoadTest -v -timeout 5m`
Expected: PASS. **Copy the actual logged jobs/sec number verbatim into Task 5.7's PROGRESS.md entry** —
same resume-honesty rule as Part 4's benchmark.

- [ ] **Step 3: Commit**

Commit message: `test: add load-test harness — 25 real workers, real docker run execution, measure sustained jobs/sec`

---

### Task 5.7: Documentation

**Files:** Modify `README.md`, `PROGRESS.md`, `CLAUDE.md`.

- [ ] **Step 1:** README: brief "Running multiple workers" note (just run `make run-worker` more than
  once — each generates its own ID automatically, no config needed, unlike the raft cluster's static
  config file).

- [ ] **Step 2:** `PROGRESS.md`: full Part 5 commit log, **plus the real load-test numbers from Task
  5.6 Step 2, copied verbatim** — worker count, job count, elapsed time, measured jobs/sec.

- [ ] **Step 3:** `CLAUDE.md`: update the "Where things stand" section. Per the roadmap, **this is the
  point the project counts as resume-done** — Branch 5 completing the full fault-tolerant core (HTTP →
  gRPC → event log → Raft → scheduling). Update the resume-done rule's wording if it still says "once
  Branch 5 lands" as a future thing rather than a completed fact.

- [ ] **Step 4: Commit**

Commit message: `docs: update PROGRESS.md for Part 5 with real load-test results`

**Part 5 complete — the core distributed system is done.** Branch 6 (REST/MCP polish + observability) is
still real, valuable work, but per this project's own resume-honesty framing, everything that makes this
a distributed-systems story rather than a CRUD app now exists and is proven.

**PR title:** `Part 5: multi-worker scheduling with heartbeat failure detection`
**PR description points:** worker identity via `worker_id` on every poll (which doubles as a heartbeat —
no separate RPC); `WorkerRegistry` + `Reaper` requeue a dead worker's in-flight jobs, leader-gated and
safe-by-construction against a freshly-elected leader with no heartbeat history; automated
scheduling/reassignment test; real load test with N real workers executing real `docker run` jobs,
measured throughput reported honestly in `PROGRESS.md` (link the actual number here once Task 5.6 runs).

---

## Self-review

- **Spec/roadmap coverage:** all 6 roadmap commit messages for Part 5 present; Task 5.1 (worker identity)
  added as real groundwork not in the original list, same precedent as Part 4's Task 4.1; "heartbeat
  timeout detection" and "reassign in-flight jobs" merged into one Task 5.4 commit since they're one
  mechanism in this design, not two — noted there rather than forcing an artificial split.
- **Placeholder scan:** two spots flagged explicitly as needing a small implementation decision during
  the session rather than guessed now — `RequeueRunning`'s loop-cleanup comment (Task 5.3) and the
  "claim-only" test seam (Task 5.5) — both are genuinely minor wiring choices, not open design questions.
- **Type/interface consistency:** `job.Store`'s `ClaimNext`/`RequeueRunning` signatures are defined once
  (Task 5.1/5.3) and consumed identically by `eventlog.Store` (the only wrapper) and every test;
  `Event.WorkerID` is written once (`ClaimNext`'s publish) and read once (`Rebuild`/`ApplyEvent`'s
  `EventJobClaimed` case) — no duplicate parsing logic.
- **Known limitations named, not hidden:** the replay-vs-live queue-ordering discrepancy for requeued
  jobs (design section); apply-then-publish's no-rollback-on-publish-failure carried forward from Part 3
  without re-litigating it; the load test's job count (500, not 100,000) is a deliberate "modest but
  real beats large but synthetic" choice, not a shortfall — nothing stops re-running the same harness
  with a larger `numJobs` later if a bigger sample is wanted.
- **Resume-honesty check:** Task 5.6's load-test number is measured, not assumed — per the earlier
  conversation in this session about what throughput is actually plausible (low tens of jobs/sec given
  sequential per-worker `docker run`, not hundreds without added concurrency), this plan does not
  pre-commit to any throughput figure anywhere.
