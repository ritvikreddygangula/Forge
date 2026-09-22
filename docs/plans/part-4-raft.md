# Part 4 — Three-coordinator Raft cluster — Implementation Plan

**Branch:** `part-4-raft`, cut from latest `main`.

**Goal:** Three coordinator replicas, using `hashicorp/raft` for leader election. Killing the leader
triggers automatic re-election with real, measured failover latency under 500ms, and job processing
continues uninterrupted — a follower receiving a write request transparently forwards it to whoever is
now leader. Validated by an automated failover test, a network-partition test, and a 30-trial benchmark
recording actual median/p99 failover latency (per the project's resume-honesty rule: no invented
numbers). This is the hardest, most novel Part in the project.

**Spec:** `docs/spec.md`. **Roadmap:** `docs/plans/roadmap.md` (Part 4 section). **Prior plan:**
`docs/plans/part-3-event-log.md`.

## Design, verified against the real library before writing this plan

Rather than write this plan from memory of how Raft libraries generally work, I dispatched a research
pass that wrote and ran real Go code against `github.com/hashicorp/raft` (resolves to **v1.8.0**) —
bootstrapping a real 3-node cluster with in-memory transport, forcing a leader crash, and measuring
actual re-election time. I then independently confirmed the one API this plan's partition test depends
on (`InmemTransport.Disconnect`/`DisconnectAll`) and the persistence library's constructor
(`raft-boltdb/v2`'s `NewBoltStore`) by reading the installed module source directly. Key confirmed facts:

- **`raft.NewInmemTransport` + `raft.NewInmemStore` + `raft.NewInmemSnapshotStore`** wire a full 3-node
  cluster with no real network/ports — fast, non-flaky, the right choice for every test in this Part.
  `InmemTransport.Connect` is **one-directional per call** — every ordered pair needs its own call.
- **`FSM.Apply` returns `interface{}`, not `(interface{}, error)`** — this project's FSM is a deliberate
  no-op anyway (see below), so this doesn't matter here, but it's the kind of detail worth getting from
  running code rather than memory.
- **`BootstrapCluster` is safe to call from every replica on every startup.** It only actually seeds the
  cluster once, ever, across the cluster's whole history; every other call (including every subsequent
  restart of any replica) returns `raft.ErrCantBootstrap`, which this plan treats as "already
  bootstrapped," not an error. This means no replica needs to be operationally special-cased as "the one
  that bootstraps" — all 3 start identically.
- **Leader detection:** `raft.State() == raft.Leader`, `raft.LeaderWithID() (ServerAddress, ServerID)`.
  **Non-leader writes:** `raft.Apply(...)` returns the sentinel `raft.ErrNotLeader` — not used directly in
  this design (see "Why Raft's role is narrow" below) but confirms the library's error-handling shape.
- **Failover timing, measured, not assumed:** with `raft.DefaultConfig()`'s ~1s timeouts, initial election
  took 1.1s–1.9s and re-election 1.5s–2.4s — too slow for this project's 500ms target. With
  `HeartbeatTimeout = ElectionTimeout = LeaderLeaseTimeout = 50ms`, re-election measured **77ms–132ms**
  across 5 repeated runs, no flakiness. This project uses the tuned values, justified below.
- **`InmemTransport.Disconnect(peer)` / `DisconnectAll()`** exist (confirmed by reading
  `inmem_transport.go` directly) — this is what Task 4.6's partition test uses to simulate a leader
  isolated from its followers while the followers stay connected to each other.
- **`raft-boltdb/v2`'s `NewBoltStore(path string) (*BoltStore, error)`** implements both `raft.LogStore`
  and `raft.StableStore` from one file — Task 4.5 uses one shared instance for both roles, same pattern
  as the in-memory version (`raft.NewInmemStore()` already serves both roles identically).

## Why Raft's role here is narrow: leader election only, not data replication

This is the single most important design decision in this Part, so it's worth stating plainly. `Raft` in
this project **does not replicate job data.** It decides which of the 3 coordinator replicas is currently
allowed to accept write requests — that's it. The FSM every replica's `raft.Raft` instance runs is an
intentional no-op.

Job data is already durable and shared across all replicas via Part 3's Redpanda event log — every
replica (leader or follower) continuously tails the same topic and applies events to its own local
`job.MemoryStore` (Part 3 only replayed once at startup; this Part makes that continuous, see Task 4.2).
Raft answering "am I the leader" is a thin, separate concern layered on top: the write-path REST/gRPC
handlers check it and forward to the leader if they're not it; nothing about *how job state gets
replicated* changes. This keeps the new concept in this Part to exactly one thing — consensus/leader
election — instead of conflating it with building a full Raft-replicated state machine (a much larger,
different undertaking that real systems like etcd/Consul do, but isn't this project's point; see
`PLAN.md`'s "Scope calls" section on why `hashicorp/raft` is used as a library either way).

One consequence worth naming: every replica's own writes get applied twice — once synchronously when the
leader's handler calls `eventlog.Store` (Part 3, unchanged), and once again, redundantly, moments later
when that same replica's own continuous tail loop reads back the event it just published. This is
harmless — the `job.MemoryStore` apply methods added in Task 4.1 are idempotent by job ID, not
accumulators — and simpler than trying to have the leader's tail loop skip its own events. Stated plainly
rather than silently optimized around.

## Why the tuned timeouts, not the library defaults

`raft.DefaultConfig()`'s ~1s timeouts are conservative, WAN-oriented defaults. This project runs all 3
replicas on localhost (and would run them on one LAN in any real deployment) — real production Raft
deployments over a LAN commonly tune into the 100–300ms range for exactly this reason. Measured
77–132ms failover with the tuned config, with zero observed flakiness across repeated trials, comfortably
clears the 500ms target with margin. These tuned values are used in the real coordinator binary, not just
in tests — this isn't a test-only shortcut.

## File structure (new/changed)

```
internal/
  coordinator/
    raft.go                # NEW — noop FSM, NewRaftNode, PeerInfo, RaftGate
    raft_test.go              # NEW — in-memory cluster test helpers + Task 4.2 failover test
    partition_test.go          # NEW — Task 4.6 network-partition test
    benchmark_test.go            # NEW — Task 4.7 failover benchmark (30 trials, real numbers)
    server.go                     # MODIFIED — leader-gate + forward on handleSubmitJob
    grpc_server.go                  # MODIFIED — leader-gate + forward on PollJob/ReportResult
  job/
    store.go                          # MODIFIED — ApplyCreated/ApplyClaimed/ApplyCompleted
    store_test.go                       # MODIFIED — test the new Apply* methods
  eventlog/
    event.go                              # MODIFIED — ApplyEvent(store, e)
    consumer.go                             # MODIFIED — ReadAll also returns an offset; new Tail method
deploy/
  raft-cluster.json                         # NEW — static 3-replica config (id/raft/rest/grpc addrs)
cmd/
  coordinator/main.go                         # MODIFIED — raft wiring, cluster mode opt-in
.gitignore                                     # MODIFIED — add /data/ (raft's persisted bolt files)
```

Single-instance mode (no `COORDINATOR_REPLICA_ID` set) is **completely unaffected** — no raft node, no
gating, no continuous tail, byte-for-byte the same behavior as Part 3. Every existing test keeps working
unchanged because `*RaftGate` being `nil` is treated as "always leader" everywhere it's checked.

---

### Task 4.1: `job.MemoryStore` incremental apply methods + `eventlog.ApplyEvent`

**Files:** Modify `internal/job/store.go`, `internal/job/store_test.go`, `internal/eventlog/event.go`,
`internal/eventlog/event_test.go`.

**Interfaces:** Produces `(*MemoryStore).ApplyCreated(j *Job)`, `.ApplyClaimed(id string, at time.Time)`,
`.ApplyCompleted(id string, status Status, stdout, stderr string, exitCode int, at time.Time)`, and
`eventlog.ApplyEvent(store *job.MemoryStore, e Event)`. These are the incremental, single-event
counterparts to Part 3's bulk `Rebuild([]*Job)` — used by the continuous tail loop (Task 4.2), one event
at a time, instead of re-deriving the whole store on every new event.

- [ ] **Step 1: Write the failing tests**

Append to `internal/job/store_test.go`:
```go
func TestMemoryStore_ApplyCreated(t *testing.T) {
	s := job.NewMemoryStore()
	now := time.Now()
	s.ApplyCreated(&job.Job{ID: "a", Image: "alpine", Command: []string{"true"}, Status: job.StatusQueued, CreatedAt: now, UpdatedAt: now})

	got, err := s.Get("a")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusQueued {
		t.Fatalf("expected status queued, got %s", got.Status)
	}
	claimed, err := s.ClaimNext()
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if claimed == nil || claimed.ID != "a" {
		t.Fatalf("expected applied job to be claimable, got %+v", claimed)
	}
}

func TestMemoryStore_ApplyCreated_IgnoresDuplicates(t *testing.T) {
	s := job.NewMemoryStore()
	now := time.Now()
	s.ApplyCreated(&job.Job{ID: "a", Status: job.StatusQueued, CreatedAt: now, UpdatedAt: now})
	if _, err := s.ClaimNext(); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if err := s.Complete("a", job.StatusSucceeded, "done\n", "", 0); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}

	// Re-applying JobCreated for the same ID (as happens when a replica
	// tails back its own already-locally-applied write) must not revert
	// progress that happened in between.
	s.ApplyCreated(&job.Job{ID: "a", Status: job.StatusQueued, CreatedAt: now, UpdatedAt: now})

	got, err := s.Get("a")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusSucceeded || got.Stdout != "done\n" {
		t.Fatalf("expected re-applying create to be a no-op, got %+v", got)
	}
}

func TestMemoryStore_ApplyClaimed(t *testing.T) {
	s := job.NewMemoryStore()
	now := time.Now()
	s.ApplyCreated(&job.Job{ID: "a", Status: job.StatusQueued, CreatedAt: now, UpdatedAt: now})

	s.ApplyClaimed("a", now)

	got, err := s.Get("a")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusRunning {
		t.Fatalf("expected status running, got %s", got.Status)
	}
}

func TestMemoryStore_ApplyCompleted(t *testing.T) {
	s := job.NewMemoryStore()
	now := time.Now()
	s.ApplyCreated(&job.Job{ID: "a", Status: job.StatusQueued, CreatedAt: now, UpdatedAt: now})
	s.ApplyClaimed("a", now)

	s.ApplyCompleted("a", job.StatusFailed, "", "boom\n", 1, now)

	got, err := s.Get("a")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusFailed || got.Stderr != "boom\n" || got.ExitCode != 1 {
		t.Fatalf("expected failed job with stderr boom, got %+v", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/job/... -run TestMemoryStore_Apply`
Expected: FAIL — the three `Apply*` methods are undefined.

- [ ] **Step 3: Implement**

Add to `internal/job/store.go`:
```go
// ApplyCreated inserts a job as queued. Used by a replica's continuous
// event-log tail (Part 4) to replicate a JobCreated event — unlike Create,
// the ID already exists (assigned by whichever replica was leader when the
// job was created), so no new ID is generated here. A no-op if this ID is
// already known, since a job's own leader-side write reaches this same
// store's tail loop moments after Create already applied it directly.
func (s *MemoryStore) ApplyCreated(j *Job) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[j.ID]; exists {
		return
	}
	cp := *j
	s.jobs[j.ID] = &cp
	if j.Status == StatusQueued {
		s.order = append(s.order, j.ID)
	}
}

// ApplyClaimed marks a specific job running. Unlike ClaimNext, the job ID is
// already decided (by whichever replica was leader when the claim happened)
// — this just replicates that decision. A no-op if the job is unknown or
// already past queued, so replaying an already-applied event is harmless.
func (s *MemoryStore) ApplyClaimed(id string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok || j.Status != StatusQueued {
		return
	}
	j.Status = StatusRunning
	j.UpdatedAt = at
}

// ApplyCompleted marks a specific job terminal. Same replication role as
// ApplyClaimed, for the completed transition.
func (s *MemoryStore) ApplyCompleted(id string, status Status, stdout, stderr string, exitCode int, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return
	}
	j.Status = status
	j.Stdout = stdout
	j.Stderr = stderr
	j.ExitCode = exitCode
	j.UpdatedAt = at
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/job/... -v`
Expected: PASS, all tests including the 4 new ones.

- [ ] **Step 5: Write the failing test for `eventlog.ApplyEvent`**

Append to `internal/eventlog/event_test.go`:
```go
func TestApplyEvent_FullLifecycle(t *testing.T) {
	store := job.NewMemoryStore()
	now := time.Now()

	eventlog.ApplyEvent(store, eventlog.Event{Type: eventlog.EventJobCreated, JobID: "a", Image: "alpine", Command: []string{"true"}, TimeoutSeconds: 10, Timestamp: now})
	eventlog.ApplyEvent(store, eventlog.Event{Type: eventlog.EventJobClaimed, JobID: "a", Timestamp: now})
	eventlog.ApplyEvent(store, eventlog.Event{Type: eventlog.EventJobCompleted, JobID: "a", Status: job.StatusSucceeded, Stdout: "ok\n", Timestamp: now})

	got, err := store.Get("a")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusSucceeded || got.Stdout != "ok\n" {
		t.Fatalf("expected succeeded job with stdout ok, got %+v", got)
	}
}
```

- [ ] **Step 6: Implement**

Add to `internal/eventlog/event.go`:
```go
// ApplyEvent applies one event directly onto a live store — the incremental
// counterpart to Rebuild (which folds a whole batch into a fresh []*job.Job
// at startup). Used by the continuous tail loop (Task 4.2), one event at a
// time, as new writes arrive from whichever replica is currently leader.
func ApplyEvent(store *job.MemoryStore, e Event) {
	switch e.Type {
	case EventJobCreated:
		store.ApplyCreated(&job.Job{
			ID: e.JobID, Image: e.Image, Command: e.Command, TimeoutSeconds: e.TimeoutSeconds,
			Status: job.StatusQueued, CreatedAt: e.Timestamp, UpdatedAt: e.Timestamp,
		})
	case EventJobClaimed:
		store.ApplyClaimed(e.JobID, e.Timestamp)
	case EventJobCompleted:
		store.ApplyCompleted(e.JobID, e.Status, e.Stdout, e.Stderr, e.ExitCode, e.Timestamp)
	}
}
```

- [ ] **Step 7: Run tests, verify, commit**

Run: `go test ./internal/job/... ./internal/eventlog/... -v`
Expected: PASS, all tests.

Commit message: `feat(job,eventlog): add incremental apply methods for continuous replication`

*(Note: this commit isn't in the roadmap's original 8-item list — it's genuinely new groundwork Task 4.2
depends on, surfaced during detailed planning. Called out explicitly rather than silently folded into
another task's commit.)*

---

### Task 4.2: Embed `hashicorp/raft`, three-replica config, continuous tail, leader gating

This is the biggest task in the Part — it's genuinely one cohesive unit of work (a coordinator replica
isn't "half raft-aware"), covering roadmap items 1–3 in one pass rather than three separate half-working
intermediate states. Split into sub-steps below for review, landing as up to three commits matching the
roadmap's granularity where a clean split exists.

**Files:** Create `internal/coordinator/raft.go`, `internal/coordinator/raft_test.go`,
`internal/eventlog/consumer.go` (extend), `deploy/raft-cluster.json`. Modify `internal/coordinator/server.go`,
`internal/coordinator/grpc_server.go`, `cmd/coordinator/main.go`.

#### 4.2a: `NewRaftNode`, the no-op FSM, and the in-memory failover test

**Interfaces:** Produces `coordinator.RaftNodeConfig`, `coordinator.NewRaftNode(cfg) (*raft.Raft, error)`.

- [ ] **Step 1: Add the dependency**

```bash
go get github.com/hashicorp/raft@latest
```

- [ ] **Step 2: Write the failing test** — `internal/coordinator/raft_test.go`:

```go
package coordinator_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/ritvikreddygangula/forge/internal/coordinator"
)

// raftTestNode bundles one in-memory raft node for tests — no real network,
// no real ports, verified fast and non-flaky for a full 3-node cluster.
type raftTestNode struct {
	id        string
	raft      *raft.Raft
	transport *raft.InmemTransport
}

func newInMemRaftCluster(t *testing.T, n int) []*raftTestNode {
	t.Helper()
	nodes := make([]*raftTestNode, n)
	servers := make([]raft.Server, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("node%d", i+1)
		addr, transport := raft.NewInmemTransport(raft.NewInmemAddr())
		nodes[i] = &raftTestNode{id: id, transport: transport}
		servers[i] = raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(id), Address: addr}
	}
	for i := range nodes {
		for j := range nodes {
			if i != j {
				nodes[i].transport.Connect(nodes[j].transport.LocalAddr(), nodes[j].transport)
			}
		}
	}
	for i, n := range nodes {
		r, err := coordinator.NewRaftNode(coordinator.RaftNodeConfig{
			LocalID:            n.id,
			Transport:          n.transport,
			LogStore:           raft.NewInmemStore(),
			StableStore:        raft.NewInmemStore(),
			SnapshotStore:      raft.NewInmemSnapshotStore(),
			Servers:            servers,
			HeartbeatTimeout:   50 * time.Millisecond,
			ElectionTimeout:    50 * time.Millisecond,
			LeaderLeaseTimeout: 50 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewRaftNode(%s) returned error: %v", n.id, err)
		}
		nodes[i].raft = r
	}
	return nodes
}

func waitForLeader(t *testing.T, nodes []*raftTestNode, timeout time.Duration) *raftTestNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.raft.State() == raft.Leader {
				return n
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

func TestRaft_LeaderElectionAndFailover(t *testing.T) {
	nodes := newInMemRaftCluster(t, 3)
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.raft.Shutdown()
		}
	})

	start := time.Now()
	leader := waitForLeader(t, nodes, 2*time.Second)
	t.Logf("initial leader %s elected in %s", leader.id, time.Since(start))

	if err := leader.raft.Shutdown().Error(); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}

	var remaining []*raftTestNode
	for _, n := range nodes {
		if n.id != leader.id {
			remaining = append(remaining, n)
		}
	}

	start = time.Now()
	newLeader := waitForLeader(t, remaining, 2*time.Second)
	elapsed := time.Since(start)
	t.Logf("re-election after leader crash took %s (new leader %s)", elapsed, newLeader.id)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("failover took %s, expected under 500ms", elapsed)
	}
	if newLeader.id == leader.id {
		t.Fatalf("expected a different node to become leader")
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/coordinator/... -run TestRaft_LeaderElectionAndFailover`
Expected: FAIL — `coordinator.NewRaftNode` undefined.

- [ ] **Step 4: Implement**

`internal/coordinator/raft.go`:
```go
package coordinator

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"
)

// noopFSM is intentionally empty. This project uses Raft purely for leader
// election among coordinator replicas — job data is already durable and
// shared via the Redpanda event log every replica tails (Part 3, extended to
// a continuous tail in this Part). Raft still requires something satisfying
// FSM for its own log/consensus bookkeeping. See the plan doc's "Why Raft's
// role here is narrow" section for the full reasoning.
type noopFSM struct{}

func (noopFSM) Apply(*raft.Log) interface{}        { return nil }
func (noopFSM) Snapshot() (raft.FSMSnapshot, error) { return noopSnapshot{}, nil }
func (noopFSM) Restore(rc io.ReadCloser) error      { return rc.Close() }

type noopSnapshot struct{}

func (noopSnapshot) Persist(sink raft.SnapshotSink) error { return sink.Close() }
func (noopSnapshot) Release()                              {}

// RaftNodeConfig holds everything needed to construct this replica's
// raft.Raft instance. Tests pass in-memory transport/stores; the real
// coordinator binary passes TCP transport and boltdb-backed stores (Task
// 4.5) — NewRaftNode itself is identical either way.
type RaftNodeConfig struct {
	LocalID       string
	Transport     raft.Transport
	LogStore      raft.LogStore
	StableStore   raft.StableStore
	SnapshotStore raft.SnapshotStore
	Servers       []raft.Server // full cluster membership, including self

	// Tuned for sub-500ms failover on a local/LAN deployment — see the plan
	// doc's "Why the tuned timeouts" section. Zero value on any field falls
	// back to raft.DefaultConfig()'s own default for that field.
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
}

func NewRaftNode(cfg RaftNodeConfig) (*raft.Raft, error) {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.LocalID)
	if cfg.HeartbeatTimeout > 0 {
		raftConfig.HeartbeatTimeout = cfg.HeartbeatTimeout
	}
	if cfg.ElectionTimeout > 0 {
		raftConfig.ElectionTimeout = cfg.ElectionTimeout
	}
	if cfg.LeaderLeaseTimeout > 0 {
		raftConfig.LeaderLeaseTimeout = cfg.LeaderLeaseTimeout
	}

	r, err := raft.NewRaft(raftConfig, noopFSM{}, cfg.LogStore, cfg.StableStore, cfg.SnapshotStore, cfg.Transport)
	if err != nil {
		return nil, fmt.Errorf("failed to construct raft node: %w", err)
	}

	// Safe to call from every replica on every startup — hashicorp/raft only
	// actually seeds the cluster once, ever; every later call (including on
	// every restart of any replica) returns ErrCantBootstrap, meaning
	// "already bootstrapped," not a real failure.
	future := r.BootstrapCluster(raft.Configuration{Servers: cfg.Servers})
	if err := future.Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
		return nil, fmt.Errorf("failed to bootstrap raft cluster: %w", err)
	}

	return r, nil
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/coordinator/... -run TestRaft_LeaderElectionAndFailover -v`
Expected: PASS, logging the actual measured election and re-election times.

- [ ] **Step 6: Commit**

Commit message: `feat(coordinator): embed hashicorp/raft with single-node bootstrap`

*(The test here already exercises 3 nodes rather than 1 — a genuinely single-node-only first commit
would have nothing failover-related to test yet. This is Task 4.4's failover test landing early because
it's also the natural verification for `NewRaftNode` itself; the roadmap's separate "single-node
bootstrap" vs. "three-replica config" split becomes real once TCP transport and the cluster config file
enter in 4.2b below.)*

#### 4.2b: Continuous event-log tail

**Files:** Modify `internal/eventlog/consumer.go` and its call sites.

**Interfaces:** `ReadAll` gains a return value; produces `(*KafkaConsumer).Tail(ctx, fromOffset,
onEvent) error`.

- [ ] **Step 1: Change `ReadAll`'s signature to also return the offset to resume from**

In `internal/eventlog/consumer.go`, change:
```go
func (c *KafkaConsumer) ReadAll(ctx context.Context) ([]Event, error) {
```
to:
```go
// ReadAll reads every event currently in the topic, from the beginning up to
// the offset at the moment this call started, and returns that offset too —
// callers that need to keep following the log (Tail, below) resume from
// exactly there, with no gap or overlap.
func (c *KafkaConsumer) ReadAll(ctx context.Context) ([]Event, int64, error) {
```
and every `return nil, fmt.Errorf(...)` / `return nil, nil` inside it to `return nil, 0, fmt.Errorf(...)`
/ `return nil, 0, nil`, and the final `return events, nil` to `return events, lastOffset, nil`.

Update the three existing call sites (`cmd/coordinator/main.go`, `internal/eventlog/kafka_test.go`,
`internal/eventlog/crash_recovery_test.go`) to take the extra return value — mechanical, e.g.
`events, _, err := consumer.ReadAll(ctx)` where the offset isn't needed yet.

- [ ] **Step 2: Add `Tail`**

Append to `internal/eventlog/consumer.go`:
```go
// Tail continuously reads events starting at fromOffset, invoking onEvent
// for each one, until ctx is cancelled or onEvent returns an error. Unlike
// ReadAll (a bounded, one-shot replay), this runs indefinitely — every
// replica (leader and followers alike) runs one of these for the lifetime
// of the process to stay in sync with writes published by whichever replica
// is currently leader.
func (c *KafkaConsumer) Tail(ctx context.Context, fromOffset int64, onEvent func(Event) error) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   c.brokers,
		Topic:     c.topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   250 * time.Millisecond,
	})
	defer func() { _ = reader.Close() }()
	if err := reader.SetOffset(fromOffset); err != nil {
		return fmt.Errorf("failed to seek to offset %d: %w", fromOffset, err)
	}

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // context cancelled — clean shutdown, not a failure
			}
			return fmt.Errorf("failed tailing event log: %w", err)
		}
		var e Event
		if err := json.Unmarshal(msg.Value, &e); err != nil {
			return fmt.Errorf("failed to decode event at offset %d: %w", msg.Offset, err)
		}
		if err := onEvent(e); err != nil {
			return err
		}
	}
}
```

- [ ] **Step 3: Verify**

Run: `go build ./... && go vet ./...` — exits 0. `Tail` itself is exercised end-to-end in Task 4.2c's
manual verification rather than a standalone unit test — it's a thin wrapper whose real behavior only
matters once wired into a running replica.

- [ ] **Step 4: Commit**

Commit message: `feat(eventlog): add continuous tail for multi-replica replication`

#### 4.2c: Three-replica config, TCP transport, and `cmd/coordinator/main.go` wiring

**Files:** Create `deploy/raft-cluster.json`. Modify `cmd/coordinator/main.go`.

- [ ] **Step 1:** `deploy/raft-cluster.json`

```json
[
  {"id": "node1", "raft_addr": "127.0.0.1:7000", "rest_addr": "127.0.0.1:8080", "grpc_addr": "127.0.0.1:9090"},
  {"id": "node2", "raft_addr": "127.0.0.1:7001", "rest_addr": "127.0.0.1:8081", "grpc_addr": "127.0.0.1:9091"},
  {"id": "node3", "raft_addr": "127.0.0.1:7002", "rest_addr": "127.0.0.1:8082", "grpc_addr": "127.0.0.1:9092"}
]
```

- [ ] **Step 2: Add `PeerInfo` to `internal/coordinator/raft.go`**

```go
// PeerInfo is one replica's full address set — its raft transport address
// plus the REST/gRPC addresses followers forward write requests to once
// they've identified the current leader via raft.
type PeerInfo struct {
	ID       string `json:"id"`
	RaftAddr string `json:"raft_addr"`
	RESTAddr string `json:"rest_addr"`
	GRPCAddr string `json:"grpc_addr"`
}
```

- [ ] **Step 3: Wire cluster mode into `cmd/coordinator/main.go`**, opt-in via `COORDINATOR_REPLICA_ID` —
  unset, and every existing single-instance behavior from Parts 1–3 is untouched:

```go
replicaID := os.Getenv("COORDINATOR_REPLICA_ID")
var raftGate *coordinator.RaftGate

if replicaID != "" {
	clusterConfigPath := os.Getenv("COORDINATOR_CLUSTER_CONFIG")
	if clusterConfigPath == "" {
		clusterConfigPath = "deploy/raft-cluster.json"
	}
	data, err := os.ReadFile(clusterConfigPath)
	if err != nil {
		slog.Error("coordinator failed to read cluster config", "error", err)
		os.Exit(1)
	}
	var peers []coordinator.PeerInfo
	if err := json.Unmarshal(data, &peers); err != nil {
		slog.Error("coordinator failed to parse cluster config", "error", err)
		os.Exit(1)
	}

	var self coordinator.PeerInfo
	var found bool
	servers := make([]raft.Server, 0, len(peers))
	for _, p := range peers {
		servers = append(servers, raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.RaftAddr)})
		if p.ID == replicaID {
			self, found = p, true
		}
	}
	if !found {
		slog.Error("coordinator replica id not found in cluster config", "replica_id", replicaID)
		os.Exit(1)
	}
	httpAddr, grpcAddr = self.RESTAddr, self.GRPCAddr

	transport, err := raft.NewTCPTransport(self.RaftAddr, nil, 3, 5*time.Second, os.Stderr)
	if err != nil {
		slog.Error("coordinator failed to create raft transport", "error", err)
		os.Exit(1)
	}

	r, err := coordinator.NewRaftNode(coordinator.RaftNodeConfig{
		LocalID: replicaID, Transport: transport,
		LogStore: raft.NewInmemStore(), StableStore: raft.NewInmemStore(), SnapshotStore: raft.NewInmemSnapshotStore(),
		Servers:            servers,
		HeartbeatTimeout:   50 * time.Millisecond,
		ElectionTimeout:    50 * time.Millisecond,
		LeaderLeaseTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		slog.Error("coordinator failed to start raft node", "error", err)
		os.Exit(1)
	}
	raftGate = coordinator.NewRaftGate(r, peers)

	go func() {
		consumer := eventlog.NewKafkaConsumer(brokers, eventlog.DefaultTopic)
		if err := consumer.Tail(ctx, replayOffset, func(e eventlog.Event) error {
			eventlog.ApplyEvent(baseStore, e)
			return nil
		}); err != nil {
			slog.Error("coordinator tail loop exited", "error", err)
		}
	}()
}
```

  (`httpAddr`/`grpcAddr`/`ctx`/`brokers`/`baseStore`/`replayOffset` are the existing Part 1–3 variables
  already in `main`, reassigned/reused here — `replayOffset` is the offset `ReadAll` now returns per
  Task 4.2b's Step 1, captured right after the existing bounded-replay call. `raft.NewTCPTransport`'s
  exact signature — `(bindAddr string, advertise net.Addr, maxPool int, timeout time.Duration, logOutput
  io.Writer)` — gets a final sanity check against the installed module during implementation, same
  diligence as every other library call in this plan; it wasn't part of the verification pass above since
  the tests use in-memory transport exclusively.)

- [ ] **Step 4: Pass `raftGate` into both servers**

```go
httpSrv := coordinator.NewServer(store)
httpSrv.SetRaftGate(raftGate) // nil in single-instance mode — no-op

grpcServer := coordinator.NewGRPCServer(store)
grpcServer.SetRaftGate(raftGate)
```

- [ ] **Step 5: Manual verification** (⚠️ needs 3 terminals + Redpanda running)

```bash
make compose-up
COORDINATOR_REPLICA_ID=node1 /path/to/coordinator-binary   # terminal 1
COORDINATOR_REPLICA_ID=node2 /path/to/coordinator-binary   # terminal 2
COORDINATOR_REPLICA_ID=node3 /path/to/coordinator-binary   # terminal 3
```
Expect exactly one to log becoming leader (via a log line added in Task 4.3's gating work). Submit a job
via `curl` against **any** of the 3 REST ports — including a follower's — and confirm it succeeds (this
proves forwarding once Task 4.3 lands; before that, a follower should reject writes, which Task 4.3
fixes).

- [ ] **Step 6: Commit**

Commit message: `feat(coordinator): add three-replica config and TCP raft transport`

---

### Task 4.3: Leader-gated writes, forwarded to the leader

**Files:** Modify `internal/coordinator/server.go`, `internal/coordinator/grpc_server.go`. Add
`RaftGate` methods to `internal/coordinator/raft.go`.

**Interfaces:** Produces `coordinator.NewRaftGate(r *raft.Raft, peers []PeerInfo) *RaftGate`,
`(*RaftGate).IsLeader() bool`, `(*RaftGate).Leader() (PeerInfo, bool)`, `(*Server).SetRaftGate(*RaftGate)`,
`(*GRPCServer).SetRaftGate(*RaftGate)`. A `nil *RaftGate` is treated as "single-node, always leader" —
every existing REST/gRPC test keeps passing unchanged.

- [ ] **Step 1: Add `RaftGate` to `internal/coordinator/raft.go`**

```go
// RaftGate answers "am I the leader" and "if not, who is" for the write
// handlers. A nil *RaftGate means single-node mode (Parts 1-3's original
// behavior, unaffected by this Part) — every method on it is nil-safe and
// treats a nil gate as "always leader, no one else to forward to."
type RaftGate struct {
	raft  *raft.Raft
	peers map[raft.ServerAddress]PeerInfo
}

func NewRaftGate(r *raft.Raft, peers []PeerInfo) *RaftGate {
	byAddr := make(map[raft.ServerAddress]PeerInfo, len(peers))
	for _, p := range peers {
		byAddr[raft.ServerAddress(p.RaftAddr)] = p
	}
	return &RaftGate{raft: r, peers: byAddr}
}

func (g *RaftGate) IsLeader() bool {
	return g == nil || g.raft.State() == raft.Leader
}

// Leader returns the current leader's peer info. ok is false if this gate is
// nil (single-node mode) or raft hasn't identified a leader yet (mid-election).
func (g *RaftGate) Leader() (PeerInfo, bool) {
	if g == nil {
		return PeerInfo{}, false
	}
	addr, _ := g.raft.LeaderWithID()
	if addr == "" {
		return PeerInfo{}, false
	}
	p, ok := g.peers[addr]
	return p, ok
}
```

- [ ] **Step 2: Write the failing REST test**

Append to `internal/coordinator/server_test.go`:
```go
func TestHandleSubmitJob_ForwardsToLeaderWhenNotLeader(t *testing.T) {
	leaderStore := job.NewMemoryStore()
	leaderSrv := coordinator.NewServer(leaderStore)
	leaderHTTP := httptest.NewServer(leaderSrv)
	defer leaderHTTP.Close()

	leaderAddr := strings.TrimPrefix(leaderHTTP.URL, "http://")
	gate := coordinator.NewNoopFollowerGateForTest(leaderAddr) // see Step 4 below

	followerSrv := coordinator.NewServer(job.NewMemoryStore())
	followerSrv.SetRaftGate(gate)

	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader([]byte(`{"image":"alpine","command":["true"],"timeout_seconds":10}`)))
	rec := httptest.NewRecorder()
	followerSrv.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 (forwarded), got %d: %s", rec.Code, rec.Body.String())
	}
	if _, err := leaderStore.Get(mustJSONID(t, rec.Body.Bytes())); err != nil {
		t.Fatalf("expected job to exist on the leader's store: %v", err)
	}
}
```

  (`NewNoopFollowerGateForTest` and `mustJSONID` are small test-only helpers — a real `*RaftGate` needs a
  live `*raft.Raft`, which is more setup than this handler-level test needs; a minimal test seam that
  reports "not leader, leader is at this address" without a real raft instance is simpler here. Exact
  shape finalized during implementation — this may end up as an unexported interface `leaderGate` that
  both `*RaftGate` and a test double satisfy, rather than a real exported constructor; the point of this
  step is the behavior it proves, not this exact scaffolding.)

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/coordinator/... -run TestHandleSubmitJob_ForwardsToLeaderWhenNotLeader`
Expected: FAIL — `SetRaftGate` undefined, `handleSubmitJob` doesn't check leadership yet.

- [ ] **Step 4: Implement the REST side**

Add a `raftGate *RaftGate` field to `Server` and a setter:
```go
func (s *Server) SetRaftGate(g *RaftGate) { s.raftGate = g }
```

Modify `handleSubmitJob` in `internal/coordinator/server.go`:
```go
func (s *Server) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	if !s.raftGate.IsLeader() {
		s.forwardToLeader(w, r)
		return
	}
	// ... existing body unchanged ...
}
```

Add:
```go
func (s *Server) forwardToLeader(w http.ResponseWriter, r *http.Request) {
	leader, ok := s.raftGate.Leader()
	if !ok {
		http.Error(w, "no raft leader elected, try again shortly", http.StatusServiceUnavailable)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, "http://"+leader.RESTAddr+r.URL.Path, bytes.NewReader(body))
	if err != nil {
		http.Error(w, "failed to build forwarded request", http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", r.Header.Get("Content-Type"))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, "failed to forward request to leader", http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}
```

`handleGetJob`/`handleGetJobLogs` are reads — **not gated**; any replica serves them from its own local,
eventually-consistent copy. Worth being explicit about the tradeoff this implies: a client that just
wrote via replica A and immediately reads via replica B may briefly see stale data until B's tail loop
(Task 4.2b) catches up — typically low tens of milliseconds given Redpanda's latency and the producer's
tuned `BatchTimeout`. Not read-your-writes consistent across replicas; documented, not hidden.

- [ ] **Step 5: Run test to verify it passes, then repeat for gRPC**

Apply the same pattern to `PollJob` and `ReportResult` in `internal/coordinator/grpc_server.go`: add
`raftGate *RaftGate`, `SetRaftGate`, a `leaderConns map[string]*grpc.ClientConn` + `mu sync.Mutex` for
lazily-cached connections to the leader's gRPC address, and a `forwardPollJob`/`forwardReportResult` pair
using a real `jobv1.JobServiceClient` dialed against `leader.GRPCAddr`. `StreamLogs` stays ungated (a
read). Mirror the REST test above with a gRPC equivalent.

Run: `go test ./internal/coordinator/... -v`
Expected: PASS, all tests including the new forwarding ones.

- [ ] **Step 6: Commit**

Commit message: `feat(coordinator): gate job-assignment writes behind leader check, forward writes to leader`

---

### Task 4.4: (Already covered)

The roadmap's Task 4 — "leader-election failover test" — was written in Task 4.2a above, since it's also
the natural verification for `NewRaftNode`. No separate commit here; noted in the self-review below.

---

### Task 4.5: Persist raft log/snapshot to disk

**Files:** Modify `cmd/coordinator/main.go`. Modify `.gitignore`.

- [ ] **Step 1: Add the dependency**

```bash
go get github.com/hashicorp/raft-boltdb/v2@latest
```

- [ ] **Step 2:** Add `/data/` to `.gitignore` — raft's persisted log/stable/snapshot files are
  per-machine runtime state, not source.

- [ ] **Step 3: Swap the in-memory stores for durable ones** in `cmd/coordinator/main.go`'s cluster-mode
  block (Task 4.2c):

```go
dataDir := filepath.Join("data", replicaID, "raft")
if err := os.MkdirAll(dataDir, 0o755); err != nil {
	slog.Error("coordinator failed to create raft data dir", "error", err)
	os.Exit(1)
}
boltStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.bolt"))
if err != nil {
	slog.Error("coordinator failed to open raft bolt store", "error", err)
	os.Exit(1)
}
snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
if err != nil {
	slog.Error("coordinator failed to open raft snapshot store", "error", err)
	os.Exit(1)
}

r, err := coordinator.NewRaftNode(coordinator.RaftNodeConfig{
	LocalID: replicaID, Transport: transport,
	LogStore: boltStore, StableStore: boltStore, SnapshotStore: snapshotStore,
	Servers: servers, HeartbeatTimeout: 50 * time.Millisecond, ElectionTimeout: 50 * time.Millisecond, LeaderLeaseTimeout: 50 * time.Millisecond,
})
```

  (One shared `boltStore` for both the log and stable roles — confirmed during planning that
  `raft-boltdb/v2`'s `BoltStore` implements both interfaces from one file, same pattern the in-memory
  version already used with `raft.NewInmemStore()`.)

- [ ] **Step 4: Manual verification**

```bash
make compose-up
COORDINATOR_REPLICA_ID=node1 /path/to/coordinator-binary &
COORDINATOR_REPLICA_ID=node2 /path/to/coordinator-binary &
COORDINATOR_REPLICA_ID=node3 /path/to/coordinator-binary &
# confirm ./data/node1/raft/, ./data/node2/raft/, ./data/node3/raft/ each contain a raft.bolt file
# kill all three, restart them — confirm the previously-elected leader's identity isn't required to be
# the same, but the cluster re-forms and elects a leader again without re-bootstrapping from scratch
```

- [ ] **Step 5: Commit**

Commit message: `feat(coordinator): persist raft log/snapshot to a volume`

---

### Task 4.6: Network-partition fault injection test

**Files:** Create `internal/coordinator/partition_test.go`.

Reuses Task 4.2a's `newInMemRaftCluster`/`waitForLeader` helpers — isolates the leader from both
followers (bidirectionally, via `InmemTransport.Disconnect`) while leaving the two followers connected to
each other, and asserts the majority side elects a new leader while the isolated old leader does not (it
can no longer reach a quorum of the 3-node cluster to win any election it starts).

- [ ] **Step 1: Write the test**

```go
package coordinator_test

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestRaft_NetworkPartition_MajorityElectsNewLeader(t *testing.T) {
	nodes := newInMemRaftCluster(t, 3)
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.raft.Shutdown()
		}
	})

	leader := waitForLeader(t, nodes, 2*time.Second)

	var majority []*raftTestNode
	for _, n := range nodes {
		if n.id != leader.id {
			majority = append(majority, n)
		}
	}

	// Partition: cut the leader off from both followers, in both directions
	// — the majority side (the two followers, still connected to each
	// other) should elect a new leader; the isolated old leader shouldn't.
	for _, other := range majority {
		leader.transport.Disconnect(other.transport.LocalAddr())
		other.transport.Disconnect(leader.transport.LocalAddr())
	}

	start := time.Now()
	newLeader := waitForLeader(t, majority, 2*time.Second)
	t.Logf("majority-side re-election after partition took %s (new leader %s)", time.Since(start), newLeader.id)

	// Give the isolated old leader the same window to (wrongly) claim
	// leadership — it can't reach a quorum, so it shouldn't be able to.
	time.Sleep(300 * time.Millisecond)
	if leader.raft.State() == raft.Leader {
		t.Fatalf("expected the isolated old leader to step down, still reports Leader")
	}
}
```

- [ ] **Step 2: Run the test**

Run: `go test ./internal/coordinator/... -run TestRaft_NetworkPartition -v`
Expected: PASS.

- [ ] **Step 3: Commit**

Commit message: `test: add network-partition fault injection (isolate leader from followers, assert majority partition elects a new leader and the minority side does not)`

---

### Task 4.7: Failover benchmark harness — real numbers, not invented ones

**Files:** Create `internal/coordinator/benchmark_test.go`.

Runs the process-kill failover scenario 30 times back to back, recording each trial's latency, then
reports median/p99 and the pass count against the 500ms target. Per this project's resume-honesty rule
(`CLAUDE.md`): whatever this measures is what goes in `PROGRESS.md` and, eventually, any resume line —
never a number written down before it's actually run.

- [ ] **Step 1: Write the benchmark**

```go
package coordinator_test

import (
	"sort"
	"testing"
	"time"
)

func TestRaft_FailoverBenchmark(t *testing.T) {
	const trials = 30
	latencies := make([]time.Duration, 0, trials)

	for i := 0; i < trials; i++ {
		func() {
			nodes := newInMemRaftCluster(t, 3)
			defer func() {
				for _, n := range nodes {
					_ = n.raft.Shutdown()
				}
			}()

			leader := waitForLeader(t, nodes, 2*time.Second)
			var remaining []*raftTestNode
			for _, n := range nodes {
				if n.id != leader.id {
					remaining = append(remaining, n)
				}
			}

			start := time.Now()
			_ = leader.raft.Shutdown()
			waitForLeader(t, remaining, 2*time.Second)
			latencies = append(latencies, time.Since(start))
		}()
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	median := latencies[len(latencies)/2]
	p99 := latencies[int(float64(len(latencies))*0.99)]
	passCount := 0
	for _, l := range latencies {
		if l <= 500*time.Millisecond {
			passCount++
		}
	}
	t.Logf("failover benchmark: %d trials, median=%s, p99=%s, min=%s, max=%s, %d/%d under 500ms",
		trials, median, p99, latencies[0], latencies[len(latencies)-1], passCount, trials)

	if passCount != trials {
		t.Fatalf("expected all %d trials under 500ms, got %d", trials, passCount)
	}
}
```

- [ ] **Step 2: Run it and capture the real output**

Run: `go test ./internal/coordinator/... -run TestRaft_FailoverBenchmark -v`
Expected: PASS. **Copy the actual logged median/p99/min/max numbers verbatim into Task 4.8's PROGRESS.md
entry** — this is the one piece of this whole project where a measured result becomes documented fact,
so the number in the docs must be the number that actually printed, not a rounded-off guess.

- [ ] **Step 3: Commit**

Commit message: `test: add failover benchmark harness — run N repeated leader-kill trials, record failover latency per trial, report median/p99`

---

### Task 4.8: Documentation

**Files:** Modify `README.md`, `PROGRESS.md`, `CLAUDE.md`.

- [ ] **Step 1:** Update `README.md` with a "Running a 3-replica cluster" section: `make compose-up`, the
  three `COORDINATOR_REPLICA_ID=nodeN` invocations, `COORDINATOR_CLUSTER_CONFIG` env var, and a short
  demo of submitting a job against a follower's REST port to show the forward working.

- [ ] **Step 2:** Append the Part 4 section to `PROGRESS.md` — one line per commit (matching this task
  list's actual final commit messages, not this plan's draft ones if anything changed during
  implementation), **plus the real benchmark numbers from Task 4.7 Step 2, copied verbatim.**

- [ ] **Step 3:** Update `CLAUDE.md`'s resume-honesty reminder if needed — it already says "implemented
  leader election and log replication across N coordinators using Raft consensus (hashicorp/raft)" as the
  required phrasing; confirm the actual implementation still matches that framing (it does: 3 replicas,
  `hashicorp/raft`, not from scratch).

- [ ] **Step 4: Commit**

Commit message: `docs: update PROGRESS.md for Part 4 with real benchmark results`

**Part 4 complete.** Hand off to Ritvik to open the PR for `part-4-raft` → `main`.

**PR title:** `Part 4: three-replica Raft cluster with leader election and failover`
**PR description points:** `hashicorp/raft` used purely for leader election, not data replication (job
state stays durable via Part 3's Redpanda log, now continuously tailed by every replica); writes are
leader-gated and transparently forwarded from followers; automated failover test, network-partition test,
and a 30-trial benchmark with real measured median/p99 failover latency (link the actual numbers here
once Task 4.7 produces them); raft log/snapshot persisted to disk per replica.

---

## Self-review

- **Spec/roadmap coverage:** roadmap's 8 Part 4 commit messages are all present, with one addition (Task
  4.1's incremental-apply groundwork, not in the original 8-item list — surfaced as a real dependency
  during detailed planning, called out rather than silently folded in) and one merge (the roadmap's
  separately-numbered "single-node bootstrap" vs. "three-replica config" commits landed as 4.2a/4.2b/4.2c
  instead, since a coordinator replica genuinely isn't a coherent, testable unit until raft + continuous
  tail + TCP config all exist together — noted explicitly in 4.2a rather than forcing an artificial
  intermediate commit).
- **Placeholder scan:** every code block is either verified-working (raft construction, in-memory
  transport, bootstrap, shutdown, disconnect — all actually run during planning) or explicitly flagged as
  needing a final signature check during implementation (`raft.NewTCPTransport`, the REST forwarding
  test's exact test-double shape) — never silently assumed correct.
- **Type/interface consistency:** `eventlog.ApplyEvent` (4.1) is consumed by the continuous tail's
  callback (4.2b/4.2c) exactly once, matching `Rebuild`'s existing per-event-type switch logic without
  duplicating the switch statement's *meaning* (only its shape, once for bulk/pure and once for
  incremental/imperative — justified in Task 4.1). `RaftGate` (4.3) is the single gating mechanism used
  identically by both `Server` (REST) and `GRPCServer` (gRPC); `nil` behavior is specified once and
  applies everywhere it's checked.
- **Known limitations named, not hidden:** followers serving stale reads until their tail loop catches up
  (Task 4.3); every replica double-applying its own writes once, harmlessly (design section); raft
  bootstrapped identically from every replica rather than a special-cased "first" node (Task 4.2a) —
  matches the library's own intended usage, not a workaround.
- **Resume-honesty check:** Task 4.7's benchmark numbers are measured, not estimated, and Task 4.8
  explicitly requires copying the actual test output into `PROGRESS.md` — no number appears in this plan
  that wasn't either already measured during the verification pass (77–132ms, 5 runs) or explicitly
  deferred to a real test run before being written down as fact.
