# Part 6 — REST API polish + thin MCP layer — Implementation Plan

**Branch:** `part-6-interfaces-observability` (this branch also carries Part 8 and, optionally, Part 9 —
see `docs/plans/roadmap.md`'s "Branch structure" section for why they're bundled. Per that same roadmap's
own rule, each Part inside the branch still gets its own detailed plan written just before it starts —
this doc covers **Part 6 only**. Part 8's plan gets written after Part 6 lands.)

**Goal:** Finish the REST surface that's been the primary external interface since Part 1 — add
`DELETE /jobs/{id}` (cancel) and streaming logs — then add a deliberately thin MCP server exposing the
same four actions (`submit_job`, `get_job_status`, `stream_logs`, `cancel_job`) as a wrapper over the same
core engine. No new distributed-systems concept here — Part 5 already closed out the fault-tolerant core;
this Part is "finish the surface of an already-working system" (see `PLAN.md`'s Branch 6 section).

**Spec:** `docs/spec.md`. **Roadmap:** `docs/plans/roadmap.md` (Part 6 section). **Prior plan:**
`docs/plans/part-5-scheduling.md`.

## Design

### Cancel only applies to queued jobs — stated as a scope decision, not a bug

The pull-based worker model (Part 1 onward) has no push channel from coordinator to worker — a worker
finds out about a job only by asking for one. Cancelling a job **already claimed and running** on some
worker would require either a new push RPC (coordinator tells a specific worker "stop") or the worker
polling for a cancellation flag mid-execution (an entirely new mechanism, not implied by "finish the REST
surface"). Building that would be a new distributed-systems concept in its own right — worth its own Part
if this project needed it, not "polish."

**Decision:** `DELETE /jobs/{id}` cancels a job only while it's still `queued` — removing it from the
pool before any worker ever claims it. A job that's already `running` or terminal returns `409 Conflict`
with a clear message, not a silent no-op. This is the same kind of explicit, named simplification as
Part 5's replay-ordering note — correct and honest within its stated scope, not a shortcut hidden from
the reader.

### Streaming logs: REST gets the same shape gRPC already has, not new capture

`GRPCServer.StreamLogs` (Part 2) already documents its own honest limitation: the executor captures
stdout/stderr as complete buffers *after* a job finishes (Part 1 behavior, unchanged), so "streaming" from
day one has meant "send whatever's captured as 1-2 chunks over a stream, then close" — not live tailing.
Building genuine incremental capture would mean changing `worker.RunJob`'s `os/exec` usage to stream
output as it's produced and relay it through the poll/report cycle in real time — a real feature, not
"finalize the REST surface," and out of scope here.

**Decision:** `GET /jobs/{id}/logs/stream` uses Server-Sent Events (`text/event-stream`) to send the same
two chunks (`event: stdout`, `event: stderr`) the gRPC version sends, then closes the connection. Real SSE
wire format, real `http.Flusher` calls — just not live tailing, exactly like its gRPC sibling. The
existing `GET /jobs/{id}/logs` (plain text, unchanged) stays for simple `curl` use; the new endpoint is
for clients that want the streaming shape (including the MCP `stream_logs` tool).

### MCP: stdio transport, thin tools that call the same `job.Store` — no new engine

The `github.com/modelcontextprotocol/go-sdk` (`v1.8.0`) is used directly, not proxied through REST over
HTTP — `internal/mcpserver`'s 4 tool handlers call the same `job.Store` interface the REST and gRPC
servers already call, exactly the way `Server` and `GRPCServer` are both thin transports over one shared
store (established pattern since Part 1). A new `cmd/mcpserver/main.go` binary constructs the same
`eventlog.Store`-wrapped `job.MemoryStore` startup sequence as `cmd/coordinator/main.go` (event-log
replay included, so MCP sees real state, not an empty store) and serves it over
`mcp.StdioTransport` — the standard way local MCP clients (Claude Desktop, the MCP Inspector CLI) launch
a server, and it needs no new network listener, port, or auth scheme to keep this "thin."

**Tools, each a direct, obvious mapping:**
- `submit_job(image, command, timeout_seconds)` → `store.Create`
- `get_job_status(id)` → `store.Get`
- `stream_logs(id)` → `store.Get`, returned as one text blob (stdout + stderr) — the MCP result type is
  already a single `CallToolResult`, not a wire stream, so "streaming" here just means "the same
  logs-fetching action as the REST/gRPC stream tools," not a literal chunked MCP response.
- `cancel_job(id)` → `store.Cancel` (same queued-only semantics as REST's `DELETE`)

No raft leader-forwarding logic in the MCP layer: `cmd/mcpserver` runs as a single local process against
whichever coordinator's `eventlog.Store` it's built with (single-instance mode only, matching the "thin
wrapper, no more effort than REST" framing) — cluster-mode MCP is out of scope, same posture as accepting
that MCP itself is a secondary interface (`PLAN.md`'s "Scope calls" section).

### Everything else about the existing architecture is untouched

`job.Store`'s existing methods (`Create`, `Get`, `ClaimNext`, `Complete`, `RequeueRunning`) are unchanged;
`Cancel` is additive. `eventlog.Store` gains one more wrapped method and one more event type, the same
pattern as every prior Part's additions (Part 3's `Create`/`ClaimNext`/`Complete`, Part 5's
`RequeueRunning`). Raft/`RaftGate` forwarding is unchanged — `DELETE /jobs/{id}` is a write, so it's
leader-gated and forwarded exactly like `POST /jobs`.

## File structure (new/changed)

```
internal/
  job/
    job.go                        # MODIFIED — Status gains StatusCancelled
    store.go                       # MODIFIED — Store gains Cancel(id); MemoryStore.Cancel, ApplyCancelled
    store_test.go                   # MODIFIED
  eventlog/
    event.go                         # MODIFIED — Event gains EventJobCancelled; Rebuild/ApplyEvent cases
    event_test.go                     # MODIFIED
    store.go                           # MODIFIED — Store.Cancel wraps + publishes
    store_test.go                       # MODIFIED
  coordinator/
    server.go                            # MODIFIED — DELETE /jobs/{id}, GET /jobs/{id}/logs/stream
    server_test.go                        # MODIFIED
  mcpserver/
    server.go                              # NEW — builds *mcp.Server, registers the 4 tools
    server_test.go                          # NEW
cmd/
  mcpserver/
    main.go                                  # NEW — replay event log, build store, run over stdio
go.mod                                          # MODIFIED — add github.com/modelcontextprotocol/go-sdk
docs/plans/part-6-rest-mcp.md                     # this file
```

---

### Task 6.1: `job.Status` gains `StatusCancelled`; `Store.Cancel`

**Files:** `internal/job/job.go`, `internal/job/store.go`, `internal/job/store_test.go`.

**Landed together with Task 6.2, not separately:** same interface-forced coupling as Part 5's Task 5.1 —
the moment `Cancel` was added to the `job.Store` interface, `go build` failed everywhere `eventlog.Store`
and the coordinator test doubles implement that interface, since Go requires every implementer to satisfy
it before anything compiles. Task 6.2's event-log support (and a `Cancel` method on
`internal/coordinator/failing_store_test.go`'s `failingStore`) had to land in the same commit rather than
being deferred to a later step.

- [x] **Step 1: Add the status constant** — `internal/job/job.go`:

```go
const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)
```

- [x] **Step 2: Write the failing tests** — append to `internal/job/store_test.go`:

```go
func TestMemoryStore_Cancel_QueuedJobBecomesCancelled(t *testing.T) {
	s := job.NewMemoryStore()
	created, _ := s.Create("alpine", []string{"true"}, 10)

	got, err := s.Cancel(created.ID)
	if err != nil {
		t.Fatalf("Cancel returned error: %v", err)
	}
	if got.Status != job.StatusCancelled {
		t.Fatalf("expected cancelled, got %+v", got)
	}

	fromGet, err := s.Get(created.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if fromGet.Status != job.StatusCancelled {
		t.Fatalf("expected Get to reflect cancellation, got %+v", fromGet)
	}
}

func TestMemoryStore_Cancel_RunningJobReturnsErrNotCancellable(t *testing.T) {
	s := job.NewMemoryStore()
	created, _ := s.Create("alpine", []string{"true"}, 10)
	if _, err := s.ClaimNext("worker-1"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}

	_, err := s.Cancel(created.ID)
	if !errors.Is(err, job.ErrNotCancellable) {
		t.Fatalf("expected ErrNotCancellable, got %v", err)
	}
}

func TestMemoryStore_Cancel_UnknownJobReturnsErrNotFound(t *testing.T) {
	s := job.NewMemoryStore()
	_, err := s.Cancel("does-not-exist")
	if !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemoryStore_Cancel_RemovesFromClaimOrder(t *testing.T) {
	// A cancelled job must never be handed out by a later ClaimNext.
	s := job.NewMemoryStore()
	created, _ := s.Create("alpine", []string{"true"}, 10)
	if _, err := s.Cancel(created.ID); err != nil {
		t.Fatalf("Cancel returned error: %v", err)
	}

	claimed, err := s.ClaimNext("worker-1")
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected no claimable job after cancelling the only one, got %+v", claimed)
	}
}
```

  (Needs `"errors"` added to the test file's imports if not already present.)

- [x] **Step 3: Run to verify failure** — `Cancel` and `ErrNotCancellable` don't exist yet, so this
  should fail to compile.

- [x] **Step 4: Implement** in `internal/job/store.go`:

```go
var ErrNotCancellable = errors.New("job is not cancellable (already running or terminal)")

// Cancel adds Cancel(id) to the Store interface.
type Store interface {
	Create(image string, command []string, timeoutSeconds int) (*Job, error)
	Get(id string) (*Job, error)
	ClaimNext(workerID string) (*Job, error)
	Complete(id string, status Status, stdout, stderr string, exitCode int) error
	RequeueRunning(workerID string) ([]*Job, error)
	Cancel(id string) (*Job, error)
}
```

```go
// Cancel only succeeds while a job is still Queued — a job already claimed
// by a worker can't be stopped mid-execution under this project's pull-based
// model (see docs/plans/part-6-rest-mcp.md for why that's a stated scope
// decision, not a missing feature). Removing it from s.order here is what
// stops a later ClaimNext from ever handing it to a worker — the job stays
// in s.jobs (so Get still finds it, now Cancelled), just no longer queued.
func (s *MemoryStore) Cancel(id string) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	if j.Status != StatusQueued {
		return nil, ErrNotCancellable
	}
	j.Status = StatusCancelled
	j.UpdatedAt = time.Now()

	for i, oid := range s.order {
		if oid == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}

	cp := *j
	return &cp, nil
}
```

  Also add `ApplyCancelled(id string, at time.Time)` (same replication role as `ApplyRequeued` — used by
  Part 4/5's continuous tail so every replica applies a leader's cancellation, not just the leader's own
  store):

```go
func (s *MemoryStore) ApplyCancelled(id string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok || j.Status != StatusQueued {
		return
	}
	j.Status = StatusCancelled
	j.UpdatedAt = at
	for i, oid := range s.order {
		if oid == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
}
```

- [x] **Step 5: Run tests, verify they pass**

Run: `go test ./internal/job/... -run TestMemoryStore_Cancel -v`

- [x] **Step 6: Update every other `job.Store` implementer/consumer that switches on the interface** —
  grep for `job.Store` usages that would need a compile-time update (there should be none beyond
  `eventlog.Store`, handled in Task 6.2 — the failing-store test double in
  `internal/coordinator/failing_store_test.go` will also need a `Cancel` method added to keep compiling,
  same as it needed `RequeueRunning` in Part 5).

- [x] **Step 7: Run full suite, verify, commit**

Run: `go build ./... && go test ./...`

Commit message: `feat(job,eventlog): add job cancellation for queued jobs` (single commit covering both Task 6.1 and 6.2 — see the note above)

---

### Task 6.2: Event log support — `EventJobCancelled`, `eventlog.Store.Cancel`

**Files:** `internal/eventlog/event.go`, `internal/eventlog/event_test.go`, `internal/eventlog/store.go`,
`internal/eventlog/store_test.go`.

- [x] **Step 1: Add the event type** — `internal/eventlog/event.go`:

```go
const EventJobCancelled EventType = "job_cancelled"
```

- [x] **Step 2: Write failing tests** for `Rebuild`/`ApplyEvent` — append to
  `internal/eventlog/event_test.go`:

```go
func TestRebuild_CancelledJobStaysCancelled(t *testing.T) {
	now := time.Now()
	jobs := eventlog.Rebuild([]eventlog.Event{
		{Type: eventlog.EventJobCreated, JobID: "a", Timestamp: now},
		{Type: eventlog.EventJobCancelled, JobID: "a", Timestamp: now},
	})
	if len(jobs) != 1 || jobs[0].Status != job.StatusCancelled {
		t.Fatalf("expected cancelled job, got %+v", jobs)
	}
}

func TestApplyEvent_Cancelled(t *testing.T) {
	store := job.NewMemoryStore()
	now := time.Now()

	eventlog.ApplyEvent(store, eventlog.Event{Type: eventlog.EventJobCreated, JobID: "a", Timestamp: now})
	eventlog.ApplyEvent(store, eventlog.Event{Type: eventlog.EventJobCancelled, JobID: "a", Timestamp: now})

	got, err := store.Get("a")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusCancelled {
		t.Fatalf("expected cancelled, got %+v", got)
	}
}
```

- [x] **Step 3: Run to verify failure, then implement** — in `Rebuild`'s fold switch, add:

```go
case EventJobCancelled:
	if j, ok := jobs[e.JobID]; ok {
		j.Status = job.StatusCancelled
	}
```

  and in `ApplyEvent`'s switch:

```go
case EventJobCancelled:
	store.ApplyCancelled(e.JobID, e.Timestamp)
```

- [x] **Step 4: Wrap in `eventlog.Store`** — `internal/eventlog/store.go`:

```go
func (s *Store) Cancel(id string) (*job.Job, error) {
	j, err := s.inner.Cancel(id)
	if err != nil {
		return nil, err
	}
	if pubErr := s.producer.Publish(context.Background(), Event{
		Type: EventJobCancelled, JobID: id, Timestamp: time.Now(),
	}); pubErr != nil {
		return j, fmt.Errorf("job cancelled locally but failed to publish: %w", pubErr)
	}
	return j, nil
}
```

  (Same known limitation as every other `eventlog.Store` write method: apply-then-publish, no rollback of
  local state if the publish fails partway through — stated once in Part 3/5, applies identically here.)

- [x] **Step 5: Write a failing-then-passing test** for the wrapper, append to
  `internal/eventlog/store_test.go`:

```go
func TestEventlogStore_Cancel_PublishesJobCancelled(t *testing.T) {
	fake := &fakeProducer{}
	base := job.NewMemoryStore()
	s := eventlog.NewStore(base, fake)
	created, _ := s.Create("alpine", []string{"true"}, 10)

	if _, err := s.Cancel(created.ID); err != nil {
		t.Fatalf("Cancel returned error: %v", err)
	}
	if len(fake.events) != 2 { // created + cancelled
		t.Fatalf("expected 2 published events, got %d: %+v", len(fake.events), fake.events)
	}
	if fake.events[1].Type != eventlog.EventJobCancelled {
		t.Fatalf("expected second event to be job_cancelled, got %v", fake.events[1].Type)
	}
}
```

- [x] **Step 6: Run full suite, verify, commit**

Run: `go build ./... && go test ./...`

Commit message: (folded into Task 6.1's commit above — `feat(job,eventlog): add job cancellation for queued jobs`)

---

### Task 6.3: REST `DELETE /jobs/{id}` and `GET /jobs/{id}/logs/stream`

**Files:** `internal/coordinator/server.go`, `internal/coordinator/server_test.go`.

- [x] **Step 1: Write failing tests** — append to `internal/coordinator/server_test.go`:

```go
func TestDELETE_Jobs_CancelsQueuedJob(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	srv := coordinator.NewServer(store)

	req := httptest.NewRequest(http.MethodDelete, "/jobs/"+created.ID, nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	got, err := store.Get(created.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusCancelled {
		t.Fatalf("expected cancelled, got %+v", got)
	}
}

func TestDELETE_Jobs_RunningJobReturns409(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	if _, err := store.ClaimNext("worker-1"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	srv := coordinator.NewServer(store)

	req := httptest.NewRequest(http.MethodDelete, "/jobs/"+created.ID, nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestDELETE_Jobs_UnknownIDReturns404(t *testing.T) {
	srv := coordinator.NewServer(job.NewMemoryStore())
	req := httptest.NewRequest(http.MethodDelete, "/jobs/does-not-exist", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestGET_JobsLogsStream_SendsSSEChunks(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"echo", "hi"}, 10)
	if _, err := store.ClaimNext("worker-1"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if err := store.Complete(created.ID, job.StatusSucceeded, "out\n", "err\n", 0); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	srv := coordinator.NewServer(store)

	req := httptest.NewRequest(http.MethodGet, "/jobs/"+created.ID+"/logs/stream", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("expected text/event-stream, got %q", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: stdout") || !strings.Contains(body, "out\n") {
		t.Fatalf("expected an stdout SSE event, got %q", body)
	}
	if !strings.Contains(body, "event: stderr") || !strings.Contains(body, "err\n") {
		t.Fatalf("expected an stderr SSE event, got %q", body)
	}
}
```

- [x] **Step 2: Run to verify failure, then implement** — `internal/coordinator/server.go`:

```go
s.mux.HandleFunc("DELETE /jobs/{id}", s.handleCancelJob)
s.mux.HandleFunc("GET /jobs/{id}/logs/stream", s.handleStreamJobLogs)
```

```go
func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	if !s.raftGate.IsLeader() {
		s.forwardToLeader(w, r)
		return
	}
	id := r.PathValue("id")
	j, err := s.store.Cancel(id)
	if err != nil {
		switch {
		case errors.Is(err, job.ErrNotFound):
			http.Error(w, "job not found", http.StatusNotFound)
		case errors.Is(err, job.ErrNotCancellable):
			http.Error(w, "job is not cancellable (already running or terminal)", http.StatusConflict)
		default:
			http.Error(w, "failed to cancel job", http.StatusInternalServerError)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(toJobResponse(j)); err != nil {
		slog.Error("failed to encode response", "error", err)
	}
}

// handleStreamJobLogs sends the same buffered stdout/stderr as
// GRPCServer.StreamLogs, framed as SSE — same "not live tailing yet" honesty
// as its gRPC sibling (see this Part's plan doc). Not leader-gated, same
// reasoning as handleGetJob: a read from this replica's own local copy.
func (s *Server) handleStreamJobLogs(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, err := s.store.Get(id)
	if err != nil {
		if errors.Is(err, job.ErrNotFound) {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get job", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	flusher, _ := w.(http.Flusher)
	if j.Stdout != "" {
		_, _ = fmt.Fprintf(w, "event: stdout\ndata: %s\n\n", strings.ReplaceAll(j.Stdout, "\n", "\ndata: "))
		if flusher != nil {
			flusher.Flush()
		}
	}
	if j.Stderr != "" {
		_, _ = fmt.Fprintf(w, "event: stderr\ndata: %s\n\n", strings.ReplaceAll(j.Stderr, "\n", "\ndata: "))
		if flusher != nil {
			flusher.Flush()
		}
	}
}
```

  (`httptest.NewRecorder()` implements `http.Flusher`, so the test above works without a real network
  connection.)

- [x] **Step 3: Run full suite, verify, commit**

Run: `go build ./... && go test ./...`

Commit message: `feat(api): finalize REST surface including log streaming and cancel`

---

### Task 6.4: Thin MCP server — `internal/mcpserver`, `cmd/mcpserver`

**Files:** Add `github.com/modelcontextprotocol/go-sdk` (`v1.8.0`) to `go.mod`. Create
`internal/mcpserver/server.go`, `internal/mcpserver/server_test.go`, `cmd/mcpserver/main.go`.

- [ ] **Step 1:** `go get github.com/modelcontextprotocol/go-sdk@v1.8.0`

- [ ] **Step 2: Write the failing tests** — `internal/mcpserver/server_test.go`. The SDK's in-process
  `mcp.NewInMemoryTransports()` (client/server pair with no real subprocess) is the test seam — no stdio,
  no process spawn needed to test tool behavior:

```go
package mcpserver_test

import (
	"context"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ritvikreddygangula/forge/internal/job"
	"github.com/ritvikreddygangula/forge/internal/mcpserver"
)

func connectedClient(t *testing.T, store job.Store) *mcp.ClientSession {
	t.Helper()
	server := mcpserver.New(store)
	clientTransport, serverTransport := mcp.NewInMemoryTransports()

	go func() {
		_ = server.Run(context.Background(), serverTransport)
	}()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client"}, nil)
	session, err := client.Connect(context.Background(), clientTransport, nil)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func TestMCP_SubmitJob_CreatesQueuedJob(t *testing.T) {
	store := job.NewMemoryStore()
	session := connectedClient(t, store)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "submit_job",
		Arguments: map[string]any{"image": "alpine", "command": []string{"true"}, "timeout_seconds": 10},
	})
	if err != nil {
		t.Fatalf("CallTool returned error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success, got error result: %+v", res)
	}
	// The tool's structured output should carry the created job's id — exact
	// assertion shape finalized during implementation once the Out type for
	// submit_job is fixed (see Step 3).
}

func TestMCP_GetJobStatus_ReturnsCurrentStatus(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	session := connectedClient(t, store)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "get_job_status",
		Arguments: map[string]any{"id": created.ID},
	})
	if err != nil {
		t.Fatalf("CallTool returned error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success, got error result: %+v", res)
	}
}

func TestMCP_CancelJob_CancelsQueuedJob(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	session := connectedClient(t, store)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "cancel_job",
		Arguments: map[string]any{"id": created.ID},
	})
	if err != nil {
		t.Fatalf("CallTool returned error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success, got error result: %+v", res)
	}
	got, err := store.Get(created.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusCancelled {
		t.Fatalf("expected cancelled, got %+v", got)
	}
}

func TestMCP_CancelJob_RunningJobReturnsErrorResult(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	if _, err := store.ClaimNext("worker-1"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	session := connectedClient(t, store)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "cancel_job",
		Arguments: map[string]any{"id": created.ID},
	})
	if err != nil {
		t.Fatalf("CallTool returned error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected an error result for cancelling a running job")
	}
}

func TestMCP_StreamLogs_ReturnsStdoutAndStderr(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	if _, err := store.ClaimNext("worker-1"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if err := store.Complete(created.ID, job.StatusSucceeded, "out\n", "err\n", 0); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	session := connectedClient(t, store)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "stream_logs",
		Arguments: map[string]any{"id": created.ID},
	})
	if err != nil {
		t.Fatalf("CallTool returned error: %v", err)
	}
	text, ok := res.Content[0].(*mcp.TextContent)
	if !ok || !strings.Contains(text.Text, "out\n") || !strings.Contains(text.Text, "err\n") {
		t.Fatalf("expected stdout+stderr in text content, got %+v", res.Content)
	}
}
```

- [ ] **Step 3: Run to verify failure, then implement** — `internal/mcpserver/server.go`:

```go
package mcpserver

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ritvikreddygangula/forge/internal/job"
)

// New builds an MCP server exposing the same 4 actions as the REST API,
// as a thin wrapper directly over job.Store — no new engine, no leader
// forwarding (single-instance use only; see this Part's plan doc).
func New(store job.Store) *mcp.Server {
	s := mcp.NewServer(&mcp.Implementation{Name: "forge", Version: "v1.0.0"}, nil)

	mcp.AddTool(s, &mcp.Tool{Name: "submit_job", Description: "Submit a job for execution"}, submitJob(store))
	mcp.AddTool(s, &mcp.Tool{Name: "get_job_status", Description: "Get a job's current status"}, getJobStatus(store))
	mcp.AddTool(s, &mcp.Tool{Name: "stream_logs", Description: "Get a job's captured stdout/stderr"}, streamLogs(store))
	mcp.AddTool(s, &mcp.Tool{Name: "cancel_job", Description: "Cancel a job while it's still queued"}, cancelJob(store))

	return s
}

type submitJobArgs struct {
	Image          string   `json:"image" jsonschema:"the container image to run"`
	Command        []string `json:"command" jsonschema:"the command to run inside the container"`
	TimeoutSeconds int      `json:"timeout_seconds,omitempty" jsonschema:"execution timeout in seconds, default 300"`
}

type jobOut struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func submitJob(store job.Store) mcp.ToolHandlerFor[submitJobArgs, jobOut] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, args submitJobArgs) (*mcp.CallToolResult, jobOut, error) {
		if args.TimeoutSeconds <= 0 {
			args.TimeoutSeconds = 300
		}
		j, err := store.Create(args.Image, args.Command, args.TimeoutSeconds)
		if err != nil {
			return nil, jobOut{}, err
		}
		return nil, jobOut{ID: j.ID, Status: string(j.Status)}, nil
	}
}

type jobIDArgs struct {
	ID string `json:"id" jsonschema:"the job id"`
}

func getJobStatus(store job.Store) mcp.ToolHandlerFor[jobIDArgs, jobOut] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, args jobIDArgs) (*mcp.CallToolResult, jobOut, error) {
		j, err := store.Get(args.ID)
		if err != nil {
			return nil, jobOut{}, err
		}
		return nil, jobOut{ID: j.ID, Status: string(j.Status)}, nil
	}
}

func streamLogs(store job.Store) mcp.ToolHandlerFor[jobIDArgs, any] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, args jobIDArgs) (*mcp.CallToolResult, any, error) {
		j, err := store.Get(args.ID)
		if err != nil {
			return nil, nil, err
		}
		text := fmt.Sprintf("--- stdout ---\n%s\n--- stderr ---\n%s\n", j.Stdout, j.Stderr)
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}, nil, nil
	}
}

func cancelJob(store job.Store) mcp.ToolHandlerFor[jobIDArgs, any] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, args jobIDArgs) (*mcp.CallToolResult, any, error) {
		// ToolHandlerFor auto-wraps a returned error into CallToolResult with
		// IsError set (confirmed against the SDK's actual doc comment) — no
		// need to hand-construct an error CallToolResult ourselves.
		if _, err := store.Cancel(args.ID); err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "cancelled"}}}, nil, nil
	}
}
```

  (Exact field/shape names — `jobOut`, whether `submit_job`'s handler returns an error vs. an
  `IsError` result for a bad image — finalized during implementation against the SDK's actual generic
  constraints; the design (thin, direct `job.Store` calls, no new logic) is fixed, this is wiring.)

- [ ] **Step 4: `cmd/mcpserver/main.go`** — mirrors `cmd/coordinator/main.go`'s startup sequence (replay
  the event log, build the same `eventlog.Store`-wrapped `job.MemoryStore`) but serves MCP over stdio
  instead of REST/gRPC:

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ritvikreddygangula/forge/internal/eventlog"
	"github.com/ritvikreddygangula/forge/internal/job"
	"github.com/ritvikreddygangula/forge/internal/mcpserver"
)

func main() {
	brokersEnv := os.Getenv("REDPANDA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "localhost:9092"
	}
	brokers := strings.Split(brokersEnv, ",")

	ctx := context.Background()
	if err := eventlog.EnsureTopic(ctx, brokers, eventlog.DefaultTopic); err != nil {
		slog.Error("mcpserver failed to reach Redpanda", "error", err)
		os.Exit(1)
	}
	events, _, err := eventlog.NewKafkaConsumer(brokers, eventlog.DefaultTopic).ReadAll(ctx)
	if err != nil {
		slog.Error("mcpserver failed to replay event log", "error", err)
		os.Exit(1)
	}
	baseStore := job.NewMemoryStore()
	baseStore.Rebuild(eventlog.Rebuild(events))

	producer := eventlog.NewKafkaProducer(brokers, eventlog.DefaultTopic)
	store := eventlog.NewStore(baseStore, producer)

	server := mcpserver.New(store)
	if err := server.Run(ctx, &mcp.StdioTransport{}); err != nil {
		slog.Error("mcpserver exited", "error", err)
		os.Exit(1)
	}
}
```

  Add a `run-mcpserver` Makefile target (`go run ./cmd/mcpserver`) alongside `run-coordinator`/`run-worker`.

- [ ] **Step 5: Run full suite, verify, commit**

Run: `go build ./... && go test ./...`

Commit message: `feat(mcp): add thin MCP server exposing submit_job, get_job_status, stream_logs, cancel_job`

---

### Task 6.5: Integration test covering submit through cancel over both REST and MCP

**Files:** `internal/coordinator/integration_test.go` (extend) or a new
`internal/mcpserver/integration_test.go`.

- [ ] **Step 1:** One end-to-end test per surface: submit → get status → (for REST) stream logs → cancel
  a second queued job, asserting each step's real response, against a real `job.Store` (in-memory, no
  Docker needed — this is about the surface/wiring, not execution, same reasoning as
  `TestEndToEnd_SubmitPollExecuteReport` from Part 2).

- [ ] **Step 2: Run, verify, commit**

Run: `go test ./... -v`

Commit message: `test: add REST and MCP integration tests covering submit through cancel`

---

### Task 6.6: Documentation

**Files:** `README.md`, `PROGRESS.md`.

- [ ] **Step 1:** README: document `DELETE /jobs/{id}`, `GET /jobs/{id}/logs/stream`, and a "Running the
  MCP server" section (`make run-mcpserver`, plus how to point the MCP Inspector CLI or Claude Desktop's
  config at the built binary via stdio).

- [ ] **Step 2:** `PROGRESS.md`: Part 6 commit log.

- [ ] **Step 3: Commit**

Commit message: `docs: write full README local run instructions (docker compose up)`

*(Note: this is Part 6's doc commit only — Branch 6's final combined-branch doc commit happens after
Part 8, and Part 9 if attempted, per the roadmap's per-branch PR structure.)*

---

## Self-review

- **Spec/roadmap coverage:** all 5 roadmap commit messages for Part 6 present (`feat(api)`, `feat(mcp)`,
  `test`, `docs` README, `docs` PROGRESS — the roadmap's PROGRESS.md line is folded into Task 6.6 Step 2
  rather than a separate commit, since Part 6 alone doesn't warrant two doc commits the way the full
  Branch 6 will once Part 8 lands).
- **Placeholder scan:** two spots flagged for implementation-time finalization — `submit_job`'s exact
  output type/error-vs-IsError shape (Task 6.4 Step 3) and the integration test's exact scenario list
  (Task 6.5 Step 1) — both minor wiring choices, not open design questions, same posture as Part 5's two
  flagged spots.
- **Type/interface consistency:** `job.Store.Cancel` is defined once (Task 6.1) and consumed identically
  by `eventlog.Store` (Task 6.2), REST (Task 6.3), and MCP (Task 6.4) — no duplicate cancellation logic.
- **Known limitations named, not hidden:** cancel-only-while-queued (not mid-execution) and
  streaming-logs-is-buffered-not-live (both inherited from and consistent with existing Part 2/5
  precedent) are stated in the Design section above, not discovered later.
- **No new distributed-systems concept introduced** — consistent with Branch 6's framing as polish over
  an already-fault-tolerant core (Part 5 remains the resume-done checkpoint).
