# Part 2 — Swap HTTP for gRPC — Implementation Plan

**Branch:** `part-2-grpc`, cut from latest `main`.

**Goal:** Replace the worker-facing HTTP polling (`GET /internal/worker/poll`, `POST /internal/worker/result`)
with gRPC. Behavior is identical to Part 1 — same job lifecycle, same executor, same worker loop shape —
only the transport between coordinator and worker changes. The external REST surface
(`POST /jobs`, `GET /jobs/{id}`, `GET /jobs/{id}/logs`) is untouched; it keeps serving clients exactly as
it does today.

**Spec:** `docs/spec.md`. **Roadmap:** `docs/plans/roadmap.md` (Part 2 section). **Prior plan:**
`docs/plans/part-0-1-http-skeleton.md` (Part 1 code this plan modifies).

## Why this Part, and what "done" looks like

Part 1 proved the job lifecycle end-to-end. Part 2 doesn't touch that lifecycle at all — it swaps out
one pipe (HTTP poll/report) for another (gRPC), so gRPC can be learned against a system that's already
known to work, instead of debugging gRPC and the job lifecycle at the same time. Done means: the exact
same `curl` walkthrough from the Part 1 README still works unchanged (REST is untouched), and the
worker now talks to the coordinator over gRPC instead of HTTP — provably, by deleting the old HTTP
worker endpoints once the gRPC path is proven.

## ⚠️ Manual step — required before Task 2.1's codegen commit

gRPC code generation needs the `buf` CLI and two Go protoc plugins, none of which are installed on this
machine yet (checked: `buf` not found, `protoc-gen-go`/`protoc-gen-go-grpc` not found). Install these
yourself, once, before we get to the codegen step:

```bash
brew install bufbuild/buf/buf
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

Make sure `$(go env GOPATH)/bin` is on your `PATH` (usually `~/go/bin`) so `buf` can find the two
plugins. Verify with:

```bash
buf --version
protoc-gen-go --version
protoc-gen-go-grpc --version
```

Nothing else in this Part needs anything beyond what Part 1 already required (Colima/Docker running).

## File structure (new/changed)

```
api/
  proto/
    jobv1/
      job.proto           # NEW — the service definition
    gen/
      jobv1/
        job.pb.go          # generated — message types
        job_grpc.pb.go      # generated — client/server interfaces
buf.yaml                   # NEW — buf module config
buf.gen.yaml                # NEW — codegen config (local plugins, no network needed)
Makefile                    # MODIFIED — add `make proto` target
internal/
  coordinator/
    grpc_server.go          # NEW — implements jobv1.JobServiceServer
    grpc_server_test.go       # NEW
    worker_handlers.go        # DELETED (Task 2.7)
    worker_handlers_test.go    # DELETED (Task 2.7)
    server.go                  # MODIFIED — remove the two HTTP worker routes (Task 2.7)
  worker/
    loop.go                    # MODIFIED — gRPC client instead of net/http
    loop_test.go                 # MODIFIED — fake gRPC server instead of httptest
cmd/
  coordinator/main.go           # MODIFIED — also listen on a gRPC port
  worker/main.go                 # MODIFIED — dial gRPC instead of passing a URL
```

---

### Task 2.1: Define the proto service

**Files:** Create `api/proto/jobv1/job.proto`, `buf.yaml`, `buf.gen.yaml`.

- [ ] **Step 1:** `api/proto/jobv1/job.proto`

```proto
syntax = "proto3";

package forge.job.v1;

option go_package = "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1;jobv1";

// JobService is the worker-facing transport: poll for work, report results,
// fetch logs. It intentionally mirrors the internal HTTP endpoints it replaces
// (GET /internal/worker/poll, POST /internal/worker/result) plus adds
// StreamLogs for parity with the external GET /jobs/{id}/logs endpoint.
service JobService {
  rpc PollJob(PollJobRequest) returns (PollJobResponse);
  rpc ReportResult(ReportResultRequest) returns (ReportResultResponse);
  rpc StreamLogs(StreamLogsRequest) returns (stream LogChunk);
}

message PollJobRequest {}

message PollJobResponse {
  // Unset (nil) when the queue is empty — callers must check has_job.
  bool has_job = 1;
  Job job = 2;
}

message Job {
  string id = 1;
  string image = 2;
  repeated string command = 3;
  int32 timeout_seconds = 4;
}

message ReportResultRequest {
  string id = 1;
  string status = 2; // "succeeded" or "failed"
  string stdout = 3;
  string stderr = 4;
  int32 exit_code = 5;
}

message ReportResultResponse {}

message StreamLogsRequest {
  string id = 1;
}

message LogChunk {
  string stream = 1; // "stdout" or "stderr"
  string data = 2;
}
```

  (`has_job` exists because proto3 doesn't let you distinguish "unset message" from "empty message"
  without wrapper types or `optional` — an explicit bool is simpler than reaching for either.)

- [ ] **Step 2:** `buf.yaml`

```yaml
version: v2
modules:
  - path: api/proto
lint:
  use:
    - STANDARD
breaking:
  use:
    - FILE
```

- [ ] **Step 3:** `buf.gen.yaml` — local plugins, so generation never needs network access:

```yaml
version: v2
plugins:
  - local: protoc-gen-go
    out: api/proto/gen
    opt: paths=source_relative
  - local: protoc-gen-go-grpc
    out: api/proto/gen
    opt: paths=source_relative
```

- [ ] **Step 4: Commit**

Commit message: `feat(api): define job.proto service (PollJob, ReportResult, StreamLogs)`

---

### Task 2.2: Wire buf codegen into the Makefile, generate and commit the code

**Files:** Modify `Makefile`. Create (generated) `api/proto/gen/jobv1/job.pb.go`, `job_grpc.pb.go`.

- [ ] **Step 1:** Add to `Makefile`:

```makefile
proto:
	buf generate
```

- [ ] **Step 2:** Run `make proto`. This requires the manual step above to be done first.

- [ ] **Step 3:** Run `go mod tidy` — pulls in `google.golang.org/grpc` and `google.golang.org/protobuf`
  as real dependencies now that generated code imports them.

- [ ] **Step 4: Verify**

Run: `go build ./...`
Expected: exits 0 — the generated package compiles standalone (nothing imports it yet).

**Decision:** generated code is committed to the repo (not regenerated in CI). This is standard Go
practice and means CI never needs `buf`/protoc installed — `make proto` is a local, occasional command,
run only when `job.proto` changes.

- [ ] **Step 5: Commit**

Commit message: `build: wire buf codegen into Makefile`

---

### Task 2.3: gRPC server alongside REST

**Files:** Create `internal/coordinator/grpc_server.go`, `internal/coordinator/grpc_server_test.go`.

**Interfaces:**
- Consumes: `job.Store` (already used by `Server`), generated `jobv1.JobServiceServer`,
  `jobv1.UnimplementedJobServiceServer`.
- Produces: `coordinator.NewGRPCServer(store job.Store) *coordinator.GRPCServer`, implementing
  `jobv1.JobServiceServer` — a `PollJob`/`ReportResult`/`StreamLogs` trio that calls the exact same
  `job.Store` methods (`ClaimNext`, `Complete`, `Get`) the HTTP handlers call today.

- [ ] **Step 1: Write the failing tests** (using `google.golang.org/grpc/test/bufconn` — an in-memory
  listener, so these are ordinary fast unit tests, no real network/port involved)

`internal/coordinator/grpc_server_test.go`:
```go
package coordinator_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
	"github.com/ritvikreddygangula/forge/internal/coordinator"
	"github.com/ritvikreddygangula/forge/internal/job"
)

func dialGRPCServer(t *testing.T, store job.Store) jobv1.JobServiceClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	jobv1.RegisterJobServiceServer(srv, coordinator.NewGRPCServer(store))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return jobv1.NewJobServiceClient(conn)
}

func TestGRPC_PollJob_EmptyQueue(t *testing.T) {
	client := dialGRPCServer(t, job.NewMemoryStore())

	resp, err := client.PollJob(context.Background(), &jobv1.PollJobRequest{})
	if err != nil {
		t.Fatalf("PollJob returned error: %v", err)
	}
	if resp.HasJob {
		t.Fatalf("expected has_job=false on empty queue, got true")
	}
}

func TestGRPC_PollJob_ClaimsQueuedJob(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	client := dialGRPCServer(t, store)

	resp, err := client.PollJob(context.Background(), &jobv1.PollJobRequest{})
	if err != nil {
		t.Fatalf("PollJob returned error: %v", err)
	}
	if !resp.HasJob {
		t.Fatal("expected has_job=true, got false")
	}
	if resp.Job.Id != created.ID {
		t.Fatalf("expected claimed job id %s, got %s", created.ID, resp.Job.Id)
	}
}

func TestGRPC_ReportResult_Success(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	store.ClaimNext()
	client := dialGRPCServer(t, store)

	_, err := client.ReportResult(context.Background(), &jobv1.ReportResultRequest{
		Id: created.ID, Status: "succeeded", Stdout: "ok\n", ExitCode: 0,
	})
	if err != nil {
		t.Fatalf("ReportResult returned error: %v", err)
	}

	got, _ := store.Get(created.ID)
	if got.Status != job.StatusSucceeded {
		t.Fatalf("expected status succeeded, got %s", got.Status)
	}
}

func TestGRPC_ReportResult_InvalidStatus(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	client := dialGRPCServer(t, store)

	_, err := client.ReportResult(context.Background(), &jobv1.ReportResultRequest{
		Id: created.ID, Status: "bogus",
	})
	if err == nil {
		t.Fatal("expected an error for an invalid status, got nil")
	}
}

func TestGRPC_ReportResult_NotFound(t *testing.T) {
	client := dialGRPCServer(t, job.NewMemoryStore())

	_, err := client.ReportResult(context.Background(), &jobv1.ReportResultRequest{
		Id: "does-not-exist", Status: "succeeded",
	})
	if err == nil {
		t.Fatal("expected an error for an unknown job id, got nil")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/coordinator/... -run TestGRPC`
Expected: FAIL — `coordinator.NewGRPCServer` undefined.

- [ ] **Step 3: Implement**

`internal/coordinator/grpc_server.go`:
```go
package coordinator

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
	"github.com/ritvikreddygangula/forge/internal/job"
)

// GRPCServer implements jobv1.JobServiceServer over the same job.Store the
// REST Server uses — both are thin transports over one shared store.
type GRPCServer struct {
	jobv1.UnimplementedJobServiceServer
	store job.Store
}

func NewGRPCServer(store job.Store) *GRPCServer {
	return &GRPCServer{store: store}
}

func toProtoJob(j *job.Job) *jobv1.Job {
	return &jobv1.Job{
		Id:             j.ID,
		Image:          j.Image,
		Command:        j.Command,
		TimeoutSeconds: int32(j.TimeoutSeconds),
	}
}

func (s *GRPCServer) PollJob(ctx context.Context, req *jobv1.PollJobRequest) (*jobv1.PollJobResponse, error) {
	j, err := s.store.ClaimNext()
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to claim job")
	}
	if j == nil {
		return &jobv1.PollJobResponse{HasJob: false}, nil
	}
	return &jobv1.PollJobResponse{HasJob: true, Job: toProtoJob(j)}, nil
}

func (s *GRPCServer) ReportResult(ctx context.Context, req *jobv1.ReportResultRequest) (*jobv1.ReportResultResponse, error) {
	st := job.Status(req.Status)
	if st != job.StatusSucceeded && st != job.StatusFailed {
		return nil, status.Error(codes.InvalidArgument, "status must be succeeded or failed")
	}

	if err := s.store.Complete(req.Id, st, req.Stdout, req.Stderr, int(req.ExitCode)); err != nil {
		if errors.Is(err, job.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "job not found")
		}
		return nil, status.Error(codes.Internal, "failed to complete job")
	}
	return &jobv1.ReportResultResponse{}, nil
}
```

  (`StreamLogs` is added in Task 2.5 — kept out of this commit so each commit stays about one thing.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/coordinator/... -v`
Expected: PASS — the 5 new gRPC tests, plus all existing REST tests untouched.

- [ ] **Step 5: Wire it into `cmd/coordinator/main.go`** — serve gRPC on a second port, alongside the
  existing REST listener, both against the same `job.Store` instance:

```go
package main

import (
	"log/slog"
	"net"
	"net/http"
	"os"

	"google.golang.org/grpc"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
	"github.com/ritvikreddygangula/forge/internal/coordinator"
	"github.com/ritvikreddygangula/forge/internal/job"
)

func main() {
	httpAddr := os.Getenv("COORDINATOR_ADDR")
	if httpAddr == "" {
		httpAddr = ":8080"
	}
	grpcAddr := os.Getenv("COORDINATOR_GRPC_ADDR")
	if grpcAddr == "" {
		grpcAddr = ":9090"
	}

	store := job.NewMemoryStore()

	grpcLis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		slog.Error("coordinator failed to listen (gRPC)", "error", err)
		os.Exit(1)
	}
	grpcSrv := grpc.NewServer()
	jobv1.RegisterJobServiceServer(grpcSrv, coordinator.NewGRPCServer(store))
	go func() {
		slog.Info("coordinator gRPC starting", "addr", grpcAddr)
		if err := grpcSrv.Serve(grpcLis); err != nil {
			slog.Error("coordinator gRPC exited", "error", err)
			os.Exit(1)
		}
	}()

	httpSrv := coordinator.NewServer(store)
	slog.Info("coordinator HTTP starting", "addr", httpAddr)
	if err := http.ListenAndServe(httpAddr, httpSrv); err != nil {
		slog.Error("coordinator HTTP exited", "error", err)
		os.Exit(1)
	}
}
```

  Plaintext gRPC (no TLS) is a deliberate local-dev simplification, same spirit as Part 1's Docker
  sandbox caveat — worth naming explicitly rather than silently skipping, not something to carry into
  any future cloud deployment unexamined.

- [ ] **Step 6: Commit**

Commit message: `feat(coordinator): implement gRPC server alongside REST`

---

### Task 2.4: Worker gRPC client, replacing HTTP polling

**Files:** Modify `internal/worker/loop.go`, `internal/worker/loop_test.go`. Modify `cmd/worker/main.go`.

**Interfaces:**
- `worker.NewLoop(coordinatorAddr string) (*Loop, error)` now dials gRPC instead of storing a base URL
  (returns an error since dialing can fail, unlike the old string-only constructor).
- `(*Loop).RunOnce`/`(*Loop).Run` keep their exact existing signatures — this is the point of having
  kept `Execute` as an injectable field in Part 1; the transport swap shouldn't ripple into that.

- [ ] **Step 1: Write the failing tests** — replace the `httptest.Server`-based fakes with a fake
  `jobv1.JobServiceServer` served over `bufconn`, same pattern as Task 2.3's tests:

`internal/worker/loop_test.go` (full replacement):
```go
package worker_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
	"github.com/ritvikreddygangula/forge/internal/worker"
)

// fakeJobServer is a minimal jobv1.JobServiceServer double, configured per test.
type fakeJobServer struct {
	jobv1.UnimplementedJobServiceServer
	pollResp   *jobv1.PollJobResponse
	reportedID string
	reported   *jobv1.ReportResultRequest
}

func (f *fakeJobServer) PollJob(ctx context.Context, req *jobv1.PollJobRequest) (*jobv1.PollJobResponse, error) {
	return f.pollResp, nil
}

func (f *fakeJobServer) ReportResult(ctx context.Context, req *jobv1.ReportResultRequest) (*jobv1.ReportResultResponse, error) {
	f.reportedID = req.Id
	f.reported = req
	return &jobv1.ReportResultResponse{}, nil
}

func newTestLoop(t *testing.T, fake *fakeJobServer) *worker.Loop {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	jobv1.RegisterJobServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	l, err := worker.NewLoopWithDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	})
	if err != nil {
		t.Fatalf("failed to build test loop: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func TestLoop_RunOnce_NoJobQueued(t *testing.T) {
	fake := &fakeJobServer{pollResp: &jobv1.PollJobResponse{HasJob: false}}
	l := newTestLoop(t, fake)
	executed := false
	l.Execute = func(ctx context.Context, image string, command []string, timeoutSeconds int) (worker.ExecResult, error) {
		executed = true
		return worker.ExecResult{}, nil
	}

	if err := l.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	if executed {
		t.Fatal("expected Execute not to be called when no job is queued")
	}
}

func TestLoop_RunOnce_ExecutesAndReportsSuccess(t *testing.T) {
	fake := &fakeJobServer{pollResp: &jobv1.PollJobResponse{
		HasJob: true,
		Job:    &jobv1.Job{Id: "job-1", Image: "alpine", Command: []string{"true"}, TimeoutSeconds: 10},
	}}
	l := newTestLoop(t, fake)
	l.Execute = func(ctx context.Context, image string, command []string, timeoutSeconds int) (worker.ExecResult, error) {
		return worker.ExecResult{Stdout: "ok\n", ExitCode: 0}, nil
	}

	if err := l.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	if fake.reportedID != "job-1" {
		t.Fatalf("expected reported id job-1, got %q", fake.reportedID)
	}
	if fake.reported.Status != "succeeded" {
		t.Fatalf("expected status succeeded, got %v", fake.reported.Status)
	}
}

func TestLoop_RunOnce_NonZeroExitReportsFailed(t *testing.T) {
	fake := &fakeJobServer{pollResp: &jobv1.PollJobResponse{
		HasJob: true,
		Job:    &jobv1.Job{Id: "job-2", Image: "alpine", Command: []string{"false"}, TimeoutSeconds: 10},
	}}
	l := newTestLoop(t, fake)
	l.Execute = func(ctx context.Context, image string, command []string, timeoutSeconds int) (worker.ExecResult, error) {
		return worker.ExecResult{ExitCode: 1}, nil
	}

	if err := l.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	if fake.reported.Status != "failed" {
		t.Fatalf("expected status failed, got %v", fake.reported.Status)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/worker/... -run TestLoop`
Expected: FAIL (compile error) — `worker.NewLoopWithDialer` undefined.

- [ ] **Step 3: Implement**

`internal/worker/loop.go` (full replacement of the transport-facing parts; `RunOnce`/`Run` keep the same
shape):
```go
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
)

type Loop struct {
	client       jobv1.JobServiceClient
	conn         *grpc.ClientConn
	PollInterval time.Duration
	Execute      func(ctx context.Context, image string, command []string, timeoutSeconds int) (ExecResult, error)
}

// NewLoop dials the coordinator's gRPC address (e.g. "localhost:9090").
func NewLoop(coordinatorGRPCAddr string) (*Loop, error) {
	conn, err := grpc.NewClient(coordinatorGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial coordinator: %w", err)
	}
	return newLoop(conn), nil
}

// NewLoopWithDialer is the test seam — lets tests point the gRPC client at an
// in-memory bufconn listener instead of a real network address.
func NewLoopWithDialer(dialer func(context.Context, string) (net.Conn, error)) (*Loop, error) {
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial test coordinator: %w", err)
	}
	return newLoop(conn), nil
}

func newLoop(conn *grpc.ClientConn) *Loop {
	return &Loop{
		client:       jobv1.NewJobServiceClient(conn),
		conn:         conn,
		PollInterval: 2 * time.Second,
		Execute:      RunJob,
	}
}

func (l *Loop) Close() error { return l.conn.Close() }

func (l *Loop) RunOnce(ctx context.Context) error {
	resp, err := l.client.PollJob(ctx, &jobv1.PollJobRequest{})
	if err != nil {
		return fmt.Errorf("poll failed: %w", err)
	}
	if !resp.HasJob {
		return nil
	}
	j := resp.Job

	slog.Info("job claimed", "id", j.Id, "image", j.Image)

	result, execErr := l.Execute(ctx, j.Image, j.Command, int(j.TimeoutSeconds))
	status := "succeeded"
	if execErr != nil || result.ExitCode != 0 {
		status = "failed"
	}
	if execErr != nil {
		result.Stderr = result.Stderr + "\n" + execErr.Error()
	}

	_, err = l.client.ReportResult(ctx, &jobv1.ReportResultRequest{
		Id: j.Id, Status: status, Stdout: result.Stdout, Stderr: result.Stderr, ExitCode: int32(result.ExitCode),
	})
	if err != nil {
		return fmt.Errorf("report failed: %w", err)
	}
	slog.Info("job reported", "id", j.Id, "status", status)
	return nil
}

func (l *Loop) Run(ctx context.Context) {
	ticker := time.NewTicker(l.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := l.RunOnce(ctx); err != nil {
				slog.Error("worker loop iteration failed", "error", err)
			}
		}
	}
}

var errNotImplemented = errors.New("not implemented")
```

  (`errNotImplemented` is a placeholder that shouldn't survive review — drop it if nothing ends up using
  it; called out here rather than silently left in.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/worker/... -v`
Expected: PASS — all `TestLoop_*` tests; `TestRunJob_*` (the `integration`-tagged executor tests) are
unaffected since `executor.go` isn't touched by this Part.

- [ ] **Step 5: Update `cmd/worker/main.go`** to dial the gRPC address instead of an HTTP base URL:

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ritvikreddygangula/forge/internal/worker"
)

func main() {
	coordinatorAddr := os.Getenv("COORDINATOR_GRPC_ADDR")
	if coordinatorAddr == "" {
		coordinatorAddr = "localhost:9090"
	}

	l, err := worker.NewLoop(coordinatorAddr)
	if err != nil {
		slog.Error("worker failed to connect", "error", err)
		os.Exit(1)
	}
	defer l.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("worker starting", "coordinator", coordinatorAddr)
	l.Run(ctx)
	slog.Info("worker stopped")
}
```

- [ ] **Step 6: Manual end-to-end verification** (same shape as Part 1 Task 1.6, now over gRPC)

```bash
colima start   # if not already running
make run-coordinator   # now also logs "coordinator gRPC starting addr=:9090"
make run-worker         # now logs "worker starting coordinator=localhost:9090"

curl -s -X POST localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"image":"alpine:3.19","command":["echo","hello over grpc"],"timeout_seconds":30}'
# copy the id, then:
curl -s localhost:8080/jobs/<id>
```

Expected: identical result to Part 1 — `"status":"succeeded"`, `"stdout":"hello over grpc\n"` — proving
the transport swap didn't change behavior.

- [ ] **Step 7: Commit**

Commit message: `feat(worker): replace HTTP polling client with gRPC client`

---

### Task 2.5: StreamLogs over gRPC

**Files:** Modify `internal/coordinator/grpc_server.go`, `internal/coordinator/grpc_server_test.go`.

**Scope note:** the executor still captures stdout/stderr as complete buffers after `docker run`
finishes (Part 1 behavior, unchanged) — there's no live output capture yet. So `StreamLogs` here sends
whatever the store currently holds as one or two chunks and closes, the same information the REST
`/jobs/{id}/logs` endpoint already returns, just over a streaming RPC shape. True line-by-line live
tailing would need the executor to capture incrementally, which is out of scope for this Part.

- [ ] **Step 1: Write the failing test**

Append to `internal/coordinator/grpc_server_test.go`:
```go
func TestGRPC_StreamLogs(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	store.ClaimNext()
	store.Complete(created.ID, job.StatusSucceeded, "hello\n", "", 0)
	client := dialGRPCServer(t, store)

	stream, err := client.StreamLogs(context.Background(), &jobv1.StreamLogsRequest{Id: created.ID})
	if err != nil {
		t.Fatalf("StreamLogs returned error: %v", err)
	}

	var chunks []*jobv1.LogChunk
	for {
		chunk, err := stream.Recv()
		if err != nil {
			break // io.EOF ends the stream; any other error fails via chunk count below
		}
		chunks = append(chunks, chunk)
	}
	if len(chunks) != 1 || chunks[0].Data != "hello\n" {
		t.Fatalf("expected one stdout chunk %q, got %+v", "hello\n", chunks)
	}
}

func TestGRPC_StreamLogs_NotFound(t *testing.T) {
	client := dialGRPCServer(t, job.NewMemoryStore())

	stream, err := client.StreamLogs(context.Background(), &jobv1.StreamLogsRequest{Id: "does-not-exist"})
	if err != nil {
		t.Fatalf("StreamLogs returned error: %v", err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("expected an error receiving from the stream for an unknown job id, got nil")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/coordinator/... -run TestGRPC_StreamLogs`
Expected: FAIL — `StreamLogs` not implemented (embedding `UnimplementedJobServiceServer` makes it
compile, but calling it returns an `Unimplemented` status, not the expected chunks).

- [ ] **Step 3: Implement**

Add to `internal/coordinator/grpc_server.go`:
```go
func (s *GRPCServer) StreamLogs(req *jobv1.StreamLogsRequest, stream jobv1.JobService_StreamLogsServer) error {
	j, err := s.store.Get(req.Id)
	if err != nil {
		if errors.Is(err, job.ErrNotFound) {
			return status.Error(codes.NotFound, "job not found")
		}
		return status.Error(codes.Internal, "failed to get job")
	}
	if j.Stdout != "" {
		if err := stream.Send(&jobv1.LogChunk{Stream: "stdout", Data: j.Stdout}); err != nil {
			return err
		}
	}
	if j.Stderr != "" {
		if err := stream.Send(&jobv1.LogChunk{Stream: "stderr", Data: j.Stderr}); err != nil {
			return err
		}
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/coordinator/... -v`
Expected: PASS, all gRPC tests including the two new `StreamLogs` cases.

- [ ] **Step 5: Commit**

Commit message: `feat(coordinator): stream logs over gRPC for GET /jobs/{id}/logs parity`

---

### Task 2.6: End-to-end gRPC integration test

**Files:** Create `internal/coordinator/integration_test.go`.

Automates the manual verification from Task 2.4 Step 6: a real (non-bufconn) coordinator serving both
REST and gRPC on `localhost` random ports, a real `worker.Loop` dialing the gRPC port with a fake
`Execute` (no real Docker needed — this test is about the transport, not the executor, which already
has its own `integration`-tagged tests), one full submit → poll → execute → report → GET cycle.

- [ ] **Step 1: Write the test**

`internal/coordinator/integration_test.go`:
```go
package coordinator_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
	"github.com/ritvikreddygangula/forge/internal/coordinator"
	"github.com/ritvikreddygangula/forge/internal/job"
	"github.com/ritvikreddygangula/forge/internal/worker"
)

func TestEndToEnd_SubmitPollExecuteReport(t *testing.T) {
	store := job.NewMemoryStore()

	restSrv := httptest.NewServer(coordinator.NewServer(store))
	defer restSrv.Close()

	grpcLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	jobv1.RegisterJobServiceServer(grpcSrv, coordinator.NewGRPCServer(store))
	go func() { _ = grpcSrv.Serve(grpcLis) }()
	defer grpcSrv.Stop()

	l, err := worker.NewLoop(grpcLis.Addr().String())
	if err != nil {
		t.Fatalf("failed to build worker loop: %v", err)
	}
	defer l.Close()
	l.Execute = func(ctx context.Context, image string, command []string, timeoutSeconds int) (worker.ExecResult, error) {
		return worker.ExecResult{Stdout: "hello from test\n", ExitCode: 0}, nil
	}

	resp, err := http.Post(restSrv.URL+"/jobs", "application/json",
		httptestBody(`{"image":"alpine","command":["echo","hi"],"timeout_seconds":10}`))
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	var submitted map[string]any
	json.NewDecoder(resp.Body).Decode(&submitted)
	resp.Body.Close()
	id := submitted["id"].(string)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := l.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	got, err := store.Get(id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.Status != job.StatusSucceeded {
		t.Fatalf("expected status succeeded, got %s", got.Status)
	}
	if got.Stdout != "hello from test\n" {
		t.Fatalf("expected stdout %q, got %q", "hello from test\n", got.Stdout)
	}
}

func httpTestBody(s string) *bytesReader { return newBytesReader(s) }
```

  (`httptestBody`/`bytesReader` — use `strings.NewReader` from the standard library directly; the
  placeholder names above are a reminder to wire that up during implementation, not code to type
  verbatim. `net/http.Post`'s third argument wants an `io.Reader`, so `strings.NewReader(jsonBody)` is
  the actual call — this note gets deleted once the real code is written.)

- [ ] **Step 2: Run test to verify it fails, then passes once wired correctly**

Run: `go test ./internal/coordinator/... -run TestEndToEnd -v`
Expected: PASS once the `strings.NewReader` fix above is applied — confirms submit-via-REST,
execute-via-gRPC-worker, and read-result-via-store all agree on one job's final state.

- [ ] **Step 3: Commit**

Commit message: `test: add gRPC integration test covering submit to result`

---

### Task 2.7: Remove the superseded HTTP worker endpoints

**Files:** Delete `internal/coordinator/worker_handlers.go`, `internal/coordinator/worker_handlers_test.go`.
Modify `internal/coordinator/server.go`.

`internal/coordinator/failing_store_test.go` **stays** — `server_test.go` (the REST submit/get/logs
tests) still uses the shared `failingStore` double for its own error-path tests; it isn't specific to
the worker-polling endpoints being removed.

- [ ] **Step 1:** Delete `internal/coordinator/worker_handlers.go` and
  `internal/coordinator/worker_handlers_test.go`.

- [ ] **Step 2:** In `internal/coordinator/server.go`, remove the two now-dead routes from `routes()`:

```go
func (s *Server) routes() {
	s.mux.HandleFunc("POST /jobs", s.handleSubmitJob)
	s.mux.HandleFunc("GET /jobs/{id}", s.handleGetJob)
	s.mux.HandleFunc("GET /jobs/{id}/logs", s.handleGetJobLogs)
}
```

- [ ] **Step 3: Verify**

Run: `go build ./... && go test ./...`
Expected: builds clean, all tests pass — the deleted HTTP worker tests are gone, the new gRPC tests
(Tasks 2.3/2.5) cover the same ground, and the REST submit/get/logs tests (Task 1.2/1.7, unaffected)
still pass.

- [ ] **Step 4: Commit**

Commit message: `chore: remove worker-facing HTTP endpoints superseded by gRPC`

---

### Task 2.8: Documentation

**Files:** Modify `README.md`, `PROGRESS.md`.

- [ ] **Step 1:** Update `README.md`'s "Environment variables" section to add `COORDINATOR_GRPC_ADDR`
  (default `:9090` on the coordinator side, `localhost:9090` as the worker's default target), and note
  in "Running locally" that the coordinator now logs two listeners on startup.

- [ ] **Step 2:** Append a `## Part 2 — gRPC transport → part-2-grpc (merged via PR #2)` section to
  `PROGRESS.md`, one line per commit, following the format established in Part 1's section.

- [ ] **Step 3: Commit**

Commit message: `docs: update PROGRESS.md for Part 2`

**Part 2 complete.** `make test` (and, with Colima running, `make test-integration`) both pass. Hand off
to Ritvik to open the PR for `part-2-grpc` → `main`.

**PR title:** `Part 2: swap coordinator-worker transport to gRPC`
**PR description points:** worker-facing transport is now gRPC (`PollJob`, `ReportResult`, `StreamLogs`)
instead of HTTP polling; REST external surface unchanged; generated code committed, regenerate via
`make proto`; end-to-end behavior verified identical to Part 1 via both manual `curl` walkthrough and an
automated integration test; old HTTP worker endpoints removed once gRPC path was proven.

---

## Self-review

- **Spec/roadmap coverage:** matches `docs/plans/roadmap.md`'s 8-commit Part 2 sequence exactly, one
  commit message per task above, in the same order.
- **Placeholder scan:** one intentional flag left in Task 2.4's draft (`errNotImplemented`) and one in
  Task 2.6 (`httpTestBody`/`bytesReader` naming) — both called out explicitly as things to clean up
  during implementation, not left silently ambiguous.
- **Type/interface consistency:** `jobv1.JobServiceServer` (generated in Task 2.1/2.2) is implemented
  once by `GRPCServer` (Task 2.3, extended in Task 2.5) and consumed once by `worker.Loop` via
  `jobv1.JobServiceClient` (Task 2.4) — no duplicate definitions. `Loop.Execute`'s signature is
  byte-for-byte unchanged from Part 1, so Task 2.4 doesn't ripple into `executor.go` at all.
- **Manual step called out up front**, not discovered mid-task: the `buf`/protoc-plugin install is
  flagged before Task 2.1 rather than surfacing as a surprise build failure during Task 2.2.
