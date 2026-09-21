# Distributed Job Orchestrator — Part 0 + Part 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scaffold the repo (Part 0), then build the first fully working vertical slice: submit a job over plain HTTP, one worker polls, runs it in a Docker container, reports the result back, and the caller can read status + logs (Part 1). No gRPC, no Kafka, no Raft yet — those are separate later plans.

**Architecture:** A `coordinator` HTTP server backed by an in-memory job store (queued → running → succeeded/failed), and a `worker` binary that polls the coordinator over HTTP, executes jobs via `docker run` (through `os/exec`, not the Docker SDK — keeps this Part about the job lifecycle, not Docker SDK API surface), and reports results back.

**Tech Stack:** Go 1.23+, stdlib `net/http` (Go 1.22+ pattern routing), `github.com/google/uuid`, `log/slog`, stdlib `testing`, `golangci-lint`, GitHub Actions. See the roadmap doc for full-project stack rationale.

**Spec:** `docs/spec.md`. **Roadmap:** `docs/plans/roadmap.md`.

## Global Constraints

- **Git:** Claude never runs git commands. Every "Commit" step below ends with a commit message to hand to Ritvik, not a `git commit` invocation.
- **Branching:** Part 0 tasks commit straight to `main`. Part 1 tasks (1.1 onward) happen on branch `part-1-http-skeleton`, cut from `main` after Part 0 lands.
- Module path: `github.com/ritvikreddygangula/forge` — adjust everywhere below if the actual GitHub remote differs.
- Job schema for Part 1 (deliberately minimal — `inputs` as structured env vars is a later refinement once Part 1's lifecycle is proven): `{image string, command []string, timeout_seconds int}`.
- A worker's local Docker daemon (Colima) must be running to execute jobs — this is a local dev prerequisite, not something the coordinator manages.

## Prerequisites (one-time environment setup, not a commit)

Confirmed not yet installed on this machine — run before Task 0.1:

```bash
brew install go colima docker
colima start
docker info   # should succeed once colima is up
```

## File Structure

```
forge/
  go.mod
  go.sum
  .gitignore
  .golangci.yml
  Makefile
  PROGRESS.md
  .github/workflows/ci.yml
  cmd/
    coordinator/main.go
    worker/main.go
  internal/
    job/
      job.go
      store.go
      store_test.go
    coordinator/
      server.go
      server_test.go
      worker_handlers.go
      worker_handlers_test.go
    worker/
      executor.go
      executor_test.go   (build tag: integration)
      loop.go
      loop_test.go
```

---

## Part 0 — Repo scaffold

### Task 0.1: Go module + minimal buildable skeleton

**Files:**
- Create: `go.mod`
- Create: `.gitignore`
- Create: `cmd/coordinator/main.go`
- Create: `cmd/worker/main.go`

**Interfaces:**
- Produces: module path `github.com/ritvikreddygangula/forge`, two buildable (but not-yet-functional) binaries.

- [ ] **Step 1: Initialize the module**

```bash
go mod init github.com/ritvikreddygangula/forge
```

- [ ] **Step 2: Add `.gitignore`**

```gitignore
/bin/
*.log
.DS_Store
```

- [ ] **Step 3: Add placeholder binaries**

`cmd/coordinator/main.go`:
```go
package main

import "log/slog"

func main() {
	slog.Info("coordinator: not yet implemented")
}
```

`cmd/worker/main.go`:
```go
package main

import "log/slog"

func main() {
	slog.Info("worker: not yet implemented")
}
```

- [ ] **Step 4: Verify it builds**

Run: `go build ./...`
Expected: exits 0, no output.

- [ ] **Step 5: Commit**

Commit message: `chore: scaffold Go module and repo layout`

---

### Task 0.2: Makefile

**Files:**
- Create: `Makefile`

- [ ] **Step 1: Write the Makefile**

```makefile
.PHONY: build test test-integration lint run-coordinator run-worker

build:
	go build ./...

test:
	go test ./...

test-integration:
	go test -tags=integration ./...

lint:
	golangci-lint run

run-coordinator:
	go run ./cmd/coordinator

run-worker:
	go run ./cmd/worker
```

- [ ] **Step 2: Verify targets work**

Run: `make build && make test`
Expected: both exit 0 (`make test` reports "no test files" — expected, none exist yet).

- [ ] **Step 3: Commit**

Commit message: `chore: add Makefile with build/test/lint targets`

---

### Task 0.3: Lint config + CI workflow

**Files:**
- Create: `.golangci.yml`
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Write `.golangci.yml`**

```yaml
run:
  timeout: 3m

linters:
  enable:
    - govet
    - staticcheck
    - unused
    - errcheck
```

- [ ] **Step 2: Write `.github/workflows/ci.yml`**

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:

jobs:
  build-test-lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.23"
      - run: go build ./...
      - run: go vet ./...
      - run: go test ./...
      - uses: golangci/golangci-lint-action@v6
        with:
          version: latest
```

Note: this runs `go test ./...` only (no `-tags=integration`) so CI never needs a Docker daemon for Part 1. Wire an integration-test CI job explicitly (GitHub-hosted runners do have Docker) when it's actually worth the runtime cost — not required for Part 0/1.

- [ ] **Step 3: Verify locally**

Run: `golangci-lint run` (install via `brew install golangci-lint` if missing)
Expected: exits 0, no findings (nothing to lint yet beyond the two placeholder files).

- [ ] **Step 4: Commit**

Commit message: `ci: add GitHub Actions workflow for build, vet, lint, test`

---

### Task 0.4: PROGRESS.md

**Files:**
- Create: `PROGRESS.md`

- [ ] **Step 1: Seed the progress log**

```markdown
# Progress Log

Cross-session continuity log. Update at the end of every session, even short ones.

## Session log

### 2026-09-19
- Wrote roadmap plan (`docs/superpowers/plans/2026-09-19-distributed-orchestrator-roadmap.md`) covering all 9 Parts, tech stack decisions, and commit sequencing.
- Wrote detailed Part 0 + Part 1 implementation plan.
- Next: execute Part 0 scaffold tasks, then start Part 1 on branch `part-1-http-skeleton`.
```

- [ ] **Step 2: Commit**

Commit message: `docs: add PROGRESS.md for cross-session continuity`

**Part 0 complete → Ritvik merges straight to `main` (no PR needed per spec's git workflow). Part 1 starts on a fresh branch cut from `main`.**

---

## Part 1 — Plain HTTP skeleton

*(branch: `part-1-http-skeleton`)*

### Task 1.1: Job domain model + in-memory store

**Files:**
- Create: `internal/job/job.go`
- Create: `internal/job/store.go`
- Test: `internal/job/store_test.go`

**Interfaces:**
- Produces: `job.Status` (`StatusQueued`, `StatusRunning`, `StatusSucceeded`, `StatusFailed`), `job.Job` struct, `job.Store` interface (`Create`, `Get`, `ClaimNext`, `Complete`), `job.NewMemoryStore() *job.MemoryStore`, `job.ErrNotFound`.

- [ ] **Step 1: Add the `google/uuid` dependency**

```bash
go get github.com/google/uuid
```

- [ ] **Step 2: Write the failing test**

`internal/job/store_test.go`:
```go
package job_test

import (
	"testing"

	"github.com/ritvikreddygangula/forge/internal/job"
)

func TestMemoryStore_CreateAndGet(t *testing.T) {
	s := job.NewMemoryStore()
	j, err := s.Create("python:3.11", []string{"pytest", "tests/"}, 300)
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if j.Status != job.StatusQueued {
		t.Fatalf("expected status queued, got %s", j.Status)
	}

	got, err := s.Get(j.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.ID != j.ID {
		t.Fatalf("expected id %s, got %s", j.ID, got.ID)
	}
}

func TestMemoryStore_Get_NotFound(t *testing.T) {
	s := job.NewMemoryStore()
	_, err := s.Get("does-not-exist")
	if err != job.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemoryStore_ClaimNext_FIFO(t *testing.T) {
	s := job.NewMemoryStore()
	first, _ := s.Create("alpine", []string{"true"}, 10)
	s.Create("alpine", []string{"true"}, 10)

	claimed, err := s.ClaimNext()
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected a claimed job, got nil")
	}
	if claimed.ID != first.ID {
		t.Fatalf("expected FIFO claim of %s, got %s", first.ID, claimed.ID)
	}
	if claimed.Status != job.StatusRunning {
		t.Fatalf("expected claimed job status running, got %s", claimed.Status)
	}
}

func TestMemoryStore_ClaimNext_EmptyQueue(t *testing.T) {
	s := job.NewMemoryStore()
	claimed, err := s.ClaimNext()
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected nil when queue empty, got %+v", claimed)
	}
}

func TestMemoryStore_Complete(t *testing.T) {
	s := job.NewMemoryStore()
	j, _ := s.Create("alpine", []string{"true"}, 10)
	s.ClaimNext()

	if err := s.Complete(j.ID, job.StatusSucceeded, "ok\n", "", 0); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}

	got, _ := s.Get(j.ID)
	if got.Status != job.StatusSucceeded {
		t.Fatalf("expected status succeeded, got %s", got.Status)
	}
	if got.Stdout != "ok\n" {
		t.Fatalf("expected stdout %q, got %q", "ok\n", got.Stdout)
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/job/...`
Expected: FAIL — package `internal/job` doesn't exist yet.

- [ ] **Step 4: Implement the domain model**

`internal/job/job.go`:
```go
package job

import "time"

type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

type Job struct {
	ID             string
	Image          string
	Command        []string
	TimeoutSeconds int
	Status         Status
	Stdout         string
	Stderr         string
	ExitCode       int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
```

- [ ] **Step 5: Implement the store**

`internal/job/store.go`:
```go
package job

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

var ErrNotFound = errors.New("job not found")

type Store interface {
	Create(image string, command []string, timeoutSeconds int) (*Job, error)
	Get(id string) (*Job, error)
	ClaimNext() (*Job, error)
	Complete(id string, status Status, stdout, stderr string, exitCode int) error
}

type MemoryStore struct {
	mu    sync.Mutex
	jobs  map[string]*Job
	order []string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{jobs: make(map[string]*Job)}
}

func (s *MemoryStore) Create(image string, command []string, timeoutSeconds int) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	j := &Job{
		ID:             uuid.NewString(),
		Image:          image,
		Command:        command,
		TimeoutSeconds: timeoutSeconds,
		Status:         StatusQueued,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	s.jobs[j.ID] = j
	s.order = append(s.order, j.ID)

	cp := *j
	return &cp, nil
}

func (s *MemoryStore) Get(id string) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *j
	return &cp, nil
}

func (s *MemoryStore) ClaimNext() (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, id := range s.order {
		j := s.jobs[id]
		if j.Status == StatusQueued {
			j.Status = StatusRunning
			j.UpdatedAt = time.Now()
			cp := *j
			return &cp, nil
		}
	}
	return nil, nil
}

func (s *MemoryStore) Complete(id string, status Status, stdout, stderr string, exitCode int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return ErrNotFound
	}
	j.Status = status
	j.Stdout = stdout
	j.Stderr = stderr
	j.ExitCode = exitCode
	j.UpdatedAt = time.Now()
	return nil
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/job/... -v`
Expected: PASS, all 5 tests.

- [ ] **Step 7: Commit**

Commit message: `feat(job): add in-memory job store with state transitions`

---

### Task 1.2: Coordinator HTTP handlers — submit + get

**Files:**
- Create: `internal/coordinator/server.go`
- Test: `internal/coordinator/server_test.go`

**Interfaces:**
- Consumes: `job.Store` interface, `job.NewMemoryStore()` from Task 1.1.
- Produces: `coordinator.NewServer(store job.Store) *coordinator.Server`, `(*Server).ServeHTTP` (so `Server` is a drop-in `http.Handler`).

- [ ] **Step 1: Write the failing tests**

`internal/coordinator/server_test.go`:
```go
package coordinator_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ritvikreddygangula/forge/internal/coordinator"
	"github.com/ritvikreddygangula/forge/internal/job"
)

func TestHandleSubmitJob(t *testing.T) {
	srv := coordinator.NewServer(job.NewMemoryStore())

	body := []byte(`{"image":"python:3.11","command":["pytest","tests/"],"timeout_seconds":60}`)
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}
	if got["status"] != "queued" {
		t.Fatalf("expected status queued, got %v", got["status"])
	}
	if got["id"] == "" || got["id"] == nil {
		t.Fatalf("expected non-empty id in response, got %v", got["id"])
	}
}

func TestHandleSubmitJob_MissingFields(t *testing.T) {
	srv := coordinator.NewServer(job.NewMemoryStore())

	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader([]byte(`{}`)))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleGetJob(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	srv := coordinator.NewServer(store)

	req := httptest.NewRequest(http.MethodGet, "/jobs/"+created.ID, nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var got map[string]any
	json.Unmarshal(rec.Body.Bytes(), &got)
	if got["id"] != created.ID {
		t.Fatalf("expected id %s, got %v", created.ID, got["id"])
	}
}

func TestHandleGetJob_NotFound(t *testing.T) {
	srv := coordinator.NewServer(job.NewMemoryStore())

	req := httptest.NewRequest(http.MethodGet, "/jobs/does-not-exist", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/coordinator/...`
Expected: FAIL — package doesn't exist yet.

- [ ] **Step 3: Implement the server**

`internal/coordinator/server.go`:
```go
package coordinator

import (
	"encoding/json"
	"net/http"

	"github.com/ritvikreddygangula/forge/internal/job"
)

type Server struct {
	store job.Store
	mux   *http.ServeMux
}

func NewServer(store job.Store) *Server {
	s := &Server{store: store, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /jobs", s.handleSubmitJob)
	s.mux.HandleFunc("GET /jobs/{id}", s.handleGetJob)
}

type submitJobRequest struct {
	Image          string   `json:"image"`
	Command        []string `json:"command"`
	TimeoutSeconds int      `json:"timeout_seconds"`
}

type jobResponse struct {
	ID             string   `json:"id"`
	Image          string   `json:"image"`
	Command        []string `json:"command"`
	TimeoutSeconds int      `json:"timeout_seconds"`
	Status         string   `json:"status"`
	ExitCode       int      `json:"exit_code"`
	Stdout         string   `json:"stdout,omitempty"`
	Stderr         string   `json:"stderr,omitempty"`
}

func toJobResponse(j *job.Job) jobResponse {
	return jobResponse{
		ID:             j.ID,
		Image:          j.Image,
		Command:        j.Command,
		TimeoutSeconds: j.TimeoutSeconds,
		Status:         string(j.Status),
		ExitCode:       j.ExitCode,
		Stdout:         j.Stdout,
		Stderr:         j.Stderr,
	}
}

func (s *Server) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var req submitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.Image == "" || len(req.Command) == 0 {
		http.Error(w, "image and command are required", http.StatusBadRequest)
		return
	}
	if req.TimeoutSeconds <= 0 {
		req.TimeoutSeconds = 300
	}

	j, err := s.store.Create(req.Image, req.Command, req.TimeoutSeconds)
	if err != nil {
		http.Error(w, "failed to create job", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(toJobResponse(j))
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, err := s.store.Get(id)
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(toJobResponse(j))
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/coordinator/... -v`
Expected: PASS, all 4 tests.

- [ ] **Step 5: Commit**

Commit message: `feat(coordinator): add POST /jobs and GET /jobs/{id} handlers`

---

### Task 1.3: Worker-facing poll + result endpoints

**Files:**
- Modify: `internal/coordinator/server.go` (add route registrations only)
- Create: `internal/coordinator/worker_handlers.go`
- Test: `internal/coordinator/worker_handlers_test.go`

**Interfaces:**
- Consumes: `job.Store.ClaimNext`, `job.Store.Complete` from Task 1.1; `toJobResponse` from Task 1.2.
- Produces: `GET /internal/worker/poll` → `{"job": null | {...jobResponse}}`; `POST /internal/worker/result` (body `{id, status, stdout, stderr, exit_code}`) → `204 No Content`.

- [ ] **Step 1: Write the failing tests**

`internal/coordinator/worker_handlers_test.go`:
```go
package coordinator_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ritvikreddygangula/forge/internal/coordinator"
	"github.com/ritvikreddygangula/forge/internal/job"
)

func TestHandleWorkerPoll_EmptyQueue(t *testing.T) {
	srv := coordinator.NewServer(job.NewMemoryStore())

	req := httptest.NewRequest(http.MethodGet, "/internal/worker/poll", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	var got map[string]any
	json.Unmarshal(rec.Body.Bytes(), &got)
	if got["job"] != nil {
		t.Fatalf("expected job: null when queue empty, got %v", got["job"])
	}
}

func TestHandleWorkerPoll_ClaimsQueuedJob(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	srv := coordinator.NewServer(store)

	req := httptest.NewRequest(http.MethodGet, "/internal/worker/poll", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	var got map[string]any
	json.Unmarshal(rec.Body.Bytes(), &got)
	claimed := got["job"].(map[string]any)
	if claimed["id"] != created.ID {
		t.Fatalf("expected claimed job id %s, got %v", created.ID, claimed["id"])
	}
	if claimed["status"] != "running" {
		t.Fatalf("expected claimed job status running, got %v", claimed["status"])
	}
}

func TestHandleWorkerResult_Success(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	store.ClaimNext()
	srv := coordinator.NewServer(store)

	body := []byte(`{"id":"` + created.ID + `","status":"succeeded","stdout":"ok\n","stderr":"","exit_code":0}`)
	req := httptest.NewRequest(http.MethodPost, "/internal/worker/result", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
	}

	got, _ := store.Get(created.ID)
	if got.Status != job.StatusSucceeded {
		t.Fatalf("expected status succeeded, got %s", got.Status)
	}
}

func TestHandleWorkerResult_InvalidStatus(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	srv := coordinator.NewServer(store)

	body := []byte(`{"id":"` + created.ID + `","status":"bogus"}`)
	req := httptest.NewRequest(http.MethodPost, "/internal/worker/result", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/coordinator/... -run TestHandleWorker`
Expected: FAIL — routes don't exist yet (404s where 200/204 expected).

- [ ] **Step 3: Register the new routes**

Modify `internal/coordinator/server.go`, inside `routes()`:
```go
func (s *Server) routes() {
	s.mux.HandleFunc("POST /jobs", s.handleSubmitJob)
	s.mux.HandleFunc("GET /jobs/{id}", s.handleGetJob)
	s.mux.HandleFunc("GET /internal/worker/poll", s.handleWorkerPoll)
	s.mux.HandleFunc("POST /internal/worker/result", s.handleWorkerResult)
}
```

- [ ] **Step 4: Implement the handlers**

`internal/coordinator/worker_handlers.go`:
```go
package coordinator

import (
	"encoding/json"
	"net/http"

	"github.com/ritvikreddygangula/forge/internal/job"
)

type pollResponse struct {
	Job *jobResponse `json:"job"`
}

func (s *Server) handleWorkerPoll(w http.ResponseWriter, r *http.Request) {
	j, err := s.store.ClaimNext()
	if err != nil {
		http.Error(w, "failed to claim job", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if j == nil {
		json.NewEncoder(w).Encode(pollResponse{})
		return
	}
	resp := toJobResponse(j)
	json.NewEncoder(w).Encode(pollResponse{Job: &resp})
}

type reportResultRequest struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exit_code"`
}

func (s *Server) handleWorkerResult(w http.ResponseWriter, r *http.Request) {
	var req reportResultRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	status := job.Status(req.Status)
	if status != job.StatusSucceeded && status != job.StatusFailed {
		http.Error(w, "status must be succeeded or failed", http.StatusBadRequest)
		return
	}

	if err := s.store.Complete(req.ID, status, req.Stdout, req.Stderr, req.ExitCode); err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/coordinator/... -v`
Expected: PASS, all 8 tests (4 from Task 1.2 + 4 new).

- [ ] **Step 6: Commit**

Commit message: `feat(coordinator): add worker poll and result-reporting endpoints`

---

### Task 1.4: Docker-based job executor (worker side)

**Files:**
- Create: `internal/worker/executor.go`
- Test: `internal/worker/executor_test.go` (build tag `integration` — requires Colima/Docker running)

**Interfaces:**
- Produces: `worker.ExecResult{Stdout, Stderr, ExitCode string/string/int}`, `worker.RunJob(ctx context.Context, image string, command []string, timeoutSeconds int) (ExecResult, error)`.

- [ ] **Step 1: Write the failing integration test**

`internal/worker/executor_test.go`:
```go
//go:build integration

package worker_test

import (
	"context"
	"testing"

	"github.com/ritvikreddygangula/forge/internal/worker"
)

func TestRunJob_Success(t *testing.T) {
	result, err := worker.RunJob(context.Background(), "alpine:3.19", []string{"echo", "hello"}, 30)
	if err != nil {
		t.Fatalf("RunJob returned error: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", result.ExitCode)
	}
	if result.Stdout != "hello\n" {
		t.Fatalf("expected stdout %q, got %q", "hello\n", result.Stdout)
	}
}

func TestRunJob_NonZeroExit(t *testing.T) {
	result, err := worker.RunJob(context.Background(), "alpine:3.19", []string{"sh", "-c", "exit 3"}, 30)
	if err != nil {
		t.Fatalf("RunJob returned error: %v", err)
	}
	if result.ExitCode != 3 {
		t.Fatalf("expected exit code 3, got %d", result.ExitCode)
	}
}

func TestRunJob_Timeout(t *testing.T) {
	_, err := worker.RunJob(context.Background(), "alpine:3.19", []string{"sleep", "5"}, 1)
	if err == nil {
		t.Fatal("expected a timeout error, got nil")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags=integration ./internal/worker/...`
Expected: FAIL — `worker.RunJob` undefined.

- [ ] **Step 3: Implement the executor**

`internal/worker/executor.go`:
```go
package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"
)

type ExecResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

func RunJob(ctx context.Context, image string, command []string, timeoutSeconds int) (ExecResult, error) {
	timeout := time.Duration(timeoutSeconds) * time.Second
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	args := append([]string{"run", "--rm", image}, command...)
	cmd := exec.CommandContext(ctx, "docker", args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	runErr := cmd.Run()
	result := ExecResult{Stdout: stdout.String(), Stderr: stderr.String()}

	if ctx.Err() == context.DeadlineExceeded {
		return result, fmt.Errorf("job timed out after %s", timeout)
	}
	if runErr == nil {
		return result, nil
	}

	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		result.ExitCode = exitErr.ExitCode()
		return result, nil // non-zero exit is a valid job outcome, not a Go error
	}

	return result, fmt.Errorf("failed to run docker: %w", runErr)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `colima start` (if not already running), then `go test -tags=integration ./internal/worker/... -v`
Expected: PASS, all 3 tests (first run pulls `alpine:3.19`, so allow extra time).

- [ ] **Step 5: Commit**

Commit message: `feat(worker): add Docker-based job executor`

---

### Task 1.5: Worker poll-execute-report loop

**Files:**
- Create: `internal/worker/loop.go`
- Test: `internal/worker/loop_test.go`

**Interfaces:**
- Consumes: `worker.ExecResult`, `worker.RunJob` from Task 1.4 (as the default, overridable `Execute` func — this is what makes `RunOnce` unit-testable without a real Docker daemon).
- Produces: `worker.NewLoop(coordinatorURL string) *Loop`, `(*Loop).RunOnce(ctx) error`, `(*Loop).Run(ctx)` (blocks, polls on a ticker until ctx is cancelled).

- [ ] **Step 1: Write the failing tests**

`internal/worker/loop_test.go`:
```go
package worker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ritvikreddygangula/forge/internal/worker"
)

func TestLoop_RunOnce_NoJobQueued(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"job": nil})
	}))
	defer srv.Close()

	l := worker.NewLoop(srv.URL)
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
	var reported map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/worker/poll", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"job": map[string]any{"id": "job-1", "image": "alpine", "command": []string{"true"}, "timeout_seconds": 10},
		})
	})
	mux.HandleFunc("POST /internal/worker/result", func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&reported)
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	l := worker.NewLoop(srv.URL)
	l.Execute = func(ctx context.Context, image string, command []string, timeoutSeconds int) (worker.ExecResult, error) {
		return worker.ExecResult{Stdout: "ok\n", ExitCode: 0}, nil
	}

	if err := l.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	if reported["status"] != "succeeded" {
		t.Fatalf("expected status succeeded, got %v", reported["status"])
	}
	if reported["id"] != "job-1" {
		t.Fatalf("expected id job-1, got %v", reported["id"])
	}
}

func TestLoop_RunOnce_NonZeroExitReportsFailed(t *testing.T) {
	var reported map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/worker/poll", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"job": map[string]any{"id": "job-2", "image": "alpine", "command": []string{"false"}, "timeout_seconds": 10},
		})
	})
	mux.HandleFunc("POST /internal/worker/result", func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&reported)
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	l := worker.NewLoop(srv.URL)
	l.Execute = func(ctx context.Context, image string, command []string, timeoutSeconds int) (worker.ExecResult, error) {
		return worker.ExecResult{ExitCode: 1}, nil
	}

	if err := l.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	if reported["status"] != "failed" {
		t.Fatalf("expected status failed, got %v", reported["status"])
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/worker/... -run TestLoop`
Expected: FAIL — `worker.Loop` undefined.

- [ ] **Step 3: Implement the loop**

`internal/worker/loop.go`:
```go
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type Loop struct {
	CoordinatorURL string
	HTTPClient     *http.Client
	PollInterval   time.Duration
	Execute        func(ctx context.Context, image string, command []string, timeoutSeconds int) (ExecResult, error)
}

func NewLoop(coordinatorURL string) *Loop {
	return &Loop{
		CoordinatorURL: coordinatorURL,
		HTTPClient:     &http.Client{Timeout: 10 * time.Second},
		PollInterval:   2 * time.Second,
		Execute:        RunJob,
	}
}

type polledJob struct {
	ID             string   `json:"id"`
	Image          string   `json:"image"`
	Command        []string `json:"command"`
	TimeoutSeconds int      `json:"timeout_seconds"`
}

type pollResponse struct {
	Job *polledJob `json:"job"`
}

func (l *Loop) pollOnce(ctx context.Context) (*polledJob, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, l.CoordinatorURL+"/internal/worker/poll", nil)
	if err != nil {
		return nil, err
	}
	resp, err := l.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pr pollResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return nil, err
	}
	return pr.Job, nil
}

func (l *Loop) reportResult(ctx context.Context, id, status string, result ExecResult) error {
	payload, err := json.Marshal(map[string]any{
		"id":        id,
		"status":    status,
		"stdout":    result.Stdout,
		"stderr":    result.Stderr,
		"exit_code": result.ExitCode,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, l.CoordinatorURL+"/internal/worker/result", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := l.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected status reporting result: %d", resp.StatusCode)
	}
	return nil
}

func (l *Loop) RunOnce(ctx context.Context) error {
	j, err := l.pollOnce(ctx)
	if err != nil {
		return fmt.Errorf("poll failed: %w", err)
	}
	if j == nil {
		return nil
	}

	slog.Info("job claimed", "id", j.ID, "image", j.Image)

	result, execErr := l.Execute(ctx, j.Image, j.Command, j.TimeoutSeconds)
	status := "succeeded"
	if execErr != nil || result.ExitCode != 0 {
		status = "failed"
	}
	if execErr != nil {
		result.Stderr = result.Stderr + "\n" + execErr.Error()
	}

	if err := l.reportResult(ctx, j.ID, status, result); err != nil {
		return fmt.Errorf("report failed: %w", err)
	}
	slog.Info("job reported", "id", j.ID, "status", status)
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/worker/... -v`
Expected: PASS (3 loop tests; the `integration`-tagged executor tests are skipped by default, which is correct here).

- [ ] **Step 5: Commit**

Commit message: `feat(worker): add poll-execute-report main loop`

---

### Task 1.6: Wire the two binaries + manual end-to-end verification

**Files:**
- Modify: `cmd/coordinator/main.go`
- Modify: `cmd/worker/main.go`

**Interfaces:**
- Consumes: `coordinator.NewServer`, `job.NewMemoryStore`, `worker.NewLoop`, `(*Loop).Run`.

- [ ] **Step 1: Wire the coordinator**

`cmd/coordinator/main.go`:
```go
package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/ritvikreddygangula/forge/internal/coordinator"
	"github.com/ritvikreddygangula/forge/internal/job"
)

func main() {
	addr := os.Getenv("COORDINATOR_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	store := job.NewMemoryStore()
	srv := coordinator.NewServer(store)

	slog.Info("coordinator starting", "addr", addr)
	if err := http.ListenAndServe(addr, srv); err != nil {
		slog.Error("coordinator exited", "error", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 2: Wire the worker**

`cmd/worker/main.go`:
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
	coordinatorURL := os.Getenv("COORDINATOR_URL")
	if coordinatorURL == "" {
		coordinatorURL = "http://localhost:8080"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	l := worker.NewLoop(coordinatorURL)
	slog.Info("worker starting", "coordinator", coordinatorURL)
	l.Run(ctx)
	slog.Info("worker stopped")
}
```

- [ ] **Step 3: Manual end-to-end verification**

In one terminal:
```bash
colima start   # if not already running
make run-coordinator
```

In a second terminal:
```bash
make run-worker
```

In a third terminal:
```bash
curl -s -X POST localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"image":"alpine:3.19","command":["echo","hello from forge"],"timeout_seconds":30}'
# copy the "id" from the response, then:
curl -s localhost:8080/jobs/<id>
# expected within ~2s (worker poll interval): "status":"succeeded", "stdout":"hello from forge\n"
```

Expected: the job transitions `queued` → `running` → `succeeded` and stdout shows the echoed string. This is the first true end-to-end proof the system works — record it (a terminal transcript or short screen recording) for later use in the README/resume story.

- [ ] **Step 4: Commit**

Commit message: `feat: wire coordinator and worker binaries for end-to-end job execution`

---

### Task 1.7: Logs endpoint

**Files:**
- Modify: `internal/coordinator/server.go`
- Test: `internal/coordinator/server_test.go`

**Interfaces:**
- Consumes: `job.Store.Get` from Task 1.1.
- Produces: `GET /jobs/{id}/logs` → `text/plain` body with stdout/stderr sections.

- [ ] **Step 1: Write the failing test**

Append to `internal/coordinator/server_test.go`:
```go
func TestHandleGetJobLogs(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	store.ClaimNext()
	store.Complete(created.ID, job.StatusSucceeded, "hello\n", "", 0)
	srv := coordinator.NewServer(store)

	req := httptest.NewRequest(http.MethodGet, "/jobs/"+created.ID+"/logs", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte("hello")) {
		t.Fatalf("expected logs to contain job stdout, got %q", rec.Body.String())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/coordinator/... -run TestHandleGetJobLogs`
Expected: FAIL — 404, route doesn't exist.

- [ ] **Step 3: Add the route and handler**

Modify `routes()` in `internal/coordinator/server.go`:
```go
func (s *Server) routes() {
	s.mux.HandleFunc("POST /jobs", s.handleSubmitJob)
	s.mux.HandleFunc("GET /jobs/{id}", s.handleGetJob)
	s.mux.HandleFunc("GET /jobs/{id}/logs", s.handleGetJobLogs)
	s.mux.HandleFunc("GET /internal/worker/poll", s.handleWorkerPoll)
	s.mux.HandleFunc("POST /internal/worker/result", s.handleWorkerResult)
}
```

Add to `internal/coordinator/server.go` (needs `"fmt"` added to imports):
```go
func (s *Server) handleGetJobLogs(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, err := s.store.Get(id)
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "--- stdout ---\n%s\n--- stderr ---\n%s\n", j.Stdout, j.Stderr)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/coordinator/... -v`
Expected: PASS, all 9 tests.

- [ ] **Step 5: Commit**

Commit message: `feat(coordinator): add GET /jobs/{id}/logs endpoint`

---

### Task 1.8: Documentation + close out Part 1

**Files:**
- Modify: `README.md`
- Modify: `PROGRESS.md`

- [ ] **Step 1: Update README.md's "Running locally" section**

Replace the placeholder paragraph with real instructions covering `colima start`, `make run-coordinator`, `make run-worker`, and the `curl` example from Task 1.6 Step 3.

- [ ] **Step 2: Update PROGRESS.md**

Append a new dated entry summarizing: Part 0 and Part 1 complete, end-to-end HTTP job execution working locally, next up is Part 2 (gRPC) per the roadmap.

- [ ] **Step 3: Commit**

Commit message: `docs: document local run instructions and update PROGRESS.md for Part 1`

**Part 1 complete.** Full test suite (`make test`) and, with Colima running, the integration suite (`make test-integration`) should both pass. Hand off to Ritvik to open the PR for `part-1-http-skeleton` → `main`.

**PR title:** `Part 1: plain HTTP coordinator + worker job execution`
**PR description points:** in-memory job store with queued/running/succeeded/failed states; REST submit/status/logs endpoints; HTTP poll-based worker executing jobs via `docker run`; unit tests for store/handlers/loop, tagged integration tests for the real-Docker executor path; manual end-to-end verification recorded per Task 1.6.

---

## Self-review

- **Spec coverage:** "What a job is, concretely" (image/command/timeout, inputs deferred) ✓; code-execution job type demoed via the `curl` example ✓; coordinator/worker core architecture ✓; REST interface subset for Part 1 (`POST /jobs`, `GET /jobs/{id}`, `GET /jobs/{id}/logs`) ✓; Docker via Colima locally ✓; git workflow (no Claude git commands, commit messages only, branch-per-Part) ✓; PROGRESS.md discipline ✓. gRPC, Kafka, Raft, multi-worker scheduling, MCP, cloud are explicitly out of scope here — covered by later Parts per the roadmap.
- **Placeholder scan:** every step has real, complete code or an exact shell command; no "TBD"/"add error handling" placeholders.
- **Type/interface consistency:** `job.Store` (Task 1.1) is consumed identically by `coordinator.NewServer` (Task 1.2) and `worker_handlers.go` (Task 1.3); `ExecResult`/`RunJob` (Task 1.4) match the `Execute` field signature used in `loop.go` (Task 1.5); `toJobResponse`/`jobResponse` (Task 1.2) is reused by the logs handler (Task 1.7) via `s.store.Get`, not redefined.
