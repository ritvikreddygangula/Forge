# Part 8 — Observability — Implementation Plan

**Branch:** `part-6-interfaces-observability` (same branch as Part 6 — no new branch; Branch 6 bundles
Parts 6+8+9 into one PR, per `docs/plans/roadmap.md`'s "Branch structure" section).

**Goal:** Prometheus scrapes coordinator/worker metrics, a Grafana dashboard shows job
throughput/latency/failure rate, and logs are structured JSON to stdout. No new distributed-systems
concept — same "finish the surface of an already-working system" framing as Part 6.

**Spec:** `docs/spec.md`. **Roadmap:** `docs/plans/roadmap.md` (Part 8 section). **Prior plan:**
`docs/plans/part-6-rest-mcp.md`.

## Design

### Metrics live in a new `internal/metrics` package, wrapped the same way `eventlog.Store` wraps `job.Store`

`internal/metrics.Store` is a decorator implementing `job.Store`, the same pattern `eventlog.Store` already
established (Part 3) and `MemoryStore` → `eventlog.Store` already chains. It wraps whichever store is
inside it (`eventlog.Store` wrapping `job.MemoryStore`, in `cmd/coordinator/main.go`'s real chain) and
records a Prometheus metric after each successful call — `jobs_created_total`, `jobs_completed_total{status}`
(the throughput + failure-rate story), `jobs_cancelled_total`, `jobs_reassigned_total` (Part 5's reaper),
and `job_duration_seconds` (a histogram measuring `Complete`'s call time minus the job's `CreatedAt` —
end-to-end queue+execution latency, the only duration this project actually has clean timestamps for).
Metrics register into `prometheus.DefaultRegisterer` via `promauto`, so `promhttp.Handler()` needs no
manual registry wiring.

**Only the leader's counters move, in cluster mode — stated plainly.** A follower's REST/gRPC write
handlers forward to the leader (`RaftGate`, Part 4) before ever touching their own local store, so a
follower's `metrics.Store` never observes a forwarded write. Only the currently-leading replica's job
counters increment for cluster-wide writes; every replica's counters are equally valid for reads it serves
locally. This is the same kind of eventual-consistency-across-replicas note already made for
`handleGetJob` — named here, not discovered by a user later wondering why 3 replicas report 3 different
job counts.

### `cmd/mcpserver` keeps its logs on stderr — stdout is the MCP wire protocol, not a log stream

The roadmap's Part 8 deliverable says "structured JSON logs to stdout." Applying that literally to
`cmd/mcpserver` would be a real bug, not a style choice: `mcp.StdioTransport` uses stdout for the actual
JSON-RPC protocol exchange with the client (confirmed in Part 6 by smoke-testing the compiled binary over
raw stdio) — any log line Claude Code or any other line written to stdout corrupts every message an MCP
client parses from that stream. So: `cmd/coordinator` and `cmd/worker` get JSON logs on stdout (matching
the deliverable, and how `docker compose logs` / `journald` typically expect container log collection to
work); `cmd/mcpserver` gets JSON logs on stderr, explicitly, with a comment explaining why — the SDK
already writes its own protocol traffic to stdout, ours must stay out of the way.

### Local Prometheus scrapes the host machine, not other compose containers — a real environment detail, not a redesign

`docker-compose.yml` today only runs Redpanda; the coordinator and worker run directly on the host via
`go run` (`make run-coordinator` / `make run-worker`), not as compose services. Adding Prometheus *inside*
compose means its scrape target for the coordinator/worker's `/metrics` endpoints is the **host machine**,
not another container on the compose network. `host.docker.internal` resolves correctly for this
out of the box under Docker Desktop and Colima (both in scope for this project — see `PLAN.md`'s tech
stack, no Docker daemon substitution needed) but is a real environment-specific detail, not something a
generic "just add prometheus.yml" snippet would get right unstated. `deploy/prometheus.yml` targets
`host.docker.internal:8080` (coordinator) and `host.docker.internal:9091` (worker, new metrics port —
see Task 8.1). Documented as a local-dev convenience, not a claim about how a real multi-host deployment's
service discovery would work.

### Grafana dashboard is provisioned as code, not clicked together and screenshotted

`deploy/grafana/provisioning/datasources/prometheus.yml` and
`deploy/grafana/provisioning/dashboards/dashboard.yml` (a provider pointing at a folder) plus the actual
dashboard JSON (`deploy/grafana/dashboards/forge.json`) auto-provision on container start — reproducible
from a fresh `make compose-up`, not a one-time manual clickthrough that only the person who built it can
reproduce. Three panels, each a direct read of a metric this Part actually adds: job throughput
(`rate(jobs_completed_total[1m])`), job latency (`histogram_quantile(0.95, ...job_duration_seconds...)`),
and failure rate (`rate(jobs_completed_total{status="failed"}[5m])`).

### Worker gets a `/metrics` HTTP endpoint it didn't have before — the smallest possible addition

`cmd/worker` currently runs no HTTP server at all (gRPC client only). Exposing worker-side metrics means
adding one — a plain `net/http.ListenAndServe` on a new `WORKER_METRICS_ADDR` (default `:9091`), serving
only `promhttp.Handler()` at `/metrics`, started in a goroutine alongside the existing poll loop. Nothing
else changes about the worker's shape.

## File structure (new/changed)

```
internal/
  metrics/
    store.go                          # NEW — metrics.Store decorator (job.Store), Prometheus metric vars
    store_test.go                      # NEW
  worker/
    loop.go                             # MODIFIED — records execution count/duration metrics in RunOnce
    loop_test.go                         # MODIFIED
  coordinator/
    server.go                             # MODIFIED — GET /metrics route
cmd/
  coordinator/
    main.go                                # MODIFIED — wraps store in metrics.NewStore; JSON logging to stdout
  worker/
    main.go                                 # MODIFIED — starts /metrics HTTP server; JSON logging to stdout
  mcpserver/
    main.go                                  # MODIFIED — JSON logging to stderr (NOT stdout — see Design)
deploy/
  docker-compose.yml                          # MODIFIED — add prometheus, grafana services
  prometheus.yml                               # NEW — scrape config (host.docker.internal targets)
  grafana/
    provisioning/datasources/prometheus.yml       # NEW
    provisioning/dashboards/dashboard.yml           # NEW
    dashboards/forge.json                            # NEW — throughput/latency/failure-rate panels
go.mod                                          # MODIFIED — add github.com/prometheus/client_golang
docs/plans/part-8-observability.md                # this file
```

---

### Task 8.1: Prometheus metrics endpoints — coordinator and worker

**Files:** `internal/metrics/store.go`, `internal/metrics/store_test.go`, `internal/coordinator/server.go`,
`internal/worker/loop.go`, `internal/worker/loop_test.go`, `cmd/coordinator/main.go`, `cmd/worker/main.go`.
Add `github.com/prometheus/client_golang` (`v1.24.1`) to `go.mod`.

- [x] **Step 1:** `go get github.com/prometheus/client_golang@v1.24.1`

- [x] **Step 2: Write the failing tests** — `internal/metrics/store_test.go`. Assertions use
  before/after **deltas**, not absolute values — these are process-global `promauto` metrics, so a test
  can't assume it's the only one that ever touched a given counter:

```go
package metrics_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ritvikreddygangula/forge/internal/job"
	"github.com/ritvikreddygangula/forge/internal/metrics"
)

func TestMetricsStore_Create_IncrementsJobsCreatedTotal(t *testing.T) {
	inner := job.NewMemoryStore()
	s := metrics.NewStore(inner)
	before := testutil.ToFloat64(metrics.JobsCreatedTotal)

	if _, err := s.Create("alpine", []string{"true"}, 10); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	if after := testutil.ToFloat64(metrics.JobsCreatedTotal); after != before+1 {
		t.Fatalf("expected jobs_created_total to increment by 1, got %v -> %v", before, after)
	}
}

func TestMetricsStore_Complete_IncrementsJobsCompletedTotalByStatus(t *testing.T) {
	inner := job.NewMemoryStore()
	s := metrics.NewStore(inner)
	created, _ := s.Create("alpine", []string{"true"}, 10)
	if _, err := inner.ClaimNext("worker-1"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	before := testutil.ToFloat64(metrics.JobsCompletedTotal.WithLabelValues("succeeded"))

	if err := s.Complete(created.ID, job.StatusSucceeded, "ok\n", "", 0); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}

	if after := testutil.ToFloat64(metrics.JobsCompletedTotal.WithLabelValues("succeeded")); after != before+1 {
		t.Fatalf("expected jobs_completed_total{status=succeeded} to increment by 1, got %v -> %v", before, after)
	}
}

func TestMetricsStore_Cancel_IncrementsJobsCancelledTotal(t *testing.T) {
	inner := job.NewMemoryStore()
	s := metrics.NewStore(inner)
	created, _ := s.Create("alpine", []string{"true"}, 10)
	before := testutil.ToFloat64(metrics.JobsCancelledTotal)

	if _, err := s.Cancel(created.ID); err != nil {
		t.Fatalf("Cancel returned error: %v", err)
	}

	if after := testutil.ToFloat64(metrics.JobsCancelledTotal); after != before+1 {
		t.Fatalf("expected jobs_cancelled_total to increment by 1, got %v -> %v", before, after)
	}
}

func TestMetricsStore_Cancel_RejectedCancelDoesNotIncrement(t *testing.T) {
	inner := job.NewMemoryStore()
	s := metrics.NewStore(inner)
	created, _ := s.Create("alpine", []string{"true"}, 10)
	if _, err := inner.ClaimNext("worker-1"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	before := testutil.ToFloat64(metrics.JobsCancelledTotal)

	if _, err := s.Cancel(created.ID); err == nil {
		t.Fatal("expected an error cancelling a running job")
	}

	if after := testutil.ToFloat64(metrics.JobsCancelledTotal); after != before {
		t.Fatalf("expected no increment for a rejected cancel, got %v -> %v", before, after)
	}
}

func TestMetricsStore_RequeueRunning_IncrementsJobsReassignedTotal(t *testing.T) {
	inner := job.NewMemoryStore()
	s := metrics.NewStore(inner)
	if _, err := inner.Create("alpine", []string{"true"}, 10); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if _, err := inner.ClaimNext("dead-worker"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	before := testutil.ToFloat64(metrics.JobsReassignedTotal)

	if _, err := s.RequeueRunning("dead-worker"); err != nil {
		t.Fatalf("RequeueRunning returned error: %v", err)
	}

	if after := testutil.ToFloat64(metrics.JobsReassignedTotal); after != before+1 {
		t.Fatalf("expected jobs_reassigned_total to increment by 1, got %v -> %v", before, after)
	}
}

// histogramSampleCount reads the total number of observations a histogram
// has recorded so far. testutil.CollectAndCount counts metric *series*
// (always 1 for a histogram, regardless of how many Observe calls happened),
// not observations — found this the hard way when the first version of this
// test passed vacuously (1 -> 1 "matched" its own wrong expectation).
func histogramSampleCount(t *testing.T) uint64 {
	t.Helper()
	var m dto.Metric
	if err := metrics.JobDurationSeconds.Write(&m); err != nil {
		t.Fatalf("failed to write histogram metric: %v", err)
	}
	return m.GetHistogram().GetSampleCount()
}

func TestMetricsStore_Complete_ObservesJobDuration(t *testing.T) {
	inner := job.NewMemoryStore()
	s := metrics.NewStore(inner)
	created, _ := s.Create("alpine", []string{"true"}, 10)
	if _, err := inner.ClaimNext("worker-1"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	beforeCount := histogramSampleCount(t)

	if err := s.Complete(created.ID, job.StatusSucceeded, "ok\n", "", 0); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}

	if after := histogramSampleCount(t); after != beforeCount+1 {
		t.Fatalf("expected one new job_duration_seconds observation, got %d -> %d", beforeCount, after)
	}
}
```
  (needs `dto "github.com/prometheus/client_model/go"` added to the test file's imports.)

- [x] **Step 3: Run to verify failure, then implement** — `internal/metrics/store.go`:

```go
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ritvikreddygangula/forge/internal/job"
)

var (
	JobsCreatedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_created_total", Help: "Total number of jobs submitted.",
	})
	JobsCompletedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jobs_completed_total", Help: "Total number of jobs completed, by final status.",
	}, []string{"status"})
	JobsCancelledTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_cancelled_total", Help: "Total number of jobs cancelled while queued.",
	})
	JobsReassignedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_reassigned_total", Help: "Total number of jobs reassigned away from a dead worker.",
	})
	JobDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "job_duration_seconds", Help: "End-to-end job duration from creation to completion.",
		Buckets: prometheus.DefBuckets,
	})
)

// Store wraps a job.Store and records Prometheus metrics after each
// successful mutation — the same decorator shape eventlog.Store already
// established for publishing events. See docs/plans/part-8-observability.md
// for why only the raft leader's counters move in cluster mode.
type Store struct {
	inner job.Store
}

func NewStore(inner job.Store) *Store { return &Store{inner: inner} }

func (s *Store) Create(image string, command []string, timeoutSeconds int) (*job.Job, error) {
	j, err := s.inner.Create(image, command, timeoutSeconds)
	if err != nil {
		return nil, err
	}
	JobsCreatedTotal.Inc()
	return j, nil
}

func (s *Store) Get(id string) (*job.Job, error) { return s.inner.Get(id) }

func (s *Store) ClaimNext(workerID string) (*job.Job, error) { return s.inner.ClaimNext(workerID) }

func (s *Store) Complete(id string, status job.Status, stdout, stderr string, exitCode int) error {
	before, getErr := s.inner.Get(id)
	if err := s.inner.Complete(id, status, stdout, stderr, exitCode); err != nil {
		return err
	}
	JobsCompletedTotal.WithLabelValues(string(status)).Inc()
	if getErr == nil {
		JobDurationSeconds.Observe(time.Since(before.CreatedAt).Seconds())
	}
	return nil
}

func (s *Store) RequeueRunning(workerID string) ([]*job.Job, error) {
	requeued, err := s.inner.RequeueRunning(workerID)
	if err == nil {
		JobsReassignedTotal.Add(float64(len(requeued)))
	}
	return requeued, err
}

func (s *Store) Cancel(id string) (*job.Job, error) {
	j, err := s.inner.Cancel(id)
	if err != nil {
		return nil, err
	}
	JobsCancelledTotal.Inc()
	return j, nil
}
```

- [x] **Step 4: Coordinator `/metrics` route** — `internal/coordinator/server.go`:

```go
s.mux.Handle("GET /metrics", promhttp.Handler())
```
  (new imports: `"github.com/prometheus/client_golang/prometheus/promhttp"`. Not leader-gated — every
  replica serves its own local metrics.)

  `cmd/coordinator/main.go`: wrap the store before passing it to `NewGRPCServer`/`NewServer`:
```go
store := metrics.NewStore(eventlog.NewStore(baseStore, producer))
```

- [x] **Step 5: Worker metrics + `/metrics` server** — new metrics in `internal/metrics/store.go`:

```go
var (
	WorkerJobsExecutedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "worker_jobs_executed_total", Help: "Total number of jobs this worker has executed, by outcome.",
	}, []string{"status"})
	WorkerExecutionDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "worker_execution_duration_seconds", Help: "Real docker run execution duration.",
		Buckets: prometheus.DefBuckets,
	})
)
```
  `internal/worker/loop.go`'s `RunOnce`, around the existing `l.Execute(...)` call:
```go
execStart := time.Now()
result, execErr := l.Execute(ctx, j.Image, j.Command, int(j.TimeoutSeconds))
metrics.WorkerExecutionDurationSeconds.Observe(time.Since(execStart).Seconds())
```
  and after `status` is determined:
```go
metrics.WorkerJobsExecutedTotal.WithLabelValues(status).Inc()
```
  Test with the same before/after-delta pattern as Task 8.1 Step 2, using the existing `fakeJobServer`
  harness in `loop_test.go` with a slow fake `Execute` to prove the histogram actually observes something
  non-zero (verify-by-breaking: temporarily skip the `Observe` call, confirm the test's count assertion
  fails, restore).

  `cmd/worker/main.go`:
```go
metricsAddr := os.Getenv("WORKER_METRICS_ADDR")
if metricsAddr == "" {
	metricsAddr = ":9091"
}
go func() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe(metricsAddr, mux); err != nil {
		slog.Error("worker metrics server exited", "error", err)
	}
}()
```

- [x] **Step 6: Run full suite, verify, commit**

Run: `go build ./... && go test ./...`

Commit message: `feat: expose Prometheus metrics on coordinator and worker`

---

### Task 8.2: Prometheus + Grafana in docker-compose

**Files:** `deploy/docker-compose.yml`, `deploy/prometheus.yml` (new),
`deploy/grafana/provisioning/datasources/prometheus.yml` (new),
`deploy/grafana/provisioning/dashboards/dashboard.yml` (new), `deploy/grafana/dashboards/forge.json` (new).

- [x] **Step 1: `deploy/prometheus.yml`**

```yaml
global:
  scrape_interval: 5s
scrape_configs:
  - job_name: forge-coordinator
    static_configs:
      - targets: ["host.docker.internal:8080"]
  - job_name: forge-worker
    static_configs:
      - targets: ["host.docker.internal:9091"]
```

- [x] **Step 2: Add services to `deploy/docker-compose.yml`**

```yaml
  prometheus:
    image: prom/prometheus:latest
    container_name: forge-prometheus
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    extra_hosts:
      - "host.docker.internal:host-gateway"
    ports:
      - "9095:9090"

  grafana:
    image: grafana/grafana:latest
    container_name: forge-grafana
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning
      - ./grafana/dashboards:/var/lib/grafana/dashboards
    ports:
      - "3000:3000"
    environment:
      - GF_AUTH_ANONYMOUS_ENABLED=true
      - GF_AUTH_ANONYMOUS_ORG_ROLE=Admin
```
  (`extra_hosts` is what makes `host.docker.internal` resolve on plain Linux Docker too, not just
  Docker Desktop/Colima's built-in resolution — belt and suspenders. Grafana anonymous admin access is a
  deliberate local-dev-only simplification, stated here, not left for someone to discover is insecure.)

- [x] **Step 3: Grafana provisioning** — `deploy/grafana/provisioning/datasources/prometheus.yml`:

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    uid: prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```
  (`uid: prometheus` is pinned explicitly, not left to auto-generate — see the real bug this caught below.)

  `deploy/grafana/provisioning/dashboards/dashboard.yml`:

```yaml
apiVersion: 1
providers:
  - name: forge
    folder: ""
    type: file
    options:
      path: /var/lib/grafana/dashboards
```

  `deploy/grafana/dashboards/forge.json`: a 3-panel dashboard (throughput, p95 latency, failure rate) —
  exact panel JSON finalized during implementation against a real running Grafana instance rather than
  hand-authored blind, same posture as Part 4/5 flagged their own minor implementation-time seams.

- [x] **Step 4: ⚠️ Manual step — verify for real**

Run `make compose-up`, confirm `docker compose -f deploy/docker-compose.yml ps` shows `forge-prometheus`
and `forge-grafana` healthy, open `http://localhost:9095/targets` and confirm both scrape targets show
`UP` (requires the coordinator and a worker actually running via `make run-coordinator`/`make run-worker`
first), then open `http://localhost:3000` and confirm the Forge dashboard renders with real data after
submitting a few jobs.

**A real bug this verification caught, not a hypothetical:** the dashboard JSON's panels reference
`datasource.uid: "prometheus"`, but without an explicit `uid` set in the datasource provisioning YAML,
Grafana auto-generates one (e.g. `PBFA97CFB590B2093`) — every panel would have silently failed to resolve
its datasource. Caught by actually querying Prometheus through Grafana's own datasource proxy
(`/api/datasources/proxy/uid/prometheus/api/v1/query`) for all 3 panel expressions and confirming real
data came back, not by reading the JSON and assuming it was right. Fixed by pinning `uid: prometheus` in
the datasource file. (A stale auto-generated datasource in Grafana's internal DB then needed the container
recreated, not just restarted, to fully clear — `docker compose rm -sf grafana && docker compose up -d
grafana`, since this setup has no persistent volume for Grafana's own database, only for
provisioning/dashboard files.)

- [x] **Step 5: Commit**

Commit message: `chore: add Prometheus and Grafana to docker-compose`

---

### Task 8.3: Structured JSON logging

**Files:** `cmd/coordinator/main.go`, `cmd/worker/main.go`, `cmd/mcpserver/main.go`.

- [x] **Step 1:** `cmd/coordinator/main.go` and `cmd/worker/main.go`, first line of `main()`:

```go
slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
```

- [x] **Step 2:** `cmd/mcpserver/main.go` — **stderr, not stdout** (see this doc's Design section: stdout
  is the live MCP JSON-RPC channel):

```go
slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
```

- [x] **Step 3: ⚠️ Manual step — verify for real**

Run `make run-coordinator`, confirm each log line is now valid single-line JSON (e.g. pipe through
`jq .` and confirm it parses). Run `make run-mcpserver` with a real MCP client connected (or the raw
stdio smoke test from Part 6) and confirm stdout is still clean JSON-RPC only — no log lines mixed in.

- [x] **Step 4: Commit**

Commit message: `refactor: switch logging to structured JSON via log/slog`

---

### Task 8.4: Documentation

**Files:** `README.md`, `PROGRESS.md`, `CLAUDE.md`.

- [ ] **Step 1:** README: "Metrics and dashboards" section — `make compose-up` now also starts
  Prometheus (`:9095`) and Grafana (`:3000`, anonymous admin, local dev only), what the 3 dashboard panels
  show, and the worker's new `WORKER_METRICS_ADDR` env var.

- [ ] **Step 2:** `PROGRESS.md`: Part 8 commit log.

- [ ] **Step 3:** `CLAUDE.md`: update "Where things stand" — if Part 9 (CI dogfooding stretch) isn't
  being attempted, note that Branch 6 is complete and ready for its single combined PR; if Part 9 is
  attempted, this becomes an interim update instead.

- [ ] **Step 4: Commit**

Commit message: `docs: add Grafana dashboard notes, update PROGRESS.md for Part 8`

---

## Self-review

- **Spec/roadmap coverage:** all 4 roadmap commit messages for Part 8 present, one task per commit,
  matching commit-for-commit rather than splitting further (unlike Part 6, no interface-forced coupling
  is expected here since `metrics.Store` is purely additive and doesn't change the `job.Store` interface
  itself).
- **Placeholder scan:** one spot flagged for implementation-time finalization — the actual Grafana
  dashboard panel JSON (Task 8.2 Step 3), authored against a real running instance rather than guessed —
  same posture as Part 5/6's flagged minor wiring seams.
- **Known limitations named, not hidden:** only-the-leader's-counters-move in cluster mode, and
  `host.docker.internal`/`extra_hosts` being a local-dev networking detail rather than a real
  multi-host service-discovery story, are both stated in the Design section, not discovered later.
- **A real, load-bearing correctness constraint, not just a style choice:** `cmd/mcpserver` logging to
  stderr instead of stdout isn't a preference — logging to stdout would silently corrupt the MCP
  protocol stream Part 6 already proved works over real stdio. Named explicitly so it's never
  "simplified" back to matching the other two binaries by someone skimming the roadmap's literal wording.
- **No new distributed-systems concept introduced** — consistent with Branch 6's framing as polish over
  an already-fault-tolerant core (Part 5 remains the resume-done checkpoint).
