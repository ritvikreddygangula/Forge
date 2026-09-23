# Progress Log

Append-only, cross-session continuity log. One section per branch (or per docs-only batch of
work on `main`), one line per commit, in commit order. Detail lives in the commit message itself —
this is just enough to scan and know what landed and why, without reading every diff.

Update this at the end of every branch, and at the end of any session even if the branch isn't
done yet.

---

## Repo init → `main`
- `first commit`, `readme & .gitignore` — empty repo, license/readme placeholders.

## Part 0 — repo scaffold → `main`
- `docs: add project roadmap and Part 0-1 implementation plan` — wrote the 9-part roadmap and the detailed Part 0+1 task breakdown before touching code.
- `chore: scaffold Go module and repo layout` — buildable skeleton, two placeholder binaries (`cmd/coordinator`, `cmd/worker`).
- `chore: add Makefile with build/test/lint targets` — `make build/test/lint/run-coordinator/run-worker`.
- `ci: add GitHub Actions workflow for build, vet, lint, test` — CI runs on every push/PR from here on.
- `docs: add PROGRESS.md for cross-session continuity` — this file, first version.

## Part 1 — plain HTTP skeleton → `part-1-http-skeleton` (merged via PR #1)
- `feat(job): add in-memory job store with state transitions` — `job.Store`: queued → running → succeeded/failed.
- `feat(coordinator): add POST /jobs and GET /jobs/{id} handlers` — external submit + status REST endpoints.
- `fix(coordinator): return 500 (not 404) for non-not-found store errors in GET /jobs/{id}` — status-code correctness fix caught in review.
- `feat(coordinator): add worker poll and result-reporting endpoints` — internal `GET /internal/worker/poll`, `POST /internal/worker/result`.
- `feat(worker): add Docker-based job executor` — runs a job via `docker run` through `os/exec`, with timeout handling.
- `feat(worker): add poll-execute-report main loop` — ties poll → execute → report into one loop, injectable `Execute` for unit testing.
- `feat: wire coordinator and worker binaries for end-to-end job execution` — `cmd/coordinator` and `cmd/worker` now actually run the system.
- `feat(coordinator): add GET /jobs/{id}/logs endpoint` — stdout/stderr retrieval.
- `fix(coordinator): return 500 (not 404) for non-not-found store errors in GET /jobs/{id}/logs` — same class of fix as above, applied to the logs endpoint.
- `docs: document local run instructions and update PROGRESS.md for Part 1` — README `curl` walkthrough.
- `ci: lower go directive to 1.23, bump golangci-lint-action to v7, add -race` — CI hardening found during review.
- `fix: handle unchecked errors, reject flag-like image values, type worker payload` — lint/security cleanup (unchecked errors, `docker run` argument-injection guard).
- `test: check errors in tests, cover store-error branches, add concurrency test` — closed test gaps found in review, added a concurrent-access test for the in-memory store.
- `docs: fix README accuracy on logging output, timing, and env vars` — corrected README claims that drifted from actual behavior.
- `docs: restructure Parts 2-9 into a 6-branch delivery plan` — roadmap rewritten from one-branch-per-Part to the current 6-branch structure.
- `docs: bump Part 4 to 3 coordinators, add partition/load-test benchmarking, lock resume-honesty rule` — Raft plan corrected to a real 3-node quorum, resume-honesty rule added (see roadmap's Global Constraints).
- `docs: lock Part 5 load test to real Docker execution, no synthetic job type` — load-test benchmark decision recorded ahead of time.

**End-to-end verified:** submitted an `alpine:3.19` job via `curl`, worker executed it in a real Docker container via Colima, status and logs came back correct.

## Docs restructuring → `main`
- `docs: add high-level project plan for pre-read` — added `PLAN.md`.
- `docs: reorganize docs into docs/ and clarify MCP's role` — moved `distributed-orchestrator-spec.md` → `docs/spec.md` and the dated plan files → `docs/plans/roadmap.md` / `docs/plans/part-0-1-http-skeleton.md` for a flatter root; clarified MCP stays as a thin, optional Branch 6 add-on rather than a headline feature (already covered by DeltaLedger); moved the "done for resume" checkpoint to Branch 5 (core engine) instead of Branch 6.
- `docs: add CLAUDE.md and restructure PROGRESS.md format` — added `CLAUDE.md` so a fresh Claude Code session can pick this project back up without a human re-explaining state; switched this file to one-section-per-branch, one-line-per-commit going forward.

## Part 2 — gRPC transport → `part-2-grpc`
- `feat(api): define job.proto service (PollJob, ReportResult, StreamLogs)` — the gRPC contract mirroring the HTTP worker endpoints it replaces.
- `docs: add Part 2 gRPC implementation plan` — task-by-task TDD plan written before touching code.
- `build: wire buf codegen into Makefile, bump Go to 1.25 (grpc's minimum)` — `make proto` regenerates committed code; Go bumped from 1.23 since `google.golang.org/grpc` v1.84.0 requires it.
- `docs: note the reset-hard/uncommitted-changes lesson in CLAUDE.md` — process note, not project work.
- `feat(coordinator): implement gRPC server alongside REST` — `GRPCServer` implements `PollJob`/`ReportResult` over the same `job.Store` the REST handlers use; coordinator now runs both listeners (`:8080` REST, `:9090` gRPC).
- `feat(worker): replace HTTP polling client with gRPC client` — `worker.Loop` now dials gRPC instead of building HTTP requests; `Execute`'s signature untouched.
- `feat(coordinator): stream logs over gRPC for GET /jobs/{id}/logs parity` — `StreamLogs` sends the store's current stdout/stderr as chunks; not live tailing (executor still captures complete buffers, unchanged from Part 1).
- `test: add gRPC integration test covering submit to result` — automates the manual submit→poll→execute→report→get cycle against real REST + gRPC servers.
- `chore: remove worker-facing HTTP endpoints superseded by gRPC` — deleted `GET /internal/worker/poll` and `POST /internal/worker/result` once the gRPC path was proven.
- `docs: update PROGRESS.md for Part 2` — this entry, plus README updates for the two-listener startup and `COORDINATOR_GRPC_ADDR`.
- `fix: quote golangci-lint version as string, check ClaimNext errors in tests` — CI failure on the PR: `.golangci.yml`'s `version: 2` needed to be a string for golangci-lint v2.13.2's schema; fixing it surfaced 2 real unchecked-error findings in test code.

**End-to-end verified twice:** the automated integration test (fake executor), and a manual run of the
actual compiled binaries — submitted a job via REST, a real worker polled it over real gRPC on `:9090`,
executed it in a real `alpine:3.19` Docker container, and reported back correctly. Same result as
Part 1's HTTP version, confirming the transport swap didn't change behavior.

## Part 3 — Kafka/Redpanda event log → `part-3-event-log`
- `docs: add Part 3 event log implementation plan` — design verified against a real local Redpanda container before writing any code (topic-creation idempotency, offset semantics, empty-topic behavior).
- `chore: add Redpanda service to docker-compose` — `deploy/docker-compose.yml` (first use of Compose in this project), `make compose-up`/`compose-down`.
- `refactor(job): back the job store with the replay-rebuilt state` — `MemoryStore.Rebuild(jobs []*Job)`, restores FIFO queue order for still-queued jobs; moved ahead of the roadmap's listed order since later tasks depend on it.
- `feat(eventlog): publish job-state transitions to Kafka-API topic` — `eventlog.Event`/`Rebuild` (pure fold), `KafkaProducer`/`KafkaConsumer`/`EnsureTopic` (real `segmentio/kafka-go`), and `eventlog.Store` — a `job.Store`-implementing decorator that publishes an event after each mutation, zero changes to REST/gRPC handlers.
- `feat(coordinator): rebuild in-memory job state by replaying event log on startup` — `cmd/coordinator/main.go` now replays the full log into a fresh store before serving; new `REDPANDA_BROKERS` env var.
- `test: add crash-recovery test (kill coordinator mid-job, restart, assert state rebuilt)` — the headline proof, against a real broker, no mocked Kafka.
- `docs: update PROGRESS.md for Part 3` — this entry; README crash-recovery walkthrough.

**Three real bugs found and fixed by stress-testing, not just reasoning about the code:**
- `kafka.Reader`'s default `MaxWait` (10s) meant every replay silently took ~10s even with data already
  present — set to 250ms.
- Hardcoding the topic name (`"job-events"`) meant all tests shared one topic; a queued-but-never-claimed
  job left behind by one test polluted another test's FIFO assertions. Fixed by parameterizing the topic
  everywhere; production still defaults to `eventlog.DefaultTopic`.
- `kafka.Writer`'s topic-metadata cache (`Transport.MetadataTTL`, default 15s) made retrying a failed
  publish to a freshly-created topic pointless — the retry kept hitting the same stale cached answer.
  Set to 250ms. Found only by running the integration suite 8-12x in a row; a single passing run gave no
  signal either bug existed.

**Crash recovery verified twice:** the automated test above, and manually with the real compiled
coordinator binary — submitted and completed a real job, killed the process outright, started a brand
new one against the same Redpanda broker, got `jobs_restored=1` and the exact same job status/stdout
back with zero in-process continuity between the two coordinator instances.

## Part 4 — Three-coordinator Raft cluster → `part-4-raft`
- `docs: add Part 4 raft implementation plan` — design verified against a real `hashicorp/raft` (v1.8.0) 3-node in-memory cluster during planning: measured 77-132ms re-election with tuned 50ms timeouts vs. 1.1-2.4s on library defaults.
- `feat(job,eventlog): add incremental apply methods for continuous replication` — `MemoryStore.ApplyCreated/ApplyClaimed/ApplyCompleted` + `eventlog.ApplyEvent`, the incremental counterparts to Part 3's bulk `Rebuild`, needed so every replica can keep applying new events as they arrive, not just once at startup.
- `feat(coordinator): embed hashicorp/raft with single-node bootstrap` — `NewRaftNode` with a deliberate no-op FSM (raft here is leader election only, not data replication — job state stays durable via the Redpanda log every replica tails); this commit's test already covers the roadmap's separate "failover test" item, logging real measured re-election times.
- `feat(eventlog): add continuous tail for multi-replica replication` — `ReadAll` now also returns an offset; new `Tail(ctx, fromOffset, onEvent)` runs indefinitely so every replica stays in sync with whichever replica is currently leader.
- `feat(coordinator): add three-replica config and TCP raft transport` — `deploy/raft-cluster.json`, real TCP raft transport, `cmd/coordinator/main.go` wired for cluster mode (opt-in via `COORDINATOR_REPLICA_ID`; unset, single-instance mode from Parts 1-3 is untouched).
- `feat(coordinator): gate job-assignment writes behind leader check, forward writes to leader` — `RaftGate`; REST/gRPC write handlers forward to whoever raft says is leader, reads stay ungated (eventually consistent across replicas, documented not hidden).
- `feat(coordinator): persist raft log/snapshot to a volume` — `raft-boltdb/v2` for log/stable storage, `raft.NewFileSnapshotStore` for snapshots, one `data/<replica-id>/raft/` directory per replica. Go bumped 1.25→1.26 (raft-boltdb's genuine minimum).
- `test: add network-partition fault injection (isolate leader from followers, assert majority partition elects a new leader and the minority side does not)` — 10/10 repeated runs, 65-132ms re-election.
- `test: add failover benchmark harness — run N repeated leader-kill trials, record failover latency per trial, report median/p99` — **real measured result: 30 trials, median 106ms, p99 136ms, min 59ms, max 136ms, 30/30 under the 500ms target.** Re-run for sanity: 108ms/161ms that run — consistent, both comfortably under target.
- `docs: update PROGRESS.md for Part 4 with real benchmark results` — this entry; README's "Running a 3-replica cluster" section.

**A three-day-old zombie process caused most of the debugging pain in this Part, not a code bug.** While
verifying the leader-forwarding logic (Task 4.3), jobs submitted through a follower sometimes silently
vanished — forwarded, seemingly accepted, but absent from the leader's store moments later. Chased it
through several real (and ultimately unrelated) fixes — `Tail` swallowing transient errors silently,
`kafka.Writer`'s connection pooling getting stuck, `kafka.Reader`'s `MaxWait`/`Transport.MetadataTTL`
defaults, a genuine port collision in `deploy/raft-cluster.json` (node3's gRPC port originally clashed
with Redpanda's own 9092) — all real, all fixed, none of them the actual cause. The actual cause: a
`go build` temp binary from the very first "Part 0-1" session on **September 19th**, three days earlier,
still running the ancient pre-gRPC coordinator, bound to port 8080 via an IPv6 wildcard that coexisted
with the real test processes' IPv4-specific bindings — so requests randomly landed on a three-day-old
zombie with a completely disconnected in-memory store. Found via `lsof -i` per port, killed both it and
a matching zombie worker process, and every remaining "bug" evaporated. Lesson: when local manual testing
gets inexplicably inconsistent, check `lsof -i` for surprise listeners before trusting the code is wrong.

**Leader failover and forwarding verified for real, repeatedly:** a 10-trial stress test killing and
restarting all 3 real coordinator processes, always submitting through whichever replica was a follower
that trial — 10/10 clean, with leadership genuinely rotating across all 3 nodes. Also verified raft state
survives a full restart of all 3 processes (no re-bootstrap, `ErrCantBootstrap` correctly recognized as
"already initialized" rather than an error).

## Part 5 — Multi-worker scheduling with heartbeat failure detection → `part-5-scheduling`
- `docs: add Part 5 scheduling implementation plan` — worker identity, `WorkerRegistry`, `Reaper`, a real multi-worker scheduling/failure test, and a real load test, planned before code. Key design call: "least-loaded" scheduling falls out for free from the existing pull-model (a worker only polls when idle, since a poll blocks on execution) — no scheduling algorithm needed this Part.
- `feat: add worker identity to poll requests` — `PollJobRequest.worker_id`, `Job.WorkerID`, `Store.ClaimNext(workerID)`; added as real groundwork not in the original roadmap list, same precedent as Part 4's Task 4.1.
- `feat: track worker load via heartbeats` — `coordinator.WorkerRegistry` records last-heartbeat-per-worker; `DeadWorkers` never flags a worker it has no history for, protecting a freshly-elected raft leader (empty registry) from mass false-positive reassignment.
- `test: add Rebuild/ApplyEvent coverage for job requeuing` — `EventJobRequeued`, `job.Store.RequeueRunning`; turned out mostly already implemented as a side effect of the worker-identity commit, just needed direct test coverage.
- `feat: add heartbeat-timeout failure detection and reassignment` — `coordinator.Reaper`, leader-gated, wired into `cmd/coordinator/main.go` on a 2s tick with a 6s dead-worker timeout (3× the poll interval). **A real end-to-end smoke test (real compiled binaries, real Docker) caught a genuine bug before this shipped:** a job running longer than the dead-worker timeout made its own live, busy worker look dead and get falsely reaped mid-execution — because `Loop.RunOnce` blocks on `Execute` for the whole job, so a worker sends no heartbeat while a job is running. Fixed in the same commit by adding a real `Heartbeat` gRPC RPC that `Loop` calls on its own ticker from a goroutine alongside `Execute`, independent of polling. Re-verified with the same manual smoke test afterward: a `sleep 20` job stays alive past the old false-reap window with zero reap events logged, and killing a real worker mid-job still correctly triggers reassignment ~7s later.
- `test: add multi-worker scheduling and failure-reassignment test` — two real `worker.Loop`s against one real coordinator (fake `Execute`, no Docker needed — this test is about scheduling, not execution): one worker claims a job and never reports back, the reaper reassigns it, a survivor claims and completes it. Added `Loop.PollOnce` as a small test seam (claim without execute/report) to simulate a crashed worker without a real hang.
- `test: add load-test harness — 25 real workers, real docker run execution, measure sustained jobs/sec` — real coordinator (REST+gRPC, real Redpanda), 25 real `worker.Loop`s, real `docker run alpine:3.19 true` per job, 500 real jobs submitted up front. **No synthetic no-op job type used to inflate the number.**

**Real measured load-test results (two runs, no invented numbers):**
- Run 1: 500/500 jobs completed by 25 workers in 33.24s — **15.0 jobs/sec**
- Run 2: 500/500 jobs completed by 25 workers in 43.46s — **11.5 jobs/sec**

Both runs: 500/500 succeeded, zero failures, zero timeouts. The run-to-run spread is real `docker run`
container-startup variance inside the Colima VM, not noise or a bug — and it lands exactly where the plan
doc predicted before running it: low tens of jobs/sec given per-container Docker overhead and strictly
sequential execution per worker (no per-worker concurrency added this Part — a deliberate, separate scope
decision, not implied by "multiple workers").

**A manual smoke test, not a unit test, found this Part's one real bug.** All four safety-critical unit
tests (heartbeat recording, leader-gating, dead-worker detection, requeue-on-reap) passed from the start
and were individually verified non-vacuous by deliberately breaking then restoring the implementation —
standard practice all session. None of them caught the false-reap-of-a-live-worker bug, because no unit
test exercised a job whose execution time actually exceeded the reap timeout; every unit test used an
instantaneous fake `Execute`. Only running the real compiled coordinator and worker binaries against a
real `sleep 30` Docker container, and watching wall-clock behavior, surfaced it. Lesson carried forward:
timeout/liveness logic needs at least one test (even a manual one) where the timed thing actually takes
longer than the timeout, not just a fake that returns instantly.

## Part 6 — REST API polish + thin MCP layer → `part-6-interfaces-observability`
- `docs: add Part 6 REST/MCP implementation plan` — bundled into the first code commit below (see that
  commit's file list); design decisions locked in before code: cancel only applies to queued jobs (no
  push channel exists to interrupt a running one — a stated scope decision, not a missing feature), and
  streaming logs means the same buffered-then-sent shape gRPC's `StreamLogs` already has, not live tailing.
- `add job cancellation for queued jobs` — `job.Status` gains `StatusCancelled`; `job.Store.Cancel(id)`
  succeeds only while a job is still `queued`, returns `ErrNotCancellable` otherwise. Same interface-forced
  coupling as Part 5's Task 5.1 hit again: adding `Cancel` to `job.Store` broke the build everywhere
  `eventlog.Store` and coordinator's `failingStore` test double implement that interface, so event-log
  replication (`EventJobCancelled`, `Rebuild`/`ApplyEvent` cases, `eventlog.Store.Cancel`) had to land in
  the same commit rather than a separate one.
- `feat: add job cancel and streaming logs endpoints` — `DELETE /jobs/{id}` (200/409/404), `GET
  /jobs/{id}/logs/stream` (real SSE framing, `event: stdout`/`event: stderr`). Verified against a real
  running coordinator, not just `httptest`: cancel's three outcomes confirmed by real `curl`, and the SSE
  endpoint confirmed with `curl -N` against a job a real worker actually executed.
- `feat: add thin MCP server for submit/status/logs/cancel` — new `internal/mcpserver` + `cmd/mcpserver`,
  built on `github.com/modelcontextprotocol/go-sdk` v1.8.0, serving `submit_job`/`get_job_status`/
  `stream_logs`/`cancel_job` over stdio as thin wrappers directly over the same `job.Store` REST uses — no
  new engine, no leader forwarding (single-instance only, matching MCP's "thin, secondary interface"
  framing). **Verified over real stdio, not just the SDK's in-memory test transport:** built the actual
  binary and drove it with a raw JSON-RPC handshake — `tools/list` correctly showed all 4 tools with
  auto-inferred JSON schemas, and a real `tools/call` for `submit_job` created a real queued job.
- `test: add e2e tests for REST and MCP cancel/logs` — one full submit-through-cancel walk per surface
  (`internal/coordinator/integration_test.go`, `internal/mcpserver/integration_test.go`), each proving a
  completed job can't be cancelled (409 / error result) and a queued one can.

No real distributed-systems concept in this Part — it's finishing the surface of the already-fault-tolerant
core Part 5 completed, per this project's own framing (`PLAN.md`'s Branch 6 section).

## Next
Branch 5 is complete — this is the resume-done checkpoint (see `CLAUDE.md`). Branch 6 (REST/MCP polish +
observability) is in progress — Part 6 (REST/MCP) is done; Part 8 (observability: Prometheus/Grafana,
structured JSON logging) and optionally Part 9 (CI dogfooding stretch) are next, each getting its own
detailed plan written just before it starts, per the roadmap's per-Part planning rule.
