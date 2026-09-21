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

**End-to-end verified twice:** the automated integration test (fake executor), and a manual run of the
actual compiled binaries — submitted a job via REST, a real worker polled it over real gRPC on `:9090`,
executed it in a real `alpine:3.19` Docker container, and reported back correctly. Same result as
Part 1's HTTP version, confirming the transport swap didn't change behavior.

## Next
Branch 3 (`part-3-event-log`) — not started yet.
