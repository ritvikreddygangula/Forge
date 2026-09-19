# Distributed Job Orchestrator — Roadmap & Tech Stack Decisions

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> This is the **master roadmap** — it sequences all 9 Parts, locks in the tech stack, and defines the commit sequence per Part. Per the writing-plans scope check, each Part is a separate subsystem with its own working, testable deliverable, so **each Part gets its own detailed bite-sized plan** written just before that Part starts (requirements for Part N+1 sharpen once Part N is actually built). The detailed plan for Part 0 + Part 1 already exists: `docs/superpowers/plans/2026-09-19-distributed-orchestrator-part0-1.md`.

**Goal:** Build a crash-safe, distributed job orchestrator (REST + MCP, gRPC internals, Kafka-backed event log, Raft leader election) as a portfolio/learning project, following the spec's strict one-new-concept-at-a-time learning sequence.

**Architecture:** Go coordinator + Go worker agents, gRPC between them, Kafka/Redpanda as the durable event log the coordinator replays to rebuild state, hashicorp/raft embedded in the coordinator binary for leader election across replicas, Postgres for queryable job/worker metadata, Docker (via Colima locally) as the job sandbox, REST + MCP as the two external interfaces over the same core engine.

**Spec:** `distributed-orchestrator-spec.md` (repo root) — this plan argues from that spec; read both.

## Global Constraints

- **Git workflow:** Claude never runs git commands. Output one short imperative-mood commit message per sub-step, and a PR title/description when a Part completes. Ritvik performs all git operations (`git add`, `git commit`, `git push`, PR creation).
- **Branching:** Part 0 → straight to `main`. Every other Part → its own branch `part-N-short-description` cut from latest `main`, merged via PR when the Part's deliverable works end-to-end.
- **Sequencing discipline:** Do not skip ahead. Each Part must be fully working before the next starts — this is the spec's core teaching mechanism (never debug more than one new concept at once).
- **PROGRESS.md:** Update at the end of every session, even short ones, at the repo root.
- **Resume discipline:** Don't add partial/unfinished bullets to the resume. This project replaces "Deep Research Multi-Agent Systems" only once fully done (Part 6, or Part 9 if the stretch goal is pursued).
- **Don't drift toward "another AI pipeline."** This project's whole resume value is being a systems project (concurrency, fault tolerance, Go) that contrasts with the Filing Agent (agentic AI, MCP, evals). Keep the MCP server thin — a delegation interface, not the point of the project.
- **Part 7 (cloud/Terraform/k3s) is on hold.** Do not provision AWS resources or write Terraform against a real account until that decision is explicitly revisited after Part 6.

---

## Tech stack — recommendation

The spec already locks in most of this; where it left something open (`Kafka (or Redpanda)`) or unspecified, here's the call and why.

| Layer | Choice | Why |
|---|---|---|
| Language | Go 1.23+ | Spec requirement. Also genuinely the right tool for this — goroutines/channels map directly onto "coordinator juggling many workers," and it's the resume-differentiator from the Filing Agent's Python/AI stack. |
| Coordinator/worker transport | gRPC (`google.golang.org/grpc` + `protobuf`) | Spec requirement (Part 2). Use **buf** (not raw `protoc`) for schema linting/codegen — it's the current standard, one config file, no local protoc-plugin PATH fights. |
| Event log | **Redpanda**, not real Kafka | Kafka-API compatible (your code uses `segmentio/kafka-go` or `confluent-kafka-go` exactly as it would against real Kafka — the resume bullet "Kafka-backed event log" stays accurate since it's the protocol, not the binary, that matters), but it's a single static binary with no ZooKeeper/JVM. Docker Compose stays a 1-service add instead of a 3-service (ZK + broker + tooling) headache. If you ever want to demo against literal Apache Kafka, swapping the compose service is a non-event because you coded to the Kafka wire protocol either way. |
| Leader election | `hashicorp/raft` embedded in the coordinator binary | Spec requirement (Part 4). No separate etcd service. |
| Metadata store | Postgres (`jackc/pgx/v5` driver) + `golang-migrate` for schema migrations | pgx is the modern idiomatic Postgres driver for Go (faster, better type support than `lib/pq`). golang-migrate keeps migrations as plain versioned `.sql` files — simple, no ORM magic to explain in an interview. |
| Job sandbox | Docker via Colima locally; CLI (`os/exec` + `docker run`) in Part 1, optionally the Docker Engine SDK (`docker/docker/client`) later if you want the extra depth | Spec requirement. Starting with the CLI in Part 1 keeps Part 1 about the HTTP job lifecycle, not Docker SDK intricacies — consistent with the spec's "one new concept at a time" rule. Swapping to the SDK is a clean, optional Part-1.5 refactor once the lifecycle is solid. |
| REST framework | Stdlib `net/http` with Go 1.22+'s pattern-based `ServeMux` (`"GET /jobs/{id}"`) | No router dependency needed for a handful of routes; gRPC (Part 2 onward) carries the real internal traffic anyway, so REST stays a thin, boring layer — deliberately, since over-engineering the REST layer isn't where the learning value is. |
| MCP server | `github.com/modelcontextprotocol/go-sdk` (confirm latest state at Part 6 implementation time) | Official Go SDK for MCP; keeps the MCP layer a thin wrapper delegating into the same core engine as REST, per the spec's "same actions, exposed" framing. |
| Structured logging | Stdlib `log/slog` | Zero extra dependency, JSON-structured output feeds directly into Part 8 observability without a rewrite. |
| Metrics | `prometheus/client_golang` + Prometheus + Grafana (Part 8) | Spec requirement. |
| Testing | Stdlib `testing` + `testify/assert`/`require` for readability; `testcontainers-go` for integration tests that spin up real Redpanda/Postgres/Docker | Keeps unit tests dependency-light; integration tests are honest (real infra, not mocks) which matters a lot for a fault-tolerance-focused project — a mocked Kafka would undercut the whole "crash-safe" story. |
| Lint/CI | `golangci-lint` + GitHub Actions (`go build`, `go vet`, `golangci-lint run`, `go test ./...`) | Standard, cheap, and Part 9's CI-dogfooding story lands better if this project has been dogfooding CI on itself since Part 0. |
| Local orchestration | Docker Compose (`coordinator`, `worker`, `redpanda`, `postgres`, and later `prometheus`/`grafana`) | Spec requirement — must fully cover Parts 1–6 with no cloud dependency. |
| Cloud (deferred) | Self-managed k3s on EC2 + Terraform, only if greenlit after Part 6 | Per spec — not decided yet, don't provision anything now. |

**Module path:** `github.com/ritvikreddygangula/forge` (adjust in Part 0 if the actual GitHub remote differs).

## Repo layout (locked in during Part 0, referenced by every later Part)

```
forge/
  cmd/
    coordinator/       main.go — coordinator binary entrypoint
    worker/             main.go — worker binary entrypoint
  internal/
    job/                 job domain model + store interface + in-memory impl (Part 1), Postgres impl (Part 3+)
    coordinator/         HTTP/gRPC handlers, scheduling, raft integration
    worker/               poll/execute/report loop, Docker executor
    eventlog/            Kafka/Redpanda producer + consumer, state-rebuild-on-replay logic (Part 3)
    mcpserver/            MCP tool handlers (Part 6)
  api/
    proto/                job.proto, generated gRPC/protobuf code (Part 2)
  deploy/
    docker-compose.yml    full local stack
    k3s/                   manifests (Part 7, on hold)
  docs/
    superpowers/plans/    plan documents (this file and successors)
  PROGRESS.md
  README.md
  distributed-orchestrator-spec.md
```

---

## Part-by-part plan

Each Part below lists: goal, deliverable (what "done" looks like end-to-end), and the sequential commit messages Claude will output as it works through that Part's detailed plan. Detailed bite-sized (TDD) plans are written **one Part at a time**, just before that Part starts — see the note at the top of this file for why.

### Part 0 — Repo scaffold → commits straight to `main`
**Deliverable:** `go build ./...` and `go test ./...` succeed on an empty-but-structured repo; CI runs on every push.
**Detailed plan:** `docs/superpowers/plans/2026-09-19-distributed-orchestrator-part0-1.md` (combined with Part 1)
1. `chore: scaffold Go module and repo layout`
2. `chore: add Makefile with build/test/lint targets`
3. `ci: add GitHub Actions workflow for build, vet, lint, test`
4. `docs: add PROGRESS.md for cross-session continuity`

### Part 1 — Plain HTTP skeleton → branch `part-1-http-skeleton`
**Deliverable:** Submit a job via `POST /jobs`, one worker polls over HTTP, runs it in a Docker container via Colima, reports result back; `GET /jobs/{id}` and `GET /jobs/{id}/logs` show the outcome.
**Detailed plan:** `docs/superpowers/plans/2026-09-19-distributed-orchestrator-part0-1.md`
1. `feat(job): add in-memory job store with state transitions`
2. `feat(coordinator): add POST /jobs and GET /jobs/{id} handlers`
3. `feat(coordinator): add worker poll and result-reporting endpoints`
4. `feat(worker): add Docker-based job executor`
5. `feat(worker): add poll-execute-report main loop`
6. `feat(coordinator): add GET /jobs/{id}/logs endpoint`
7. `docs: document local run instructions for Part 1`
8. `docs: update PROGRESS.md at end of Part 1`

### Part 2 — Swap HTTP for gRPC → branch `part-2-grpc`
**Deliverable:** Same worker-facing behavior as Part 1 (poll, execute, report), now over gRPC; REST stays for the external submit/status surface.
1. `feat(api): define job.proto service (PollJob, ReportResult, StreamLogs)`
2. `build: wire buf codegen into Makefile`
3. `feat(coordinator): implement gRPC server alongside REST`
4. `feat(worker): replace HTTP polling client with gRPC client`
5. `feat(coordinator): stream logs over gRPC for GET /jobs/{id}/logs parity`
6. `test: add gRPC integration test covering submit to result`
7. `chore: remove worker-facing HTTP endpoints superseded by gRPC`
8. `docs: update PROGRESS.md for Part 2`

### Part 3 — Kafka/Redpanda event log → branch `part-3-event-log`
**Deliverable:** Job-state transitions are published to Redpanda; killing and restarting the coordinator rebuilds full in-memory state by replaying the log — verified by an automated test, not just manual demo.
1. `chore: add Redpanda service to docker-compose`
2. `feat(eventlog): publish job-state transitions to Kafka-API topic`
3. `feat(coordinator): rebuild in-memory job state by replaying event log on startup`
4. `test: add crash-recovery test (kill coordinator mid-job, restart, assert state rebuilt)`
5. `refactor(job): back the job store with the replay-rebuilt state`
6. `docs: update PROGRESS.md for Part 3`

### Part 4 — Second coordinator + Raft leader election → branch `part-4-raft`
**Deliverable:** Two coordinator replicas; killing the leader triggers automatic re-election within the sub-500ms target and in-flight jobs keep progressing.
1. `feat(coordinator): embed hashicorp/raft with single-node bootstrap`
2. `feat(coordinator): add second replica config and TCP raft transport`
3. `feat(coordinator): gate job-assignment writes behind leader check, forward writes to leader`
4. `test: add leader-election failover test (kill leader, assert re-election and continuity)`
5. `feat(coordinator): persist raft log/snapshot to a volume`
6. `docs: update PROGRESS.md for Part 4`

### Part 5 — Multiple workers + real scheduling → branch `part-5-scheduling`
**Deliverable:** Several workers running concurrently; jobs go to the least-loaded worker; a killed worker's in-flight jobs get reassigned automatically via heartbeat-timeout detection.
1. `feat(coordinator): track worker load via heartbeats`
2. `feat(coordinator): add least-loaded worker assignment strategy`
3. `feat(coordinator): add heartbeat-timeout failure detection`
4. `feat(coordinator): reassign in-flight jobs from dead workers`
5. `test: add multi-worker scheduling and failure-reassignment test`
6. `docs: update PROGRESS.md for Part 5`

### Part 6 — MCP server + REST API polish → branch `part-6-mcp-rest`
**Deliverable:** Full REST surface (`POST /jobs`, `GET /jobs/{id}`, `GET /jobs/{id}/logs` streaming, `DELETE /jobs/{id}`) and an MCP server exposing the same four actions, both as thin layers over the now-complete core engine. README documents `docker compose up` end to end. **This is the point where the system is "done" for resume purposes if Part 9 isn't pursued.**
1. `feat(api): finalize REST surface including log streaming and cancel`
2. `feat(mcp): add MCP server exposing submit_job, get_job_status, stream_logs, cancel_job`
3. `test: add MCP tool-call integration test`
4. `docs: write full README local run instructions (docker compose up)`
5. `docs: update PROGRESS.md for Part 6 — mark local system feature-complete`

### Part 7 — Terraform + self-managed k3s on EC2 (ON HOLD)
Not planned in detail until the cloud-deployment decision is explicitly revisited after Part 6. No commits until then.

### Part 8 — Observability → branch `part-8-observability`
**Deliverable:** Prometheus scrapes coordinator/worker metrics, Grafana dashboard shows job throughput/latency/failure rate, logs are structured JSON to stdout.
1. `feat(coordinator,worker): expose Prometheus metrics endpoints`
2. `chore: add Prometheus and Grafana to docker-compose`
3. `refactor: switch logging to structured JSON via log/slog`
4. `docs: add Grafana dashboard notes, update PROGRESS.md for Part 8`

### Part 9 — CI job type dogfooding (stretch) → branch `part-9-ci-dogfooding`
**Deliverable:** This system runs as real CI for the Filing Agent repo — clone, run test suite, report pass/fail + logs.
1. `feat(job): add CI-style job type (clone repo, run test command, capture pass/fail)`
2. `chore: point Filing Agent repo's CI at this orchestrator`
3. `docs: write up the cross-project CI story for README/resume`

---

## Self-review

- **Spec coverage:** Every architecture element (coordinator, worker, gRPC, Kafka/Redpanda, raft, Postgres, Docker, REST, MCP, deployment posture, learning sequence, git workflow) maps to a Part above; Part 7 is intentionally deferred per spec. Job types: CI-style lands in Part 9, code-execution lands as the Part 1 demo job body (arbitrary image+command *is* a code-execution job — no separate Part needed, matches spec's "recommended demo pair" once Part 9 adds the CI type).
- **Placeholder scan:** No part above is implemented in this file beyond commit-message sequencing — that's intentional (see scope-check note); the actual step-by-step TDD content lives in the per-Part detailed plans, starting with Part 0-1.
- **Type/interface consistency:** Deferred to the detailed per-Part plans, where real signatures get fixed. This roadmap only fixes the repo layout package boundaries (`internal/job`, `internal/eventlog`, etc.), which later plans must honor.
