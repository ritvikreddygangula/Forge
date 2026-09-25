# Forge — Project Plan

A read-through companion before we start Part 2. For full detail see
[docs/spec.md](docs/spec.md) (the spec) and
[docs/plans/roadmap.md](docs/plans/roadmap.md)
(the exhaustive per-commit roadmap). This doc is the "what and why," not the "type this exact code."
If you're a fresh Claude Code session and not the person reading this in an editor, start at
[CLAUDE.md](CLAUDE.md) instead — it's the entry point built for that.

## What we're building

A distributed job orchestrator — mini GitHub Actions / mini Kubernetes Jobs, built from scratch in Go.
You submit a job (a container image + command), a coordinator hands it to a worker, the worker runs it
in Docker and reports back. The interesting part isn't the job-running itself — it's making the
**coordinator crash-safe**: a durable event log it can replay, and Raft leader election across multiple coordinator replicas so no single process is a single point of failure.

**Why it exists:** it's a portfolio piece specifically about distributed-systems fundamentals
(concurrency, fault tolerance, Go) — deliberately different from an AI/agents project, for range on a
resume. That framing matters for scope decisions later: don't let this drift into "another AI pipeline,"
and don't put a benchmark number anywhere (failover latency, throughput) that wasn't actually measured.

## Architecture

```
        REST / MCP*                    gRPC (from Part 2)
 client ───────────► coordinator ◄────────────► worker(s)
                       │      ▲                    │
                       ▼      │                    ▼
                   Postgres   │              docker run
                  (metadata)  │             (job sandbox)
                       │      │
                       ▼      │
                  Redpanda event log (from Part 3)
                (job-state transitions, replayed on restart)

           hashicorp/raft embedded in coordinator (from Part 4)
                 → leader election across replicas

  * REST is primary; MCP is a thin optional wrapper added in Branch 6 — not the point of the project.
```

Today (end of Part 1), it's just the top-left box: one coordinator, one worker, plain HTTP, an
in-memory store. Everything else in the diagram gets added one Part at a time.

## Tech stack (locked in)

| Layer | Choice |
|---|---|
| Language | Go 1.23 |
| Coordinator↔worker transport | plain HTTP now → gRPC from Part 2 |
| Event log | Redpanda (Kafka-wire-compatible, no ZooKeeper/JVM) |
| Leader election | `hashicorp/raft`, embedded in the coordinator binary |
| Metadata store | ~~Postgres via `pgx`~~ — superseded by the event-sourced Redpanda log; see "Scope calls" below |
| Job sandbox | Docker (via Colima locally), driven by `os/exec` |
| External interfaces | REST (primary), thin optional MCP server (Branch 6, see "Scope calls" below) |
| Logging/metrics | `log/slog`, Prometheus + Grafana (Branch 6) |
| Local orchestration | Docker Compose |
| Cloud | **Deferred.** Not decided, nothing provisioned. Revisited after Branch 6. |

## Scope calls (and why)

Three decisions worth being explicit about, since they shape what "done" means:

- **Raft means the `hashicorp/raft` library, never hand-rolled consensus.** This was never on the
  table as "implement Raft from scratch" — that's a multi-month research-grade undertaking (leader
  election, log replication, snapshotting, membership changes, all with subtle correctness bugs) that
  wouldn't teach you more about *this* project's actual point, which is wiring a coordinator so it
  survives real failures. Using a production-grade library and understanding its failure modes,
  persistence model, and how to gate writes/forward to the leader is exactly what real systems do
  (etcd, CockroachDB, Consul all embed Raft libraries too) — it's legitimate, resume-honest distributed
  systems work, not a shortcut. Branch 4 stays as planned: 3 replicas, real failover testing, real
  benchmark numbers.
- **MCP stays, but it is not the point (clarified 2026-09-21).** The original spec framed MCP as
  "connective tissue" between this project and an AI-agent project — that framing overstated it. MCP is
  already covered by the DeltaLedger project, so it's not a headline resume line here, and it isn't
  worth spending real design effort on. This project's actual differentiator is distributed-systems
  thinking: concurrency, fault tolerance, consensus, crash recovery (Parts 1-5). MCP stays as a small,
  thin, optional wrapper in Branch 6 — same four actions as REST, no more effort than that — precisely
  *because* it's cheap once REST exists, not because it's central. `docs/spec.md` and
  `docs/plans/roadmap.md` reflect this.
- **Postgres was never built — superseded by the event log, not forgotten (clarified 2026-09-24).** The
  original spec/roadmap called for Postgres as the "metadata store" for queryable job/worker history. In
  practice, the event-sourced Kafka/Redpanda log plus in-memory replay-on-startup (Part 3 onward) already
  gives durable, correct job state, and the actual REST/gRPC/MCP surface only ever needs one query shape —
  get a job by ID — which the in-memory store already serves directly. Postgres would only earn its keep
  if this project needed complex queries, filtering, or long-term historical reporting across a large job
  history, and it never does. Building it now would be unused scope creep on an otherwise finished
  project, not a real gap — this note exists so the mismatch between `docs/spec.md`'s architecture list
  and the actual code is a documented decision, not an accidental omission.

## Manual / off-repo steps

Some steps in this project can't be done by editing files — installing Docker/Colima, starting a local
daemon, creating a cloud account, provisioning infrastructure, generating an API key. Whenever a branch
needs one of these, it'll be called out explicitly and separately from the code-and-commit steps, marked
**⚠️ Manual step**, with exact instructions — never silently assumed. Nothing through Branch 6 needs
anything beyond a local Docker/Colima install; that changes only if/when Part 7 (cloud) is greenlit.

## Where we are

**Done and merged to `main`:**
- **Part 0** — repo scaffold, Makefile, CI (build/vet/lint/test), `.golangci.yml`.
- **Part 1** — plain HTTP skeleton, merged via PR #1:
  - `internal/job` — job domain model + in-memory store (`queued → running → succeeded/failed`)
  - `internal/coordinator` — `POST /jobs`, `GET /jobs/{id}`, `GET /jobs/{id}/logs`, plus the
    worker-facing `GET /internal/worker/poll` and `POST /internal/worker/result`
  - `internal/worker` — Docker-based executor (`docker run` via `os/exec`) and the poll-execute-report
    loop
  - Verified end-to-end locally: submit a job via `curl`, worker picks it up, runs it in a real
    container, status/logs come back correct.

`go build ./...`, `go vet ./...`, and `go test ./...` all pass on `main` right now.

## The 6 branches ahead

Everything after Part 0 ships as its own branch, cut from `main`, merged back via one PR each —
**sequentially, never two branches open at once.** Parts 6, 8, and 9 are bundled onto one branch
(Branch 6) because they're additive polish on a finished core, not a new distributed-systems concept;
everything else gets isolated on its own branch because it introduces exactly one new hard idea.

### Branch 2 — `part-2-grpc`
**New concept:** gRPC. **Change:** swap the coordinator↔worker transport from HTTP polling to gRPC
(`PollJob`, `ReportResult`, `StreamLogs`), generated via `buf`. REST stays as-is for the external
submit/status surface — only the internal transport changes. Behavior is identical to Part 1; this
Part is purely "learn gRPC against a system I already understand."

### Branch 3 — `part-3-event-log`
**New concept:** durable, replayable event log. **Change:** every job-state transition gets published
to Redpanda. On restart, the coordinator no longer starts empty — it rebuilds its in-memory state by
replaying the log. This is the first Part that's actually about fault tolerance rather than just
plumbing: proven by a test that kills the coordinator mid-job, restarts it, and asserts state came
back correctly.

### Branch 4 — `part-4-raft`
**New concept:** consensus / leader election. The hardest, most novel Part. **Change:** run **three**
coordinator replicas with `hashicorp/raft` embedded (three, not two — a 2-node cluster has no quorum
advantage over one node). Only the leader accepts writes; followers forward to it. Validated by killing the leader (both a plain process kill and a simulated network partition) and measuring real failover
latency across many trials — the numbers that eventually go on a resume have to come from this
benchmark, not be invented.

### Branch 5 — `part-5-scheduling`
**New concept:** fleet-level scheduling, distinct from Branch 4's coordinator-replica fault tolerance.
**Change:** multiple workers running concurrently, jobs routed to the least-loaded one, heartbeat-based
detection when a worker dies, in-flight jobs from a dead worker reassigned automatically. Closes with a
real load test — actual `docker run` jobs at scale, not a synthetic no-op job type, measuring genuine
sustained throughput.

### Branch 6 — `part-6-interfaces-observability` (Parts 6 + 8 + 9 combined)
**New concept:** none — this is "finish the surface of an already-working system." MCP is included but
deliberately thin (see "Scope calls" above) — REST is still the interface that matters.
- Part 6: finalize the REST surface (add `DELETE /jobs/{id}`, streaming logs), plus a minimal MCP server
  (`submit_job`, `get_job_status`, `stream_logs`, `cancel_job`) as a thin wrapper over the same core
  engine — same effort level as REST, not a separate design push.
- Part 8: Prometheus metrics + Grafana dashboard, structured JSON logging.
- Part 9 (stretch): point this orchestrator at the Filing Agent repo as its real CI backend —
  clone, run tests, report pass/fail.

**The project is "done" for resume purposes once Branch 5 lands, not Branch 6.** Branch 5 is the
finished, fault-tolerant core (HTTP → gRPC → durable event log → Raft leader election → multi-worker
scheduling) — that's the actual point of the project per the "Scope calls" section above. Branch 6
(interfaces + observability, MCP included) is real, worthwhile work and worth its own resume line about
observability, it just isn't the gate — and MCP specifically shouldn't get more than a thin-wrapper's
worth of effort inside it.

### On hold — Part 7 (Terraform + self-managed k3s on EC2)
Not part of the 6-branch count. Every part through Branch 6 runs fully locally via Docker Compose —
no cloud dependency needed to prove the whole system works. The cloud question gets revisited only
after Branch 6 ships, and nothing gets provisioned against a real AWS account before that decision.

## How we'll work, branch by branch

1. One branch open at a time, cut from latest `main`.
2. I write the actual code/tests for one small step, then hand you:
   - the exact `git add` / `git commit` command with a ready-made commit message
   - a one- or two-line plain-English explanation of what changed and why
   - anything to verify first (`go test ./...`, a `curl`, `make build`, etc.)
3. I never run `git` myself — you run every command, in your own terminal, at your own pace.
4. Any step that needs something outside the code (installing software, starting a daemon, a cloud
   account, an API key) is called out separately as **⚠️ Manual step**, before the commit it unblocks.
5. When a branch's Part(s) are fully working end-to-end, I hand you the push command and a PR
   title/description to open `part-N-... → main`.
6. `PROGRESS.md` gets updated at the end of the branch (and any long session) for cross-session
   continuity — one section per branch, one line per commit, appended, never dumped in bulk.
7. `CLAUDE.md` gets its "Where things stand right now" section updated at the same points, so a fresh
   Claude Code session (if this one is ever lost) can reorient in under a minute.

## Project layout

```
CLAUDE.md              # session-continuity entry point for a fresh Claude Code session
README.md              # how to run it
PLAN.md                # this file
PROGRESS.md            # append-only commit log, grouped by branch
docs/
  spec.md              # original project spec
  plans/
    roadmap.md          # exhaustive per-branch, per-commit task sequencing
    part-0-1-http-skeleton.md   # detailed TDD plan for Part 0+1 (done)
    part-2-grpc.md              # (next — written just before Branch 2 starts)
cmd/coordinator, cmd/worker
internal/job, internal/coordinator, internal/worker
```

## Next

Start Branch 2 (`part-2-grpc`). Before writing code, I'll draft the detailed bite-sized plan for it
at `docs/plans/part-2-grpc.md` (same style as `docs/plans/part-0-1-http-skeleton.md`), since Part 2's
exact task breakdown sharpens once looked at against the now-real Part 1 code.
