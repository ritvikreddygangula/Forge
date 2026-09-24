# Forge — Claude Code entry point

Read this first, every session. It's short on purpose — it's a map to the real docs, plus the
few rules that must never silently slip.

## Read next, in this order
1. **[PLAN.md](PLAN.md)** — what the project is, the architecture, the 6-branch roadmap, why it's scoped this way.
2. **[PROGRESS.md](PROGRESS.md)** — what's actually landed, one line per commit, grouped by branch.
3. **[docs/plans/roadmap.md](docs/plans/roadmap.md)** — the exhaustive per-branch, per-commit task sequencing and tech-stack decisions.
4. **[docs/spec.md](docs/spec.md)** — the original project spec (architecture, job model, resume framing).
5. A per-part detailed TDD plan in `docs/plans/` if one exists for the part in progress (e.g. `part-0-1-http-skeleton.md`); if the current part doesn't have one yet, write it before writing code.

## Where things stand right now
- **Done and merged to `main`:** Part 0 (scaffold), Part 1 (plain HTTP coordinator/worker, PR #1), Part 2 (gRPC transport, PR #2), Part 3 (Redpanda event log + continuous-tail-free replay-once-at-startup, PR #3), Part 4 (3-replica `hashicorp/raft` cluster for leader election, PR #4 — real measured failover: 30-trial benchmark, median 106ms, p99 136ms), Part 5 (worker identity, heartbeat-timeout failure detection/reassignment, real load test — 25 workers, 500 real `docker run` jobs, 11.5-15.0 jobs/sec, PR #5). **Part 5 is the resume-done checkpoint** (see below) — everything after it is real, valuable polish, not the finish line.
- **In progress:** Branch 6 (`part-6-interfaces-observability`) — Parts 6 and 8 are both done: REST surface finalized (`DELETE /jobs/{id}` cancel, SSE log streaming), a thin MCP server (`submit_job`/`get_job_status`/`stream_logs`/`cancel_job` over stdio), and observability (Prometheus metrics on coordinator+worker, a provisioned Grafana dashboard, structured JSON logging — see `docs/plans/part-6-rest-mcp.md`, `docs/plans/part-8-observability.md`, and `PROGRESS.md`'s Part 6/8 sections for the full task lists and the real bugs manual verification caught along the way). Part 9 (CI dogfooding stretch) was deliberately skipped — not required for resume-done, and explicitly a stretch goal. `go build`/`go vet`/`go test -race`/`golangci-lint` (both tag sets) and the integration suite all pass. Not yet merged — next action is to open the PR for `part-6-interfaces-observability` → `main`.
- **Update this section** (and `PROGRESS.md`) at the end of every session, even a short one, so the next session doesn't have to reconstruct state from git log.

## Rules that must never slip
- **Claude never runs `git` commands. Not even read-adjacent ones like `git mv`, `git add`, or `git restore` — nothing.** Do the file edit/move with plain filesystem tools, then hand the user the exact `git` commands + a commit message + a one-line explanation. They run everything, at their own pace. (This has been broken once already this project — see PROGRESS.md's docs-restructuring entry. Don't repeat it.)
- **Before handing over any `git reset --hard`, branch-move, or checkout sequence, check for uncommitted changes in tracked files first** (`git status`) and fold them into the plan explicitly — don't just reason about which *commits* are safe. A `reset --hard` wipes uncommitted tracked-file edits silently, even when the commits involved are all safe to move around. (Broke this once during Part 2 — an uncommitted Makefile/go.mod/CI edit got silently destroyed by a reset I said was "safe," and the next commit landed with a message that didn't match its contents. Caught and fixed via `git commit --amend`, but shouldn't have happened.)
- **One branch open at a time**, cut from latest `main`, merged back via one PR before the next branch starts.
- **Flag every manual/off-repo step explicitly**, inline, marked "⚠️ Manual step" — installing Docker/Colima, starting a daemon, creating a cloud account, provisioning infrastructure, obtaining an API key. Never assume the environment already has something the user hasn't been told about.
- **MCP stays, but it is not the point** (clarified 2026-09-21) — it's a thin, optional Branch 6 add-on over the finished core (same four actions as REST, no more design effort than that). Already covered by the DeltaLedger project, so it's not a headline resume line here. Don't let it scope-creep past "thin wrapper," and don't gate anything on it — see the resume-done checkpoint below.
- **Raft means `hashicorp/raft` (a library), never a from-scratch consensus implementation.** Resume/interview language must say "implemented leader election and log replication across N coordinators using Raft consensus (hashicorp/raft)," never "implemented Raft from scratch."
- **No invented numbers.** Any benchmark claim (failover latency, throughput, worker count) must come from an actual test run recorded in `PROGRESS.md`, never written down before it's measured.
- **The project is "resume-done" as of Branch 5** (the full fault-tolerant core: HTTP → gRPC → event log → Raft → scheduling, all real-tested, not Branch 6). Branch 6 (REST polish + observability) is still worth doing and worth its own resume line, it just isn't the finish line.
- Don't skip ahead a Part before the current one works end-to-end — the whole point of the sequencing is never debugging more than one new concept at a time.
