# Distributed Job Orchestrator — Project Spec

## One-line pitch
A distributed job orchestrator — exposed via REST and MCP — that any service or AI agent can submit arbitrary containerized work to and get results back from. Think "mini GitHub Actions / mini Kubernetes Jobs," built from scratch, with real fault tolerance (crash-safe coordinator, leader election, durable event log).

## Why this project exists (resume framing — don't lose this)
This is the **second flagship project**, paired with the Filing Materiality & Diligence Agent. The two are deliberately different:
- **Filing Agent** → agentic AI systems, cloud infra, evals, MCP
- **Orchestrator** → distributed systems fundamentals, concurrency, fault tolerance, Go

The goal is *range*, not depth-in-one-lane. Don't let this project drift toward "another AI pipeline" — its whole value on the resume is being a foundational systems project.

Sequencing: build this **after** the Filing Agent ships and replaces Chatify on the resume. Once this project is done, it replaces Deep Research Multi-Agent Systems. Until then, do not add partial/unfinished bullets to the resume — see PROGRESS.md discipline below.

## What "a job" is, concretely
A job = a container image + a command + inputs + where to put output.

```yaml
job:
  image: python:3.11
  command: "pytest tests/"
  inputs: [repo_url, commit_sha]
  timeout: 300s
```

## Job types to actually support (pick 2 to demo well)
1. **CI-style jobs** — clone a repo, run its test suite, report pass/fail + logs. (Stretch goal: use this system as real CI for the Filing Agent repo — strong cross-project story.)
2. **Code execution jobs** — arbitrary code snippet + language, run sandboxed, return stdout/stderr/exit code.
3. Batch data-processing jobs and scheduled/cron jobs are documented as supported job types conceptually but are not required for the demo.

Recommended demo pair: **CI-runner + code-execution**, since together they're the most visually demoable.

## Core architecture
- **Coordinator** (Go) — accepts job submissions, assigns them to workers, tracks state
- **Worker agents** (Go) — long-running processes that register with the coordinator, pull jobs, execute in Docker containers, report results
- **gRPC** — coordinator ↔ worker communication (job assignment, heartbeats, log streaming)
- **Kafka (or Redpanda)** — durable, append-only event log of job-state transitions (queued → running → succeeded/failed). Coordinator rebuilds state by replaying this log on restart — this is what makes crash recovery real instead of "trust me, it recovers."
- **hashicorp/raft (Go library)** — leader election among coordinator replicas, embedded in the coordinator binary (no separate etcd service to run/pay for)
- **Postgres** — job/worker metadata and history
- **Docker** — job execution sandbox (note as a known limitation vs. gVisor/Firecracker for true untrusted-code isolation — mentioning this awareness in interviews is itself a signal)
  - Keep the container-based job model as-is — a job is a container image, full stop. Don't substitute raw OS processes/cgroups to dodge Docker; that's more manual work for weaker isolation and drops a resume-relevant skill.
  - For local dev, use the **Docker Engine directly** (e.g. via Colima on macOS, or Podman as a drop-in) rather than Docker Desktop — avoids the resource/licensing friction without changing anything about the job model itself.
  - Docker is free/open-source regardless of where it runs; *where* it runs is decided by the deployment question below.

## Interfaces
- **REST API**: `POST /jobs`, `GET /jobs/{id}`, `GET /jobs/{id}/logs` (stream), `DELETE /jobs/{id}`
- **MCP server**: `submit_job`, `get_job_status`, `stream_logs`, `cancel_job` — same actions, exposed so an AI agent (including the Filing Agent, conceptually) can delegate heavy/long-running work to this system instead of blocking its own execution. This is the real connective tissue between your two projects — worth a line in the eventual resume summary.

## Deployment — cloud decision deferred, local-first for now
- Default posture: build and run the entire system locally via Docker Compose (coordinator, worker(s), Kafka/Redpanda, Postgres). This covers Parts 1–6 in full — no cloud dependency needed to get the whole system working end-to-end.
- **The AWS/cloud question (Part 7) is explicitly not decided yet.** Don't provision AWS resources or write Terraform against a real account until that decision is revisited — planned for after Part 6, once the whole system is proven working locally.
- If/when cloud is greenlit, this is the fallback plan (unchanged from earlier thinking, just not committed to yet):
  - Not EKS — the ~$73/month flat control-plane fee isn't worth it for a portfolio project.
  - Self-managed k3s on EC2 (free-tier instance) — no control-plane fee, still a legitimate "deployed on Kubernetes" resume line.
  - Kafka/Redpanda, Prometheus/Grafana run as pods inside the k3s cluster — no separate managed service, no extra bill.
  - Stop EC2 instances when not actively developing; spin up only long enough to capture a demo, then tear down via Terraform rather than leaving anything running.
  - Terraform from the start once this path is taken — not retrofitted.
  - Resolve the AWS free-tier eligibility question before provisioning anything, whenever this is revisited.
- **Regardless of the cloud decision, the README must clearly document how to run the whole system locally** (`docker compose up` or equivalent) — this is required documentation, not optional, since local-first is the default path either way.

## Learning sequence (do NOT skip ahead — each step is fully working before the next)
This order exists specifically so you're never debugging more than one new concept at a time.

1. **Part 1 — Plain HTTP skeleton.** Coordinator + one worker over plain REST/HTTP. No gRPC, no Kafka, no leader election. "Submit a job, worker runs it in Docker, reports back." Get this fully working end-to-end first.
2. **Part 2 — Swap HTTP for gRPC.** Same behavior, better transport. Learn gRPC in isolation against a system you already understand.
3. **Part 3 — Kafka/Redpanda event log.** Job-state transitions become an append-only log. Coordinator can now crash and rebuild state by replaying.
4. **Part 4 — Second coordinator + Raft leader election.** Handle coordinator crashes, not just worker crashes.
5. **Part 5 — Multiple workers + real scheduling.** Least-loaded worker gets the job; heartbeat-based failure detection reassigns jobs from dead workers.
6. **Part 6 — MCP server + REST API polish.** Both thin interfaces over the now-working core engine.
7. **Part 7 — (on hold, decide after Part 6) Terraform + self-managed k3s on EC2.** Only pursued if the cloud deployment decision above is greenlit. Until then, the system's deployment story is: fully working locally via Docker Compose, documented in README.
8. **Part 8 — Observability.** Prometheus + Grafana in-cluster; structured logging to stdout.
9. **Part 9 (stretch) — CI job type dogfooding.** Point this system at the Filing Agent repo as its real CI backend.

## Git workflow (same convention as the Filing Agent — don't deviate)
- Part 0 (boilerplate/repo scaffold) → straight to `main`
- Each Part above → its own feature branch (`part-N-short-description`) cut from latest `main`
- Small, atomic commits per sub-step
- Claude Code **never runs git commands** — it outputs one short imperative-mood commit message per sub-step, and a PR title/description when a Part is complete. Ritvik performs all git operations.
- `PROGRESS.md` maintained at the repo root for cross-session continuity — update it at the end of every session, even a short one, so nothing gets lost between sessions.

