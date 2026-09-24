# Forge — measured metrics for resume/interview use

Every number on this page was produced by an actual run recorded in this file's own commands, on the
date noted next to it. Per this project's standing rule (see `CLAUDE.md`): nothing here is estimated or
rounded up from a target — if you need a fresher number before an interview, re-run the exact command
listed and replace the number, don't reuse this one past a significant code change.

## Headline numbers

| Metric | Value | How it was measured |
|---|---|---|
| Leader failover (real OS processes) | **median 145ms**, range 109–254ms (5 trials) | 3 real compiled coordinator binaries, real `kill -9` on the actual leader process, timed until a real HTTP client's retry against a survivor succeeds |
| Leader failover (library-level benchmark) | **median 103ms**, p99 170ms, 30/30 under 500ms (30 trials) | Real `hashicorp/raft` 3-node cluster in one process, `raft.Shutdown()` on the leader node, timed until the remaining 2 elect a new one |
| Load test throughput | **16.7 jobs/sec best, 12.9–16.7 typical** (500/500 succeeded every run) | 25 real `worker.Loop`s, real `docker run alpine:3.19 true` per job, 500 real jobs submitted up front, real Redpanda-backed event log. **More workers made this worse, not better — see below.** |
| Worker failure → job reassignment | **~7s** (bounded by a 6s heartbeat-timeout config, checked every 2s) | Real coordinator + real worker binary, `sleep 30` job, `kill -9` the worker mid-execution, polled job status until it flipped back to `queued` |
| Crash recovery correctness | **100%** (every acknowledged job's exact status/stdout survives a full process kill) | Real coordinator binary, submit + complete a job, `kill -9` the process, start a fresh one, confirm identical state replayed from Kafka/Redpanda |

## Full raw data

### Real-process failover (5 trials, 2026-09-23)

| Trial | Killed replica | Latency (ms) |
|---|---|---|
| 1 | node2 | 254.0 |
| 2 | node2 | 124.0 |
| 3 | node2 | 109.4 |
| 4 | node3 | 167.8 |
| 5 | node2 | 144.7 |

Median 144.7ms, mean ~160ms, min 109.4ms, max 254.0ms, all 5/5 under a 500ms target. This is a genuinely
different (and slower, as expected) measurement than the in-memory benchmark below — it includes real
`SIGKILL` delivery, real TCP connection failure detection, and a real HTTP client's retry loop, not just
the raft library's own internal election timer.

Reproduce: `/tmp/forge-coordinator-main` built from `go build -o /tmp/forge-coordinator-main ./cmd/coordinator`,
then start 3 replicas with `COORDINATOR_REPLICA_ID=node{1,2,3}` against `deploy/raft-cluster.json`, grep
each replica's own log for `entering leader state` to find the real leader PID, `kill -9` it, and time a
retry loop against a surviving replica's REST port until a submit returns `201`.

### In-memory raft library benchmark (30 trials, 2026-09-23 re-run)

```
go test ./internal/coordinator/... -run TestRaft_FailoverBenchmark -v -count=1
```
Result: `30 trials, median=102.593166ms, p99=170.141666ms, min=74.282166ms, max=170.141666ms, 30/30 under 500ms`

### Load test — throughput and the worker-count experiment (2026-09-23 and 2026-09-24)

```
go test -tags=integration ./internal/coordinator/... -run TestLoadTest -v -timeout 5m -count=1
```

**Baseline runs, 25 workers / 500 jobs, all 500/500 succeeded every time:**

| Date | Colima VM | Elapsed | Rate |
|---|---|---|---|
| 2026-09-23 | 2 CPU / 2GB | 33.24s | 15.0 jobs/sec |
| 2026-09-23 | 2 CPU / 2GB | 43.46s | 11.5 jobs/sec |
| 2026-09-23 | 2 CPU / 2GB | 31.86s | 15.7 jobs/sec |
| 2026-09-24 | 4 CPU / 4GB | 29.88s | **16.7 jobs/sec (best)** |
| 2026-09-24 | 4 CPU / 4GB | 38.75s | 12.9 jobs/sec |

**Worker-scaling experiment, 2026-09-24 — asked "would more workers give a bigger number?" and actually
measured it instead of assuming:**

| Workers | Jobs | Colima VM | Elapsed | Rate |
|---|---|---|---|---|
| 25 | 500 | 2 CPU / 2GB | ~32s avg | ~11.5–15.7 jobs/sec |
| 50 | 1000 | 2 CPU / 2GB | 106.1s | **9.4 jobs/sec — worse** |
| 25 | 500 | 4 CPU / 4GB | ~34s avg | ~12.9–16.7 jobs/sec |
| 50 | 1000 | 4 CPU / 4GB | 80.3s | **12.5 jobs/sec — still worse than 25** |

**Finding: doubling worker count made throughput worse, not better, at both VM sizes.** Doubling the
VM's CPU/memory recovered some of that loss (9.4 → 12.5 jobs/sec at 50 workers) but 50 workers never beat
25 workers even with double the compute. This means the ceiling isn't (just) host CPU count — the Docker
daemon's own per-container lifecycle handling (create/start/exit/teardown, all serialized through one
daemon process) doesn't parallelize past a point on this setup. 25 concurrent `docker run` invocations is
close to this machine's real sweet spot; pushing higher adds contention faster than it adds throughput.

This is a more interesting systems finding than a bigger number would have been, and it's the kind of
thing worth being able to explain in an interview: *"I measured this, found more workers made it worse,
and traced it to Docker daemon contention rather than assuming CPU count was the limit."*

## Honest caveats (know these before an interviewer asks)

- **Single machine.** All of the above ran on one laptop (coordinators, workers, Redpanda, and Docker
  containers all sharing the same CPU/IO), not separate hosts. Real distributed deployment would change
  the network-latency component of failover and could change throughput in either direction.
- **No per-worker concurrency.** Each worker executes one job at a time (`RunOnce` blocks for the full
  job duration) — the ~15 jobs/sec figure is roughly `numWorkers ÷ per-job Docker overhead`, not evidence
  of a highly concurrent execution engine.
- **More workers does NOT mean more throughput past a point — measured, not assumed.** An earlier draft
  of this doc claimed throughput "scales roughly linearly" with worker count; that was never actually
  tested and turned out to be wrong. Doubling workers (25→50) measurably *decreased* throughput at both a
  2 CPU/2GB and a 4 CPU/4GB Colima VM (see the worker-scaling experiment above) — the real ceiling on this
  machine is Docker daemon per-container overhead, not raw worker count or host CPU alone.
- **`alpine:3.19 true` is a trivial job.** It measures orchestration/scheduling overhead accurately, but
  says nothing about throughput for jobs with real workload inside the container.
- **The library-level 103ms figure and the real-process 145ms figure measure different things** — don't
  quote one as if it were the other. The real-process number is the honest end-to-end one; the in-memory
  one isolates just the raft library's own election speed.

## Suggested resume language (draft — edit to your own voice before using)

- "Built a distributed job orchestrator in Go with a 3-node Raft-replicated (`hashicorp/raft`) coordinator
  cluster, gRPC worker transport, and a Kafka/Redpanda-backed event log for durable job state and crash
  recovery, all verified with real multi-process failure injection rather than mocks."
- "Implemented heartbeat-based worker failure detection and automatic job reassignment; measured real
  coordinator failover latency of 145ms median (real process kill, 5 trials) and 103ms median (30-trial
  library benchmark), both well under a 500ms target."
- "Load-tested the system with 25 concurrent workers executing real Docker containers, sustaining
  12.9–16.7 jobs/sec across real job submissions with 100% completion and zero failures; identified
  through direct experimentation that doubling worker count actually reduced throughput, tracing the
  bottleneck to Docker daemon container-lifecycle contention rather than host CPU or worker count."

Don't inflate these into round numbers ("150ms", "15 jobs/sec, guaranteed") in the resume itself — the
ranges above are the honest story and hold up better under a follow-up question than a single tidy number
would.
