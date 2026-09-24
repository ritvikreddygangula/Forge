# Forge

A distributed job orchestrator (mini GitHub Actions / mini Kubernetes Jobs) built from scratch in Go, with real fault tolerance: crash-safe coordinator, Raft leader election, and a durable Kafka/Redpanda event log.

See [PLAN.md](./PLAN.md) for a readable overview of what's built and what's next, and [docs/spec.md](./docs/spec.md) for the full project spec, architecture, and build sequence.

## Running locally

### Prerequisites

Ensure Docker/Colima is running. If you don't already have Colima started, run:

```bash
colima start
```

### Step 1: Start Redpanda

The coordinator publishes every job-state transition to Redpanda and replays it on startup, so it needs
to be running first:

```bash
make compose-up
```

(`make compose-down` to stop it when you're done.)

### Step 2: Start the coordinator

In one terminal, start the coordinator. It listens on two ports: REST on `:8080` (for submitting jobs
and checking status) and gRPC on `:9090` (for the worker):

```bash
make run-coordinator
```

You should see output like:
```
INFO coordinator replayed event log jobs_restored=0
INFO coordinator gRPC starting addr=:9090
INFO coordinator HTTP starting addr=:8080
```

(`jobs_restored` will be non-zero if you've run jobs against this Redpanda before — see "Crash
recovery" below.)

### Step 3: Start the worker

In another terminal, start the worker pointing to the coordinator's gRPC port:

```bash
make run-worker
```

The worker will begin polling the coordinator for jobs over gRPC.

### Step 4: Submit and track a job

Using `curl`, submit a job to the coordinator and retrieve its status:

```bash
# Submit a job and capture its ID
JOB_ID=$(curl -s -X POST localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"image":"alpine:3.19","command":["echo","hello from forge"],"timeout_seconds":30}' | jq -r '.id')

# Poll the status (within ~2 seconds, the worker will execute it)
curl -s localhost:8080/jobs/$JOB_ID
```

Within ~2 seconds, you should see `"status":"succeeded"` in the response, along with the echoed stdout.

Note: the first run will also pull the `alpine:3.19` image, which can take longer than the timeout below allows — for a fresh machine, consider a higher `timeout_seconds` on the first try.

### Cancelling a job

```bash
curl -s -X DELETE localhost:8080/jobs/$JOB_ID
```

Only works while the job is still `queued`. Once a worker has claimed it, cancelling returns
`409 Conflict` instead — the pull-based worker model has no way to interrupt a job mid-execution, so
that's a deliberate scope decision, not a bug (see `docs/plans/part-6-rest-mcp.md`).

### Streaming logs

```bash
curl -N localhost:8080/jobs/$JOB_ID/logs/stream
```

Server-Sent Events (`event: stdout` / `event: stderr`), same captured output as the plain
`GET /jobs/{id}/logs` above. The executor only captures a job's output as a complete buffer once it
finishes, so this sends that buffer over a streaming wire format — not live tailing of a still-running job.

### Running multiple workers

Just run `make run-worker` again in another terminal — each worker generates its own random ID on
startup, so there's no config file to edit and no coordination needed, unlike the raft cluster's static
`deploy/raft-cluster.json` below. The coordinator hands out jobs to whichever worker polls next (a simple
pull-based scheduler — a worker only asks for more work once it's idle), and if a worker stops polling
mid-job (crash, `kill -9`, network partition), the coordinator notices within a few seconds and reassigns
its in-flight job to another worker. See `docs/plans/part-5-scheduling.md` for how that failure detection
works and `PROGRESS.md` for real measured throughput with 25 concurrent workers.

### Crash recovery

Every job-state transition is durably logged to Redpanda, not just held in memory — so killing the
coordinator doesn't lose job history. With a job already submitted and completed (Step 4 above), kill
the coordinator (Ctrl-C) and start a fresh one:

```bash
make run-coordinator
```

The log line changes to `jobs_restored=1` (or however many jobs you've run), and the same `curl
localhost:8080/jobs/$JOB_ID` from Step 4 returns the exact same status and stdout as before the
restart — rebuilt entirely by replaying the Redpanda log, with zero in-process continuity between the
old coordinator process and the new one.

### Running a 3-replica cluster

Instead of one coordinator, you can run 3 replicas using `hashicorp/raft` for leader election — only
the current leader accepts writes; the other two transparently forward write requests to it, so clients
and workers can talk to any of the 3 REST/gRPC ports and it just works. Job data itself is still shared
via the same Redpanda log every replica continuously tails (raft doesn't replicate job data — see
`docs/plans/part-4-raft.md` for why).

With `make compose-up` already running, start all 3 replicas (separate terminals, or backgrounded):

```bash
COORDINATOR_REPLICA_ID=node1 make run-coordinator
COORDINATOR_REPLICA_ID=node2 make run-coordinator
COORDINATOR_REPLICA_ID=node3 make run-coordinator
```

Each reads its own address (REST/gRPC/raft) from `deploy/raft-cluster.json` by matching its replica ID.
Exactly one will log `entering leader state`. Submit a job against **any** of the 3 REST ports —
including a follower's — and it works the same either way:

```bash
curl -s -X POST localhost:8082/jobs -H 'Content-Type: application/json' \
  -d '{"image":"alpine:3.19","command":["echo","works from any replica"],"timeout_seconds":30}'
```

Kill whichever process is currently leader (`Ctrl-C` or `kill`) — the remaining 2 elect a new leader in
well under 500ms (measured: 30-trial benchmark median ~106ms, p99 ~136ms — see `PROGRESS.md`), and
already-submitted jobs remain readable from every replica throughout.

Raft state persists to `data/<replica-id>/raft/` per replica, so a full restart of all 3 doesn't lose
cluster history.

### Running the MCP server

The same 4 actions (submit, status, logs, cancel) are also exposed over MCP, for use with an
MCP-aware client like Claude Desktop or the [MCP Inspector](https://github.com/modelcontextprotocol/inspector)
CLI. It's a thin wrapper over the same job store REST uses — not a separate engine.

```bash
make run-mcpserver
```

It runs over stdio, so most MCP hosts are configured by pointing them at the command
`go run ./cmd/mcpserver` (with `REDPANDA_BROKERS` set the same way as the coordinator, if not the
default). To try it directly with the Inspector CLI:

```bash
npx @modelcontextprotocol/inspector go run ./cmd/mcpserver
```

### Metrics and dashboards

`make compose-up` now also starts Prometheus (`http://localhost:9095`) and Grafana
(`http://localhost:3000`, anonymous admin access — local dev only, never expose this setup beyond your
own machine). Both auto-provision on startup: Prometheus scrapes the coordinator's `/metrics`
(`:8080`) and the worker's `/metrics` (`:9091` by default) via `host.docker.internal`, since both run on
the host rather than inside compose; Grafana's "Forge" dashboard is provisioned from
`deploy/grafana/dashboards/forge.json`, no manual setup needed.

The dashboard has 3 panels, each reading a metric the coordinator/worker record directly:
- **Job Throughput** — completions per second, broken out by status
- **Job Duration p95** — end-to-end latency from job creation to completion
- **Job Failure Rate** — failed completions per second

Metrics only move for whichever replica is actually handling writes (see
`docs/plans/part-8-observability.md` for why, in cluster mode). The worker's metrics port is
configurable via `WORKER_METRICS_ADDR` (default `:9091`) if you're running more than one worker on the
same machine — each needs its own port.

### Environment variables

- `COORDINATOR_ADDR` — the coordinator's REST listen address (default `:8080`); ignored in cluster mode
  (the address comes from `deploy/raft-cluster.json` instead).
- `COORDINATOR_GRPC_ADDR` — the coordinator's gRPC listen address (default `:9090`); on the worker side,
  the same variable is the address it dials (default `localhost:9090`). Also ignored in cluster mode.
- `WORKER_METRICS_ADDR` — the worker's Prometheus `/metrics` listen address (default `:9091`). Give each
  worker its own value if running more than one on the same machine.
- `REDPANDA_BROKERS` — comma-separated Redpanda broker address(es) the coordinator publishes to and
  replays from (default `localhost:9092`).
- `COORDINATOR_REPLICA_ID` — opts into cluster mode when set (e.g. `node1`); must match an `id` in the
  cluster config file. Unset (the default) runs a single standalone instance, exactly as in Parts 1-3.
- `COORDINATOR_CLUSTER_CONFIG` — path to the cluster config JSON (default `deploy/raft-cluster.json`).

### Regenerating gRPC code

The worker-facing transport (`PollJob`, `ReportResult`, `StreamLogs`) is defined in
`api/proto/jobv1/job.proto` and generated into `api/proto/gen/jobv1/`. Generated code is committed, so
this is only needed when the `.proto` file changes:

```bash
make proto   # requires buf and the protoc-gen-go / protoc-gen-go-grpc plugins on PATH
```
