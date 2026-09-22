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

### Environment variables

- `COORDINATOR_ADDR` — the coordinator's REST listen address (default `:8080`).
- `COORDINATOR_GRPC_ADDR` — the coordinator's gRPC listen address (default `:9090`); on the worker side,
  the same variable is the address it dials (default `localhost:9090`).
- `REDPANDA_BROKERS` — comma-separated Redpanda broker address(es) the coordinator publishes to and
  replays from (default `localhost:9092`).

### Regenerating gRPC code

The worker-facing transport (`PollJob`, `ReportResult`, `StreamLogs`) is defined in
`api/proto/jobv1/job.proto` and generated into `api/proto/gen/jobv1/`. Generated code is committed, so
this is only needed when the `.proto` file changes:

```bash
make proto   # requires buf and the protoc-gen-go / protoc-gen-go-grpc plugins on PATH
```
