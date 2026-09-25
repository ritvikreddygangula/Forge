# Forge

Forge is a distributed job orchestrator built in Go. Submit a job (a container image plus a command),
and a pool of workers picks it up, runs it in Docker, and reports back the result. It stays correct
through real failures: a coordinator crash doesn't lose job history, a worker crash reassigns its
in-flight job to another worker, and a 3-node cluster survives losing its leader.

Under the hood: gRPC between coordinator and workers, a Kafka/Redpanda event log for durable state,
Raft for leader election across coordinator replicas.

## Example jobs

A job is a container image plus a command. Forge runs it in Docker and reports back the exit code,
stdout, and stderr.

```json
{"image": "alpine:3.19", "command": ["true"], "timeout_seconds": 30}
```
```json
{"image": "python:3.11", "command": ["pytest", "tests/"], "timeout_seconds": 300}
```
```json
{"image": "golang:1.23", "command": ["go", "build", "./..."], "timeout_seconds": 300}
```

## Setup and run

### Prerequisites

- Go 1.26 or later
- Docker or Colima, running

### 1. Start Redpanda

```bash
make compose-up
```

### 2. Start the coordinator

```bash
make run-coordinator
```

Listens on `:8080` (REST) and `:9090` (gRPC).

### 3. Start a worker

```bash
make run-worker
```

### 4. Submit a job

```bash
JOB_ID=$(curl -s -X POST localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"image":"alpine:3.19","command":["echo","hello from forge"],"timeout_seconds":30}' | jq -r '.id')

curl -s localhost:8080/jobs/$JOB_ID
```

Within a couple of seconds you'll see `"status":"succeeded"` and the echoed stdout.

## Features

**Cancel a queued job**
```bash
curl -s -X DELETE localhost:8080/jobs/$JOB_ID
```

**Stream logs**
```bash
curl -N localhost:8080/jobs/$JOB_ID/logs/stream
```

**Run multiple workers.** Run `make run-worker` again in another terminal. Each worker gets its own ID
automatically. If a worker crashes mid-job, the coordinator reassigns the job to another worker within
a few seconds.

**Run a 3-replica cluster**
```bash
COORDINATOR_REPLICA_ID=node1 make run-coordinator
COORDINATOR_REPLICA_ID=node2 make run-coordinator
COORDINATOR_REPLICA_ID=node3 make run-coordinator
```
Only the leader accepts writes; the other two forward automatically. Submit against any of the 3 REST
ports (`:8080`, `:8081`, `:8082`) and it works the same way. If the leader dies, the remaining two elect
a new one and already-submitted jobs stay readable throughout.

**MCP server**
```bash
make run-mcpserver
```
Exposes submit, status, logs, and cancel as MCP tools over stdio, for use with Claude Desktop or the
[MCP Inspector](https://github.com/modelcontextprotocol/inspector).

**Metrics and dashboards.** `make compose-up` also starts Prometheus (`localhost:9095`) and Grafana
(`localhost:3000`) with a dashboard for job throughput, latency, and failure rate.

## Measured results

| Metric | Result |
|---|---|
| Leader failover, real process kill | median 145ms |
| Leader failover, 30-trial benchmark | median 103ms, p99 170ms |
| Load test throughput, 25 workers, real Docker jobs | 12.9 to 16.7 jobs/sec, 100% success |
| Worker crash to job reassignment | about 7 seconds |
| Crash recovery | 100%, every job's state survives a coordinator restart |

## Environment variables

- `COORDINATOR_ADDR`: REST listen address (default `:8080`)
- `COORDINATOR_GRPC_ADDR`: gRPC listen address (default `:9090`); also what the worker dials
- `WORKER_METRICS_ADDR`: worker's metrics endpoint (default `:9091`)
- `REDPANDA_BROKERS`: broker addresses (default `localhost:9092`)
- `COORDINATOR_REPLICA_ID`: sets cluster mode when present (e.g. `node1`)
- `COORDINATOR_CLUSTER_CONFIG`: path to cluster config (default `deploy/raft-cluster.json`)
