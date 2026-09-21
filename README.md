# Forge

A distributed job orchestrator (mini GitHub Actions / mini Kubernetes Jobs) built from scratch in Go, with real fault tolerance: crash-safe coordinator, Raft leader election, and a durable Kafka/Redpanda event log.

See [distributed-orchestrator-spec.md](./distributed-orchestrator-spec.md) for the full project spec, architecture, and build sequence.

## Running locally

_Not yet available — Part 1 (plain HTTP coordinator + worker) hasn't landed. Local run instructions (`docker compose up` or equivalent) will be documented here once it does._
