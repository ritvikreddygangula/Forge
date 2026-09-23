package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ritvikreddygangula/forge/internal/eventlog"
	"github.com/ritvikreddygangula/forge/internal/job"
	"github.com/ritvikreddygangula/forge/internal/mcpserver"
)

func main() {
	brokersEnv := os.Getenv("REDPANDA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "localhost:9092"
	}
	brokers := strings.Split(brokersEnv, ",")

	startupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := eventlog.EnsureTopic(startupCtx, brokers, eventlog.DefaultTopic); err != nil {
		slog.Error("mcpserver failed to reach Redpanda", "error", err)
		os.Exit(1)
	}
	events, _, err := eventlog.NewKafkaConsumer(brokers, eventlog.DefaultTopic).ReadAll(startupCtx)
	if err != nil {
		slog.Error("mcpserver failed to replay event log", "error", err)
		os.Exit(1)
	}
	rebuiltJobs := eventlog.Rebuild(events)
	baseStore := job.NewMemoryStore()
	baseStore.Rebuild(rebuiltJobs)
	slog.Info("mcpserver replayed event log", "jobs_restored", len(rebuiltJobs))

	producer := eventlog.NewKafkaProducer(brokers, eventlog.DefaultTopic)
	store := eventlog.NewStore(baseStore, producer)

	server := mcpserver.New(store)
	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		slog.Error("mcpserver exited", "error", err)
		os.Exit(1)
	}
}
