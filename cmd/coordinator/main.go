package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
	"github.com/ritvikreddygangula/forge/internal/coordinator"
	"github.com/ritvikreddygangula/forge/internal/eventlog"
	"github.com/ritvikreddygangula/forge/internal/job"
)

func main() {
	httpAddr := os.Getenv("COORDINATOR_ADDR")
	if httpAddr == "" {
		httpAddr = ":8080"
	}
	grpcAddr := os.Getenv("COORDINATOR_GRPC_ADDR")
	if grpcAddr == "" {
		grpcAddr = ":9090"
	}
	brokersEnv := os.Getenv("REDPANDA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "localhost:9092"
	}
	brokers := strings.Split(brokersEnv, ",")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := eventlog.EnsureTopic(ctx, brokers); err != nil {
		slog.Error("coordinator failed to reach Redpanda", "error", err)
		os.Exit(1)
	}

	events, err := eventlog.NewKafkaConsumer(brokers).ReadAll(ctx)
	if err != nil {
		slog.Error("coordinator failed to replay event log", "error", err)
		os.Exit(1)
	}
	rebuiltJobs := eventlog.Rebuild(events)

	baseStore := job.NewMemoryStore()
	baseStore.Rebuild(rebuiltJobs)
	slog.Info("coordinator replayed event log", "jobs_restored", len(rebuiltJobs))

	producer := eventlog.NewKafkaProducer(brokers)
	store := eventlog.NewStore(baseStore, producer)

	grpcLis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		slog.Error("coordinator failed to listen (gRPC)", "error", err)
		os.Exit(1)
	}
	grpcSrv := grpc.NewServer()
	jobv1.RegisterJobServiceServer(grpcSrv, coordinator.NewGRPCServer(store))
	go func() {
		slog.Info("coordinator gRPC starting", "addr", grpcAddr)
		if err := grpcSrv.Serve(grpcLis); err != nil {
			slog.Error("coordinator gRPC exited", "error", err)
			os.Exit(1)
		}
	}()

	httpSrv := coordinator.NewServer(store)
	slog.Info("coordinator HTTP starting", "addr", httpAddr)
	if err := http.ListenAndServe(httpAddr, httpSrv); err != nil {
		slog.Error("coordinator HTTP exited", "error", err)
		os.Exit(1)
	}
}
