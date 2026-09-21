package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ritvikreddygangula/forge/internal/worker"
)

func main() {
	coordinatorAddr := os.Getenv("COORDINATOR_GRPC_ADDR")
	if coordinatorAddr == "" {
		coordinatorAddr = "localhost:9090"
	}

	l, err := worker.NewLoop(coordinatorAddr)
	if err != nil {
		slog.Error("worker failed to connect", "error", err)
		os.Exit(1)
	}
	defer func() { _ = l.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("worker starting", "coordinator", coordinatorAddr)
	l.Run(ctx)
	slog.Info("worker stopped")
}
