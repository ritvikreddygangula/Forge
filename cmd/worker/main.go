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
	coordinatorURL := os.Getenv("COORDINATOR_URL")
	if coordinatorURL == "" {
		coordinatorURL = "http://localhost:8080"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	l := worker.NewLoop(coordinatorURL)
	slog.Info("worker starting", "coordinator", coordinatorURL)
	l.Run(ctx)
	slog.Info("worker stopped")
}
