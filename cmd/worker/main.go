package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ritvikreddygangula/forge/internal/worker"
)

func main() {
	coordinatorAddr := os.Getenv("COORDINATOR_GRPC_ADDR")
	if coordinatorAddr == "" {
		coordinatorAddr = "localhost:9090"
	}
	metricsAddr := os.Getenv("WORKER_METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":9091"
	}

	l, err := worker.NewLoop(coordinatorAddr)
	if err != nil {
		slog.Error("worker failed to connect", "error", err)
		os.Exit(1)
	}
	defer func() { _ = l.Close() }()

	go func() {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", promhttp.Handler())
		slog.Info("worker metrics starting", "addr", metricsAddr)
		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			slog.Error("worker metrics server exited", "error", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("worker starting", "coordinator", coordinatorAddr)
	l.Run(ctx)
	slog.Info("worker stopped")
}
