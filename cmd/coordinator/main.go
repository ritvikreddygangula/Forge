package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/ritvikreddygangula/forge/internal/coordinator"
	"github.com/ritvikreddygangula/forge/internal/job"
)

func main() {
	addr := os.Getenv("COORDINATOR_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	store := job.NewMemoryStore()
	srv := coordinator.NewServer(store)

	slog.Info("coordinator starting", "addr", addr)
	if err := http.ListenAndServe(addr, srv); err != nil {
		slog.Error("coordinator exited", "error", err)
		os.Exit(1)
	}
}
