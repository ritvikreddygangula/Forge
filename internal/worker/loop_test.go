package worker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ritvikreddygangula/forge/internal/worker"
)

func TestLoop_RunOnce_NoJobQueued(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"job": nil})
	}))
	defer srv.Close()

	l := worker.NewLoop(srv.URL)
	executed := false
	l.Execute = func(ctx context.Context, image string, command []string, timeoutSeconds int) (worker.ExecResult, error) {
		executed = true
		return worker.ExecResult{}, nil
	}

	if err := l.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	if executed {
		t.Fatal("expected Execute not to be called when no job is queued")
	}
}

func TestLoop_RunOnce_ExecutesAndReportsSuccess(t *testing.T) {
	var reported map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/worker/poll", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"job": map[string]any{"id": "job-1", "image": "alpine", "command": []string{"true"}, "timeout_seconds": 10},
		})
	})
	mux.HandleFunc("POST /internal/worker/result", func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&reported)
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	l := worker.NewLoop(srv.URL)
	l.Execute = func(ctx context.Context, image string, command []string, timeoutSeconds int) (worker.ExecResult, error) {
		return worker.ExecResult{Stdout: "ok\n", ExitCode: 0}, nil
	}

	if err := l.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	if reported["status"] != "succeeded" {
		t.Fatalf("expected status succeeded, got %v", reported["status"])
	}
	if reported["id"] != "job-1" {
		t.Fatalf("expected id job-1, got %v", reported["id"])
	}
}

func TestLoop_RunOnce_NonZeroExitReportsFailed(t *testing.T) {
	var reported map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/worker/poll", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"job": map[string]any{"id": "job-2", "image": "alpine", "command": []string{"false"}, "timeout_seconds": 10},
		})
	})
	mux.HandleFunc("POST /internal/worker/result", func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&reported)
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	l := worker.NewLoop(srv.URL)
	l.Execute = func(ctx context.Context, image string, command []string, timeoutSeconds int) (worker.ExecResult, error) {
		return worker.ExecResult{ExitCode: 1}, nil
	}

	if err := l.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	if reported["status"] != "failed" {
		t.Fatalf("expected status failed, got %v", reported["status"])
	}
}
