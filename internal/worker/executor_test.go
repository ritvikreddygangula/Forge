//go:build integration

package worker_test

import (
	"context"
	"testing"

	"github.com/ritvikreddygangula/forge/internal/worker"
)

func TestRunJob_Success(t *testing.T) {
	result, err := worker.RunJob(context.Background(), "alpine:3.19", []string{"echo", "hello"}, 30)
	if err != nil {
		t.Fatalf("RunJob returned error: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", result.ExitCode)
	}
	if result.Stdout != "hello\n" {
		t.Fatalf("expected stdout %q, got %q", "hello\n", result.Stdout)
	}
}

func TestRunJob_NonZeroExit(t *testing.T) {
	result, err := worker.RunJob(context.Background(), "alpine:3.19", []string{"sh", "-c", "exit 3"}, 30)
	if err != nil {
		t.Fatalf("RunJob returned error: %v", err)
	}
	if result.ExitCode != 3 {
		t.Fatalf("expected exit code 3, got %d", result.ExitCode)
	}
}

func TestRunJob_Timeout(t *testing.T) {
	_, err := worker.RunJob(context.Background(), "alpine:3.19", []string{"sleep", "5"}, 1)
	if err == nil {
		t.Fatal("expected a timeout error, got nil")
	}
}
