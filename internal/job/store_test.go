package job_test

import (
	"testing"

	"github.com/ritvikreddygangula/forge/internal/job"
)

func TestMemoryStore_CreateAndGet(t *testing.T) {
	s := job.NewMemoryStore()
	j, err := s.Create("python:3.11", []string{"pytest", "tests/"}, 300)
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if j.Status != job.StatusQueued {
		t.Fatalf("expected status queued, got %s", j.Status)
	}

	got, err := s.Get(j.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.ID != j.ID {
		t.Fatalf("expected id %s, got %s", j.ID, got.ID)
	}
}

func TestMemoryStore_Get_NotFound(t *testing.T) {
	s := job.NewMemoryStore()
	_, err := s.Get("does-not-exist")
	if err != job.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestMemoryStore_ClaimNext_FIFO(t *testing.T) {
	s := job.NewMemoryStore()
	first, _ := s.Create("alpine", []string{"true"}, 10)
	s.Create("alpine", []string{"true"}, 10)

	claimed, err := s.ClaimNext()
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected a claimed job, got nil")
	}
	if claimed.ID != first.ID {
		t.Fatalf("expected FIFO claim of %s, got %s", first.ID, claimed.ID)
	}
	if claimed.Status != job.StatusRunning {
		t.Fatalf("expected claimed job status running, got %s", claimed.Status)
	}
}

func TestMemoryStore_ClaimNext_EmptyQueue(t *testing.T) {
	s := job.NewMemoryStore()
	claimed, err := s.ClaimNext()
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected nil when queue empty, got %+v", claimed)
	}
}

func TestMemoryStore_Complete(t *testing.T) {
	s := job.NewMemoryStore()
	j, _ := s.Create("alpine", []string{"true"}, 10)
	s.ClaimNext()

	if err := s.Complete(j.ID, job.StatusSucceeded, "ok\n", "", 0); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}

	got, _ := s.Get(j.ID)
	if got.Status != job.StatusSucceeded {
		t.Fatalf("expected status succeeded, got %s", got.Status)
	}
	if got.Stdout != "ok\n" {
		t.Fatalf("expected stdout %q, got %q", "ok\n", got.Stdout)
	}
}
