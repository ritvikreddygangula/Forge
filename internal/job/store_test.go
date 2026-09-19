package job_test

import (
	"sync"
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
	if _, err := s.Create("alpine", []string{"true"}, 10); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

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
	if _, err := s.ClaimNext(); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}

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

func TestMemoryStore_ClaimNext_ConcurrentClaimsAreUnique(t *testing.T) {
	const n = 20
	s := job.NewMemoryStore()
	for i := 0; i < n; i++ {
		if _, err := s.Create("alpine", []string{"true"}, 10); err != nil {
			t.Fatalf("Create returned error: %v", err)
		}
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		claimed []string
	)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j, err := s.ClaimNext()
			if err != nil {
				t.Errorf("ClaimNext returned error: %v", err)
				return
			}
			if j == nil {
				return
			}
			mu.Lock()
			claimed = append(claimed, j.ID)
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(claimed) != n {
		t.Fatalf("expected %d jobs claimed, got %d", n, len(claimed))
	}

	seen := make(map[string]bool, n)
	for _, id := range claimed {
		if seen[id] {
			t.Fatalf("job %s claimed more than once", id)
		}
		seen[id] = true
	}
	if len(seen) != n {
		t.Fatalf("expected %d distinct jobs claimed, got %d", n, len(seen))
	}
}
