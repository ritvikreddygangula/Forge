package job_test

import (
	"sync"
	"testing"
	"time"

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

func TestMemoryStore_Rebuild(t *testing.T) {
	s := job.NewMemoryStore()
	now := time.Now()
	s.Rebuild([]*job.Job{
		{ID: "a", Image: "alpine", Command: []string{"true"}, Status: job.StatusQueued, CreatedAt: now, UpdatedAt: now},
		{ID: "b", Image: "alpine", Command: []string{"true"}, Status: job.StatusSucceeded, Stdout: "ok\n", CreatedAt: now, UpdatedAt: now},
	})

	got, err := s.Get("a")
	if err != nil {
		t.Fatalf("Get(a) returned error: %v", err)
	}
	if got.Status != job.StatusQueued {
		t.Fatalf("expected job a status queued, got %s", got.Status)
	}

	got, err = s.Get("b")
	if err != nil {
		t.Fatalf("Get(b) returned error: %v", err)
	}
	if got.Status != job.StatusSucceeded || got.Stdout != "ok\n" {
		t.Fatalf("expected job b succeeded with stdout ok, got %+v", got)
	}

	// Only the still-queued job should be claimable — b is already terminal.
	claimed, err := s.ClaimNext()
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if claimed == nil || claimed.ID != "a" {
		t.Fatalf("expected to claim job a, got %+v", claimed)
	}
}

func TestMemoryStore_ApplyCreated(t *testing.T) {
	s := job.NewMemoryStore()
	now := time.Now()
	s.ApplyCreated(&job.Job{ID: "a", Image: "alpine", Command: []string{"true"}, Status: job.StatusQueued, CreatedAt: now, UpdatedAt: now})

	got, err := s.Get("a")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusQueued {
		t.Fatalf("expected status queued, got %s", got.Status)
	}
	claimed, err := s.ClaimNext()
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if claimed == nil || claimed.ID != "a" {
		t.Fatalf("expected applied job to be claimable, got %+v", claimed)
	}
}

func TestMemoryStore_ApplyCreated_IgnoresDuplicates(t *testing.T) {
	s := job.NewMemoryStore()
	now := time.Now()
	s.ApplyCreated(&job.Job{ID: "a", Status: job.StatusQueued, CreatedAt: now, UpdatedAt: now})
	if _, err := s.ClaimNext(); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if err := s.Complete("a", job.StatusSucceeded, "done\n", "", 0); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}

	// Re-applying JobCreated for the same ID (as happens when a replica
	// tails back its own already-locally-applied write) must not revert
	// progress that happened in between.
	s.ApplyCreated(&job.Job{ID: "a", Status: job.StatusQueued, CreatedAt: now, UpdatedAt: now})

	got, err := s.Get("a")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusSucceeded || got.Stdout != "done\n" {
		t.Fatalf("expected re-applying create to be a no-op, got %+v", got)
	}
}

func TestMemoryStore_ApplyClaimed(t *testing.T) {
	s := job.NewMemoryStore()
	now := time.Now()
	s.ApplyCreated(&job.Job{ID: "a", Status: job.StatusQueued, CreatedAt: now, UpdatedAt: now})

	s.ApplyClaimed("a", now)

	got, err := s.Get("a")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusRunning {
		t.Fatalf("expected status running, got %s", got.Status)
	}
}

func TestMemoryStore_ApplyCompleted(t *testing.T) {
	s := job.NewMemoryStore()
	now := time.Now()
	s.ApplyCreated(&job.Job{ID: "a", Status: job.StatusQueued, CreatedAt: now, UpdatedAt: now})
	s.ApplyClaimed("a", now)

	s.ApplyCompleted("a", job.StatusFailed, "", "boom\n", 1, now)

	got, err := s.Get("a")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusFailed || got.Stderr != "boom\n" || got.ExitCode != 1 {
		t.Fatalf("expected failed job with stderr boom, got %+v", got)
	}
}

func TestMemoryStore_Rebuild_ClearsPriorState(t *testing.T) {
	s := job.NewMemoryStore()
	if _, err := s.Create("alpine", []string{"true"}, 10); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	s.Rebuild([]*job.Job{{ID: "only-this-one", Status: job.StatusQueued, CreatedAt: time.Now(), UpdatedAt: time.Now()}})

	if _, err := s.Get("only-this-one"); err != nil {
		t.Fatalf("expected rebuilt job to exist: %v", err)
	}
	claimed, err := s.ClaimNext()
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if claimed == nil || claimed.ID != "only-this-one" {
		t.Fatalf("expected only the rebuilt job to be claimable, got %+v", claimed)
	}
}
