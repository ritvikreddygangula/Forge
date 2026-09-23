package eventlog_test

import (
	"testing"
	"time"

	"github.com/ritvikreddygangula/forge/internal/eventlog"
	"github.com/ritvikreddygangula/forge/internal/job"
)

func TestRebuild_CreatedOnly(t *testing.T) {
	now := time.Now()
	jobs := eventlog.Rebuild([]eventlog.Event{
		{Type: eventlog.EventJobCreated, JobID: "a", Image: "alpine", Command: []string{"true"}, TimeoutSeconds: 10, Timestamp: now},
	})
	if len(jobs) != 1 || jobs[0].Status != job.StatusQueued {
		t.Fatalf("expected one queued job, got %+v", jobs)
	}
}

func TestRebuild_FullLifecycle(t *testing.T) {
	now := time.Now()
	jobs := eventlog.Rebuild([]eventlog.Event{
		{Type: eventlog.EventJobCreated, JobID: "a", Image: "alpine", Command: []string{"true"}, TimeoutSeconds: 10, Timestamp: now},
		{Type: eventlog.EventJobClaimed, JobID: "a", Timestamp: now},
		{Type: eventlog.EventJobCompleted, JobID: "a", Status: job.StatusSucceeded, Stdout: "ok\n", ExitCode: 0, Timestamp: now},
	})
	if len(jobs) != 1 {
		t.Fatalf("expected one job, got %d", len(jobs))
	}
	if jobs[0].Status != job.StatusSucceeded || jobs[0].Stdout != "ok\n" {
		t.Fatalf("expected succeeded job with stdout ok, got %+v", jobs[0])
	}
}

func TestRebuild_PreservesCreationOrder(t *testing.T) {
	now := time.Now()
	jobs := eventlog.Rebuild([]eventlog.Event{
		{Type: eventlog.EventJobCreated, JobID: "first", Timestamp: now},
		{Type: eventlog.EventJobCreated, JobID: "second", Timestamp: now},
	})
	if len(jobs) != 2 || jobs[0].ID != "first" || jobs[1].ID != "second" {
		t.Fatalf("expected [first, second] in order, got %+v", jobs)
	}
}

func TestRebuild_IgnoresEventsForUnknownJob(t *testing.T) {
	jobs := eventlog.Rebuild([]eventlog.Event{
		{Type: eventlog.EventJobClaimed, JobID: "never-created", Timestamp: time.Now()},
	})
	if len(jobs) != 0 {
		t.Fatalf("expected no jobs, got %+v", jobs)
	}
}

func TestRebuild_RequeuedJobReturnsToQueued(t *testing.T) {
	now := time.Now()
	jobs := eventlog.Rebuild([]eventlog.Event{
		{Type: eventlog.EventJobCreated, JobID: "a", Timestamp: now},
		{Type: eventlog.EventJobClaimed, JobID: "a", WorkerID: "worker-1", Timestamp: now},
		{Type: eventlog.EventJobRequeued, JobID: "a", Timestamp: now},
	})
	if len(jobs) != 1 || jobs[0].Status != job.StatusQueued || jobs[0].WorkerID != "" {
		t.Fatalf("expected job back to queued with no worker, got %+v", jobs)
	}
}

func TestApplyEvent_FullLifecycle(t *testing.T) {
	store := job.NewMemoryStore()
	now := time.Now()

	eventlog.ApplyEvent(store, eventlog.Event{Type: eventlog.EventJobCreated, JobID: "a", Image: "alpine", Command: []string{"true"}, TimeoutSeconds: 10, Timestamp: now})
	eventlog.ApplyEvent(store, eventlog.Event{Type: eventlog.EventJobClaimed, JobID: "a", Timestamp: now})
	eventlog.ApplyEvent(store, eventlog.Event{Type: eventlog.EventJobCompleted, JobID: "a", Status: job.StatusSucceeded, Stdout: "ok\n", Timestamp: now})

	got, err := store.Get("a")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusSucceeded || got.Stdout != "ok\n" {
		t.Fatalf("expected succeeded job with stdout ok, got %+v", got)
	}
}

func TestApplyEvent_Requeued(t *testing.T) {
	store := job.NewMemoryStore()
	now := time.Now()

	eventlog.ApplyEvent(store, eventlog.Event{Type: eventlog.EventJobCreated, JobID: "a", Timestamp: now})
	eventlog.ApplyEvent(store, eventlog.Event{Type: eventlog.EventJobClaimed, JobID: "a", WorkerID: "worker-1", Timestamp: now})
	eventlog.ApplyEvent(store, eventlog.Event{Type: eventlog.EventJobRequeued, JobID: "a", Timestamp: now})

	got, err := store.Get("a")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusQueued || got.WorkerID != "" {
		t.Fatalf("expected queued with no worker, got %+v", got)
	}
}

func TestRebuild_CancelledJobStaysCancelled(t *testing.T) {
	now := time.Now()
	jobs := eventlog.Rebuild([]eventlog.Event{
		{Type: eventlog.EventJobCreated, JobID: "a", Timestamp: now},
		{Type: eventlog.EventJobCancelled, JobID: "a", Timestamp: now},
	})
	if len(jobs) != 1 || jobs[0].Status != job.StatusCancelled {
		t.Fatalf("expected cancelled job, got %+v", jobs)
	}
}

func TestApplyEvent_Cancelled(t *testing.T) {
	store := job.NewMemoryStore()
	now := time.Now()

	eventlog.ApplyEvent(store, eventlog.Event{Type: eventlog.EventJobCreated, JobID: "a", Timestamp: now})
	eventlog.ApplyEvent(store, eventlog.Event{Type: eventlog.EventJobCancelled, JobID: "a", Timestamp: now})

	got, err := store.Get("a")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusCancelled {
		t.Fatalf("expected cancelled, got %+v", got)
	}
}
