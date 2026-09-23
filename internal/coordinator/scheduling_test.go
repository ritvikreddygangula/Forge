package coordinator_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
	"github.com/ritvikreddygangula/forge/internal/coordinator"
	"github.com/ritvikreddygangula/forge/internal/job"
	"github.com/ritvikreddygangula/forge/internal/worker"
)

// TestScheduling_ReassignsDeadWorkersJobToSurvivor proves the whole
// mechanism end-to-end at the coordinator level: two real worker.Loops
// against one real coordinator (fake Execute, no real Docker — this test is
// about scheduling/reassignment, not execution), with a dead worker's
// in-flight job reassigned to and completed by a survivor.
func TestScheduling_ReassignsDeadWorkersJobToSurvivor(t *testing.T) {
	store := job.NewMemoryStore()
	registry := coordinator.NewWorkerRegistry()
	grpcServer := coordinator.NewGRPCServer(store)
	grpcServer.SetWorkerRegistry(registry)

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	jobv1.RegisterJobServiceServer(s, grpcServer)
	go func() { _ = s.Serve(lis) }()
	defer s.Stop()

	created, err := store.Create("alpine", []string{"true"}, 10)
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	deadWorker, err := worker.NewLoop(lis.Addr().String())
	if err != nil {
		t.Fatalf("failed to build worker loop: %v", err)
	}
	defer func() { _ = deadWorker.Close() }()
	deadWorker.Execute = func(ctx context.Context, image string, command []string, timeoutSeconds int) (worker.ExecResult, error) {
		return worker.ExecResult{Stdout: "should never actually run\n", ExitCode: 0}, nil
	}

	// The "dead" worker claims the job via PollOnce, then simply never
	// reports back — exactly what a crashed worker looks like from the
	// coordinator's side.
	ctx := context.Background()
	pollResp, err := deadWorker.PollOnce(ctx)
	if err != nil {
		t.Fatalf("PollOnce returned error: %v", err)
	}
	if !pollResp.HasJob || pollResp.Job.Id != created.ID {
		t.Fatalf("expected the dead worker to claim job %s, got %+v", created.ID, pollResp)
	}

	got, err := store.Get(created.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusRunning || got.WorkerID == "" {
		t.Fatalf("expected job claimed by the dead worker, got %+v", got)
	}

	reaper := coordinator.NewReaper(registry, store, nil, 5*time.Second)
	reaper.Tick(time.Now().Add(10 * time.Second)) // simulate time passing, no real sleep

	got, err = store.Get(created.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusQueued {
		t.Fatalf("expected job requeued after reaping, got %+v", got)
	}

	survivor, err := worker.NewLoop(lis.Addr().String())
	if err != nil {
		t.Fatalf("failed to build survivor loop: %v", err)
	}
	defer func() { _ = survivor.Close() }()
	executed := false
	survivor.Execute = func(ctx context.Context, image string, command []string, timeoutSeconds int) (worker.ExecResult, error) {
		executed = true
		return worker.ExecResult{Stdout: "ok\n", ExitCode: 0}, nil
	}
	if err := survivor.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	if !executed {
		t.Fatal("expected the survivor to claim and execute the reassigned job")
	}

	got, err = store.Get(created.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusSucceeded {
		t.Fatalf("expected job completed by survivor, got %+v", got)
	}
}
