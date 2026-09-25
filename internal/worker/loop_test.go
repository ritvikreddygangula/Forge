package worker_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
	"github.com/ritvikreddygangula/forge/internal/metrics"
	"github.com/ritvikreddygangula/forge/internal/worker"
)

// fakeJobServer is a minimal jobv1.JobServiceServer double, configured per test.
type fakeJobServer struct {
	jobv1.UnimplementedJobServiceServer
	pollResp *jobv1.PollJobResponse
	reported *jobv1.ReportResultRequest

	mu             sync.Mutex
	heartbeatCount int
}

func (f *fakeJobServer) PollJob(ctx context.Context, req *jobv1.PollJobRequest) (*jobv1.PollJobResponse, error) {
	return f.pollResp, nil
}

func (f *fakeJobServer) ReportResult(ctx context.Context, req *jobv1.ReportResultRequest) (*jobv1.ReportResultResponse, error) {
	f.reported = req
	return &jobv1.ReportResultResponse{}, nil
}

func (f *fakeJobServer) Heartbeat(ctx context.Context, req *jobv1.HeartbeatRequest) (*jobv1.HeartbeatResponse, error) {
	f.mu.Lock()
	f.heartbeatCount++
	f.mu.Unlock()
	return &jobv1.HeartbeatResponse{}, nil
}

func (f *fakeJobServer) HeartbeatCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.heartbeatCount
}

func newTestLoop(t *testing.T, fake *fakeJobServer) *worker.Loop {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	jobv1.RegisterJobServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	l, err := worker.NewLoopWithDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	})
	if err != nil {
		t.Fatalf("failed to build test loop: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func TestLoop_RunOnce_NoJobQueued(t *testing.T) {
	fake := &fakeJobServer{pollResp: &jobv1.PollJobResponse{HasJob: false}}
	l := newTestLoop(t, fake)
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
	fake := &fakeJobServer{pollResp: &jobv1.PollJobResponse{
		HasJob: true,
		Job:    &jobv1.Job{Id: "job-1", Image: "alpine", Command: []string{"true"}, TimeoutSeconds: 10},
	}}
	l := newTestLoop(t, fake)
	l.Execute = func(ctx context.Context, image string, command []string, timeoutSeconds int) (worker.ExecResult, error) {
		return worker.ExecResult{Stdout: "ok\n", ExitCode: 0}, nil
	}

	if err := l.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	if fake.reported == nil {
		t.Fatal("expected ReportResult to be called")
	}
	if fake.reported.Id != "job-1" {
		t.Fatalf("expected reported id job-1, got %q", fake.reported.Id)
	}
	if fake.reported.Status != "succeeded" {
		t.Fatalf("expected status succeeded, got %v", fake.reported.Status)
	}
}

// TestLoop_RunOnce_HeartbeatsWhileExecuting proves a worker keeps sending
// heartbeats while blocked inside a long Execute call, not just on the poll
// that claimed the job. Without this, a job that outlives the coordinator's
// dead-worker timeout would make an actively-executing worker look dead.
func TestLoop_RunOnce_HeartbeatsWhileExecuting(t *testing.T) {
	fake := &fakeJobServer{pollResp: &jobv1.PollJobResponse{
		HasJob: true,
		Job:    &jobv1.Job{Id: "job-1", Image: "alpine", Command: []string{"sleep"}, TimeoutSeconds: 10},
	}}
	l := newTestLoop(t, fake)
	l.PollInterval = 20 * time.Millisecond
	l.Execute = func(ctx context.Context, image string, command []string, timeoutSeconds int) (worker.ExecResult, error) {
		time.Sleep(90 * time.Millisecond)
		return worker.ExecResult{ExitCode: 0}, nil
	}

	if err := l.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	if count := fake.HeartbeatCount(); count < 2 {
		t.Fatalf("expected at least 2 heartbeats sent during a 90ms execution with a 20ms interval, got %d", count)
	}
}

func TestLoop_RunOnce_NonZeroExitReportsFailed(t *testing.T) {
	fake := &fakeJobServer{pollResp: &jobv1.PollJobResponse{
		HasJob: true,
		Job:    &jobv1.Job{Id: "job-2", Image: "alpine", Command: []string{"false"}, TimeoutSeconds: 10},
	}}
	l := newTestLoop(t, fake)
	l.Execute = func(ctx context.Context, image string, command []string, timeoutSeconds int) (worker.ExecResult, error) {
		return worker.ExecResult{ExitCode: 1}, nil
	}

	if err := l.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}
	if fake.reported.Status != "failed" {
		t.Fatalf("expected status failed, got %v", fake.reported.Status)
	}
}

// workerExecutionSampleCount reads the total number of observations
// recorded so far for WorkerExecutionDurationSeconds. testutil.CollectAndCount
// counts metric series (always 1 for a histogram), not observations.
func workerExecutionSampleCount(t *testing.T) uint64 {
	t.Helper()
	var m dto.Metric
	if err := metrics.WorkerExecutionDurationSeconds.Write(&m); err != nil {
		t.Fatalf("failed to write histogram metric: %v", err)
	}
	return m.GetHistogram().GetSampleCount()
}

func TestLoop_RunOnce_RecordsExecutionMetrics(t *testing.T) {
	fake := &fakeJobServer{pollResp: &jobv1.PollJobResponse{
		HasJob: true,
		Job:    &jobv1.Job{Id: "job-3", Image: "alpine", Command: []string{"true"}, TimeoutSeconds: 10},
	}}
	l := newTestLoop(t, fake)
	l.Execute = func(ctx context.Context, image string, command []string, timeoutSeconds int) (worker.ExecResult, error) {
		return worker.ExecResult{ExitCode: 0}, nil
	}

	beforeCounter := testutil.ToFloat64(metrics.WorkerJobsExecutedTotal.WithLabelValues("succeeded"))
	beforeCount := workerExecutionSampleCount(t)

	if err := l.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce returned error: %v", err)
	}

	if after := testutil.ToFloat64(metrics.WorkerJobsExecutedTotal.WithLabelValues("succeeded")); after != beforeCounter+1 {
		t.Fatalf("expected worker_jobs_executed_total{status=succeeded} to increment by 1, got %v -> %v", beforeCounter, after)
	}
	if after := workerExecutionSampleCount(t); after != beforeCount+1 {
		t.Fatalf("expected one new worker_execution_duration_seconds observation, got %d -> %d", beforeCount, after)
	}
}
