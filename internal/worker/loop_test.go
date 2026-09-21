package worker_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
	"github.com/ritvikreddygangula/forge/internal/worker"
)

// fakeJobServer is a minimal jobv1.JobServiceServer double, configured per test.
type fakeJobServer struct {
	jobv1.UnimplementedJobServiceServer
	pollResp *jobv1.PollJobResponse
	reported *jobv1.ReportResultRequest
}

func (f *fakeJobServer) PollJob(ctx context.Context, req *jobv1.PollJobRequest) (*jobv1.PollJobResponse, error) {
	return f.pollResp, nil
}

func (f *fakeJobServer) ReportResult(ctx context.Context, req *jobv1.ReportResultRequest) (*jobv1.ReportResultResponse, error) {
	f.reported = req
	return &jobv1.ReportResultResponse{}, nil
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
