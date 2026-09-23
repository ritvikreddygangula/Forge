package worker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
	"github.com/ritvikreddygangula/forge/internal/job"
)

// DefaultPollInterval is how often a Loop polls when no other value is set.
// Exported so the coordinator can size its dead-worker timeout as a multiple
// of it without the two constants drifting apart independently.
const DefaultPollInterval = 2 * time.Second

type Loop struct {
	ID           string // sent on every poll; every poll doubles as a heartbeat
	client       jobv1.JobServiceClient
	conn         *grpc.ClientConn
	PollInterval time.Duration
	Execute      func(ctx context.Context, image string, command []string, timeoutSeconds int) (ExecResult, error)
}

// NewLoop dials the coordinator's gRPC address (e.g. "localhost:9090").
func NewLoop(coordinatorGRPCAddr string) (*Loop, error) {
	conn, err := grpc.NewClient(coordinatorGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial coordinator: %w", err)
	}
	return newLoop(conn), nil
}

// NewLoopWithDialer is the test seam — lets tests point the gRPC client at an
// in-memory bufconn listener instead of a real network address.
func NewLoopWithDialer(dialer func(context.Context, string) (net.Conn, error)) (*Loop, error) {
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial test coordinator: %w", err)
	}
	return newLoop(conn), nil
}

func newLoop(conn *grpc.ClientConn) *Loop {
	return &Loop{
		ID:           uuid.NewString(),
		client:       jobv1.NewJobServiceClient(conn),
		conn:         conn,
		PollInterval: DefaultPollInterval,
		Execute:      RunJob,
	}
}

func (l *Loop) Close() error { return l.conn.Close() }

// PollOnce claims at most one job without executing or reporting on it — the
// test seam for simulating a worker that claims a job and then crashes
// before ever finishing it, without needing a real hung Execute.
func (l *Loop) PollOnce(ctx context.Context) (*jobv1.PollJobResponse, error) {
	return l.client.PollJob(ctx, &jobv1.PollJobRequest{WorkerId: l.ID})
}

func (l *Loop) RunOnce(ctx context.Context) error {
	resp, err := l.client.PollJob(ctx, &jobv1.PollJobRequest{WorkerId: l.ID})
	if err != nil {
		return fmt.Errorf("poll failed: %w", err)
	}
	if !resp.HasJob {
		return nil
	}
	j := resp.Job

	slog.Info("job claimed", "id", j.Id, "image", j.Image)

	execDone := make(chan struct{})
	go l.heartbeatWhileExecuting(ctx, execDone)
	result, execErr := l.Execute(ctx, j.Image, j.Command, int(j.TimeoutSeconds))
	close(execDone)

	status := string(job.StatusSucceeded)
	if execErr != nil || result.ExitCode != 0 {
		status = string(job.StatusFailed)
	}
	if execErr != nil {
		result.Stderr = result.Stderr + "\n" + execErr.Error()
	}

	_, err = l.client.ReportResult(ctx, &jobv1.ReportResultRequest{
		Id:       j.Id,
		Status:   status,
		Stdout:   result.Stdout,
		Stderr:   result.Stderr,
		ExitCode: int32(result.ExitCode),
	})
	if err != nil {
		return fmt.Errorf("report failed: %w", err)
	}
	slog.Info("job reported", "id", j.Id, "status", status)
	return nil
}

// heartbeatWhileExecuting keeps this worker's liveness fresh while RunOnce is
// blocked inside Execute — PollJob only doubles as a heartbeat between jobs,
// so without this a job running longer than the coordinator's dead-worker
// timeout would make a perfectly healthy worker look dead and get reaped
// mid-execution. Stops as soon as done is closed or ctx is cancelled.
func (l *Loop) heartbeatWhileExecuting(ctx context.Context, done <-chan struct{}) {
	ticker := time.NewTicker(l.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := l.client.Heartbeat(ctx, &jobv1.HeartbeatRequest{WorkerId: l.ID}); err != nil {
				slog.Error("heartbeat failed", "error", err)
			}
		}
	}
}

func (l *Loop) Run(ctx context.Context) {
	ticker := time.NewTicker(l.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := l.RunOnce(ctx); err != nil {
				slog.Error("worker loop iteration failed", "error", err)
			}
		}
	}
}
