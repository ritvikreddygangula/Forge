package worker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
	"github.com/ritvikreddygangula/forge/internal/job"
)

type Loop struct {
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
		client:       jobv1.NewJobServiceClient(conn),
		conn:         conn,
		PollInterval: 2 * time.Second,
		Execute:      RunJob,
	}
}

func (l *Loop) Close() error { return l.conn.Close() }

func (l *Loop) RunOnce(ctx context.Context) error {
	resp, err := l.client.PollJob(ctx, &jobv1.PollJobRequest{})
	if err != nil {
		return fmt.Errorf("poll failed: %w", err)
	}
	if !resp.HasJob {
		return nil
	}
	j := resp.Job

	slog.Info("job claimed", "id", j.Id, "image", j.Image)

	result, execErr := l.Execute(ctx, j.Image, j.Command, int(j.TimeoutSeconds))
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
