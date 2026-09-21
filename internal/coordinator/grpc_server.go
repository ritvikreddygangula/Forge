package coordinator

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
	"github.com/ritvikreddygangula/forge/internal/job"
)

// GRPCServer implements jobv1.JobServiceServer over the same job.Store the
// REST Server uses — both are thin transports over one shared store.
type GRPCServer struct {
	jobv1.UnimplementedJobServiceServer
	store job.Store
}

func NewGRPCServer(store job.Store) *GRPCServer {
	return &GRPCServer{store: store}
}

func toProtoJob(j *job.Job) *jobv1.Job {
	return &jobv1.Job{
		Id:             j.ID,
		Image:          j.Image,
		Command:        j.Command,
		TimeoutSeconds: int32(j.TimeoutSeconds),
	}
}

func (s *GRPCServer) PollJob(ctx context.Context, req *jobv1.PollJobRequest) (*jobv1.PollJobResponse, error) {
	j, err := s.store.ClaimNext()
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to claim job")
	}
	if j == nil {
		return &jobv1.PollJobResponse{HasJob: false}, nil
	}
	return &jobv1.PollJobResponse{HasJob: true, Job: toProtoJob(j)}, nil
}

func (s *GRPCServer) ReportResult(ctx context.Context, req *jobv1.ReportResultRequest) (*jobv1.ReportResultResponse, error) {
	st := job.Status(req.Status)
	if st != job.StatusSucceeded && st != job.StatusFailed {
		return nil, status.Error(codes.InvalidArgument, "status must be succeeded or failed")
	}

	if err := s.store.Complete(req.Id, st, req.Stdout, req.Stderr, int(req.ExitCode)); err != nil {
		if errors.Is(err, job.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "job not found")
		}
		return nil, status.Error(codes.Internal, "failed to complete job")
	}
	return &jobv1.ReportResultResponse{}, nil
}
