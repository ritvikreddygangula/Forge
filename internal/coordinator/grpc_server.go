package coordinator

import (
	"context"
	"errors"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
	"github.com/ritvikreddygangula/forge/internal/job"
)

// GRPCServer implements jobv1.JobServiceServer over the same job.Store the
// REST Server uses — both are thin transports over one shared store.
type GRPCServer struct {
	jobv1.UnimplementedJobServiceServer
	store    job.Store
	raftGate *RaftGate // nil in single-node mode; see raft.go

	mu          sync.Mutex
	leaderConns map[string]*grpc.ClientConn // lazily dialed, keyed by leader gRPC addr
}

func NewGRPCServer(store job.Store) *GRPCServer {
	return &GRPCServer{store: store}
}

// SetRaftGate wires this replica's leader-election state into the server.
// Called only when running as part of a raft cluster (Task 4.2c); a
// GRPCServer with no gate set behaves exactly as it did before this Part —
// every write handler treats a nil gate as "always leader."
func (s *GRPCServer) SetRaftGate(g *RaftGate) { s.raftGate = g }

func toProtoJob(j *job.Job) *jobv1.Job {
	return &jobv1.Job{
		Id:             j.ID,
		Image:          j.Image,
		Command:        j.Command,
		TimeoutSeconds: int32(j.TimeoutSeconds),
	}
}

// leaderClient returns a JobServiceClient dialed against whichever replica
// raft currently says is leader, reusing a cached connection per address so
// a busy follower isn't re-dialing on every forwarded call.
func (s *GRPCServer) leaderClient() (jobv1.JobServiceClient, error) {
	leader, ok := s.raftGate.Leader()
	if !ok {
		return nil, status.Error(codes.Unavailable, "no raft leader elected, try again shortly")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if conn, ok := s.leaderConns[leader.GRPCAddr]; ok {
		return jobv1.NewJobServiceClient(conn), nil
	}
	conn, err := grpc.NewClient(leader.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, status.Error(codes.Unavailable, "failed to connect to leader")
	}
	if s.leaderConns == nil {
		s.leaderConns = make(map[string]*grpc.ClientConn)
	}
	s.leaderConns[leader.GRPCAddr] = conn
	return jobv1.NewJobServiceClient(conn), nil
}

func (s *GRPCServer) PollJob(ctx context.Context, req *jobv1.PollJobRequest) (*jobv1.PollJobResponse, error) {
	if !s.raftGate.IsLeader() {
		client, err := s.leaderClient()
		if err != nil {
			return nil, err
		}
		return client.PollJob(ctx, req)
	}

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
	if !s.raftGate.IsLeader() {
		client, err := s.leaderClient()
		if err != nil {
			return nil, err
		}
		return client.ReportResult(ctx, req)
	}

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

// StreamLogs sends whatever stdout/stderr the store currently holds for a job
// as one or two chunks, then closes the stream. The executor still captures
// output as complete buffers after the job finishes (Part 1 behavior,
// unchanged), so this is REST /jobs/{id}/logs parity, not live tailing —
// there's no incremental capture yet to stream from. Deliberately NOT
// leader-gated, same reasoning as REST's handleGetJob — a read, served from
// this replica's own local (eventually-consistent) copy.
func (s *GRPCServer) StreamLogs(req *jobv1.StreamLogsRequest, stream jobv1.JobService_StreamLogsServer) error {
	j, err := s.store.Get(req.Id)
	if err != nil {
		if errors.Is(err, job.ErrNotFound) {
			return status.Error(codes.NotFound, "job not found")
		}
		return status.Error(codes.Internal, "failed to get job")
	}
	if j.Stdout != "" {
		if err := stream.Send(&jobv1.LogChunk{Stream: "stdout", Data: j.Stdout}); err != nil {
			return err
		}
	}
	if j.Stderr != "" {
		if err := stream.Send(&jobv1.LogChunk{Stream: "stderr", Data: j.Stderr}); err != nil {
			return err
		}
	}
	return nil
}
