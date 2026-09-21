package coordinator_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
	"github.com/ritvikreddygangula/forge/internal/coordinator"
	"github.com/ritvikreddygangula/forge/internal/job"
)

func dialGRPCServer(t *testing.T, store job.Store) jobv1.JobServiceClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	jobv1.RegisterJobServiceServer(srv, coordinator.NewGRPCServer(store))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return jobv1.NewJobServiceClient(conn)
}

func TestGRPC_PollJob_EmptyQueue(t *testing.T) {
	client := dialGRPCServer(t, job.NewMemoryStore())

	resp, err := client.PollJob(context.Background(), &jobv1.PollJobRequest{})
	if err != nil {
		t.Fatalf("PollJob returned error: %v", err)
	}
	if resp.HasJob {
		t.Fatalf("expected has_job=false on empty queue, got true")
	}
}

func TestGRPC_PollJob_ClaimsQueuedJob(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	client := dialGRPCServer(t, store)

	resp, err := client.PollJob(context.Background(), &jobv1.PollJobRequest{})
	if err != nil {
		t.Fatalf("PollJob returned error: %v", err)
	}
	if !resp.HasJob {
		t.Fatal("expected has_job=true, got false")
	}
	if resp.Job.Id != created.ID {
		t.Fatalf("expected claimed job id %s, got %s", created.ID, resp.Job.Id)
	}
}

func TestGRPC_ReportResult_Success(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	store.ClaimNext()
	client := dialGRPCServer(t, store)

	_, err := client.ReportResult(context.Background(), &jobv1.ReportResultRequest{
		Id: created.ID, Status: "succeeded", Stdout: "ok\n", ExitCode: 0,
	})
	if err != nil {
		t.Fatalf("ReportResult returned error: %v", err)
	}

	got, _ := store.Get(created.ID)
	if got.Status != job.StatusSucceeded {
		t.Fatalf("expected status succeeded, got %s", got.Status)
	}
}

func TestGRPC_ReportResult_InvalidStatus(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	client := dialGRPCServer(t, store)

	_, err := client.ReportResult(context.Background(), &jobv1.ReportResultRequest{
		Id: created.ID, Status: "bogus",
	})
	if err == nil {
		t.Fatal("expected an error for an invalid status, got nil")
	}
}

func TestGRPC_ReportResult_NotFound(t *testing.T) {
	client := dialGRPCServer(t, job.NewMemoryStore())

	_, err := client.ReportResult(context.Background(), &jobv1.ReportResultRequest{
		Id: "does-not-exist", Status: "succeeded",
	})
	if err == nil {
		t.Fatal("expected an error for an unknown job id, got nil")
	}
}

func TestGRPC_StreamLogs(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	store.ClaimNext()
	if err := store.Complete(created.ID, job.StatusSucceeded, "hello\n", "", 0); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	client := dialGRPCServer(t, store)

	stream, err := client.StreamLogs(context.Background(), &jobv1.StreamLogsRequest{Id: created.ID})
	if err != nil {
		t.Fatalf("StreamLogs returned error: %v", err)
	}

	var chunks []*jobv1.LogChunk
	for {
		chunk, err := stream.Recv()
		if err != nil {
			break // io.EOF ends the stream; any other error fails via the chunk assertion below
		}
		chunks = append(chunks, chunk)
	}
	if len(chunks) != 1 || chunks[0].Data != "hello\n" || chunks[0].Stream != "stdout" {
		t.Fatalf("expected one stdout chunk %q, got %+v", "hello\n", chunks)
	}
}

func TestGRPC_StreamLogs_NotFound(t *testing.T) {
	client := dialGRPCServer(t, job.NewMemoryStore())

	stream, err := client.StreamLogs(context.Background(), &jobv1.StreamLogsRequest{Id: "does-not-exist"})
	if err != nil {
		t.Fatalf("StreamLogs returned error: %v", err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("expected an error receiving from the stream for an unknown job id, got nil")
	}
}
