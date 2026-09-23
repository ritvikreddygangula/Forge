package coordinator_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
	"github.com/ritvikreddygangula/forge/internal/coordinator"
	"github.com/ritvikreddygangula/forge/internal/job"
)

func dialGRPCServer(t *testing.T, store job.Store) jobv1.JobServiceClient {
	t.Helper()
	return dialGRPCServerInstance(t, coordinator.NewGRPCServer(store))
}

func dialGRPCServerInstance(t *testing.T, grpcServer *coordinator.GRPCServer) jobv1.JobServiceClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	jobv1.RegisterJobServiceServer(srv, grpcServer)
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

func TestGRPC_PollJob_RecordsHeartbeat(t *testing.T) {
	store := job.NewMemoryStore()
	registry := coordinator.NewWorkerRegistry()
	grpcServer := coordinator.NewGRPCServer(store)
	grpcServer.SetWorkerRegistry(registry)
	client := dialGRPCServerInstance(t, grpcServer)

	if _, err := client.PollJob(context.Background(), &jobv1.PollJobRequest{WorkerId: "worker-1"}); err != nil {
		t.Fatalf("PollJob returned error: %v", err)
	}

	// DeadWorkers with a zero timeout flags every KNOWN worker, regardless
	// of how fresh — this is what distinguishes "heartbeated at all" from
	// "never heard from" (which DeadWorkers never flags, by design). A
	// generous timeout here would pass vacuously even if PollJob never
	// recorded anything.
	dead := registry.DeadWorkers(0, time.Now())
	if len(dead) != 1 || dead[0] != "worker-1" {
		t.Fatalf("expected worker-1 to be a known (heartbeated) worker, got %v", dead)
	}
}

func TestGRPC_Heartbeat_RecordsHeartbeat(t *testing.T) {
	store := job.NewMemoryStore()
	registry := coordinator.NewWorkerRegistry()
	grpcServer := coordinator.NewGRPCServer(store)
	grpcServer.SetWorkerRegistry(registry)
	client := dialGRPCServerInstance(t, grpcServer)

	if _, err := client.Heartbeat(context.Background(), &jobv1.HeartbeatRequest{WorkerId: "worker-1"}); err != nil {
		t.Fatalf("Heartbeat returned error: %v", err)
	}

	// Zero timeout flags any KNOWN worker regardless of freshness — the same
	// vacuous-test trap as TestGRPC_PollJob_RecordsHeartbeat: a generous
	// timeout would pass even if Heartbeat never recorded anything.
	dead := registry.DeadWorkers(0, time.Now())
	if len(dead) != 1 || dead[0] != "worker-1" {
		t.Fatalf("expected worker-1 to be a known (heartbeated) worker, got %v", dead)
	}
}

func TestGRPC_Heartbeat_ForwardsToLeaderWhenNotLeader(t *testing.T) {
	nodes := newInMemRaftCluster(t, 2)
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.raft.Shutdown()
		}
	})
	leader := waitForLeader(t, nodes, 2*time.Second)

	type nodeServer struct {
		registry *coordinator.WorkerRegistry
		grpcSrv  *coordinator.GRPCServer
		addr     string
	}
	servers := make(map[string]*nodeServer, len(nodes))
	peers := make([]coordinator.PeerInfo, 0, len(nodes))
	for _, n := range nodes {
		registry := coordinator.NewWorkerRegistry()
		grpcServer := coordinator.NewGRPCServer(job.NewMemoryStore())
		grpcServer.SetWorkerRegistry(registry)
		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			t.Fatalf("failed to listen: %v", err)
		}
		s := grpc.NewServer()
		jobv1.RegisterJobServiceServer(s, grpcServer)
		go func() { _ = s.Serve(lis) }()
		t.Cleanup(s.Stop)

		servers[n.id] = &nodeServer{registry: registry, grpcSrv: grpcServer, addr: lis.Addr().String()}
		peers = append(peers, coordinator.PeerInfo{
			ID:       n.id,
			RaftAddr: string(n.transport.LocalAddr()),
			GRPCAddr: lis.Addr().String(),
		})
	}
	for _, n := range nodes {
		servers[n.id].grpcSrv.SetRaftGate(coordinator.NewRaftGate(n.raft, peers))
	}

	var followerID string
	for _, n := range nodes {
		if n.id != leader.id {
			followerID = n.id
		}
	}

	conn, err := grpc.NewClient(servers[followerID].addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial follower: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client := jobv1.NewJobServiceClient(conn)

	if _, err := client.Heartbeat(context.Background(), &jobv1.HeartbeatRequest{WorkerId: "worker-1"}); err != nil {
		t.Fatalf("Heartbeat returned error: %v", err)
	}

	dead := servers[leader.id].registry.DeadWorkers(0, time.Now())
	if len(dead) != 1 || dead[0] != "worker-1" {
		t.Fatalf("expected the leader's registry to record the forwarded heartbeat, got %v", dead)
	}
	if dead := servers[followerID].registry.DeadWorkers(0, time.Now()); len(dead) != 0 {
		t.Fatalf("expected the follower's own registry untouched, got %v", dead)
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
	if _, err := store.ClaimNext("worker-1"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
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

// TestGRPC_PollJob_ForwardsToLeaderWhenNotLeader uses a real 2-node
// in-memory raft cluster plus real TCP-backed gRPC servers — no mocked raft,
// no mocked gRPC. Polls against whichever node is NOT the leader and proves
// the leader's queued job gets claimed via forwarding, not fabricated
// locally by the follower.
func TestGRPC_PollJob_ForwardsToLeaderWhenNotLeader(t *testing.T) {
	nodes := newInMemRaftCluster(t, 2)
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.raft.Shutdown()
		}
	})
	leader := waitForLeader(t, nodes, 2*time.Second)

	type nodeServer struct {
		store   *job.MemoryStore
		grpcSrv *coordinator.GRPCServer
		addr    string
	}
	servers := make(map[string]*nodeServer, len(nodes))
	peers := make([]coordinator.PeerInfo, 0, len(nodes))
	for _, n := range nodes {
		store := job.NewMemoryStore()
		grpcServer := coordinator.NewGRPCServer(store)
		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			t.Fatalf("failed to listen: %v", err)
		}
		s := grpc.NewServer()
		jobv1.RegisterJobServiceServer(s, grpcServer)
		go func() { _ = s.Serve(lis) }()
		t.Cleanup(s.Stop)

		servers[n.id] = &nodeServer{store: store, grpcSrv: grpcServer, addr: lis.Addr().String()}
		peers = append(peers, coordinator.PeerInfo{
			ID:       n.id,
			RaftAddr: string(n.transport.LocalAddr()),
			GRPCAddr: lis.Addr().String(),
		})
	}
	for _, n := range nodes {
		servers[n.id].grpcSrv.SetRaftGate(coordinator.NewRaftGate(n.raft, peers))
	}

	created, err := servers[leader.id].store.Create("alpine", []string{"true"}, 10)
	if err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	var followerID string
	for _, n := range nodes {
		if n.id != leader.id {
			followerID = n.id
		}
	}

	conn, err := grpc.NewClient(servers[followerID].addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial follower: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client := jobv1.NewJobServiceClient(conn)

	resp, err := client.PollJob(context.Background(), &jobv1.PollJobRequest{})
	if err != nil {
		t.Fatalf("PollJob returned error: %v", err)
	}
	if !resp.HasJob || resp.Job.Id != created.ID {
		t.Fatalf("expected to poll the leader's queued job %s, got %+v", created.ID, resp)
	}

	got, err := servers[leader.id].store.Get(created.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusRunning {
		t.Fatalf("expected leader's job to be running after forwarded claim, got %s", got.Status)
	}
}

func TestGRPC_StreamLogs(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	if _, err := store.ClaimNext("worker-1"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
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
