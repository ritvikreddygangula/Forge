package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
	"github.com/ritvikreddygangula/forge/internal/coordinator"
	"github.com/ritvikreddygangula/forge/internal/eventlog"
	"github.com/ritvikreddygangula/forge/internal/job"
)

func main() {
	httpAddr := os.Getenv("COORDINATOR_ADDR")
	if httpAddr == "" {
		httpAddr = ":8080"
	}
	grpcAddr := os.Getenv("COORDINATOR_GRPC_ADDR")
	if grpcAddr == "" {
		grpcAddr = ":9090"
	}
	brokersEnv := os.Getenv("REDPANDA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "localhost:9092"
	}
	brokers := strings.Split(brokersEnv, ",")

	// ctx lives for the whole process (cancelled on SIGINT/SIGTERM) — the
	// cluster-mode tail loop below runs for the process's entire lifetime and
	// must not share a short-lived startup deadline. startupCtx is a bounded
	// child used only for the one-time replay below.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startupCtx, cancelStartup := context.WithTimeout(ctx, 15*time.Second)
	defer cancelStartup()

	if err := eventlog.EnsureTopic(startupCtx, brokers, eventlog.DefaultTopic); err != nil {
		slog.Error("coordinator failed to reach Redpanda", "error", err)
		os.Exit(1)
	}

	events, replayOffset, err := eventlog.NewKafkaConsumer(brokers, eventlog.DefaultTopic).ReadAll(startupCtx)
	if err != nil {
		slog.Error("coordinator failed to replay event log", "error", err)
		os.Exit(1)
	}
	rebuiltJobs := eventlog.Rebuild(events)

	baseStore := job.NewMemoryStore()
	baseStore.Rebuild(rebuiltJobs)
	slog.Info("coordinator replayed event log", "jobs_restored", len(rebuiltJobs))

	producer := eventlog.NewKafkaProducer(brokers, eventlog.DefaultTopic)
	store := eventlog.NewStore(baseStore, producer)

	// Cluster mode is opt-in via COORDINATOR_REPLICA_ID. Unset, and every
	// single-instance behavior from Parts 1-3 is unchanged: no raft node, no
	// continuous tail (nothing else is writing to the log to catch up on),
	// no leader gating.
	replicaID := os.Getenv("COORDINATOR_REPLICA_ID")
	var raftGate *coordinator.RaftGate

	if replicaID != "" {
		clusterConfigPath := os.Getenv("COORDINATOR_CLUSTER_CONFIG")
		if clusterConfigPath == "" {
			clusterConfigPath = "deploy/raft-cluster.json"
		}
		data, err := os.ReadFile(clusterConfigPath)
		if err != nil {
			slog.Error("coordinator failed to read cluster config", "error", err)
			os.Exit(1)
		}
		var peers []coordinator.PeerInfo
		if err := json.Unmarshal(data, &peers); err != nil {
			slog.Error("coordinator failed to parse cluster config", "error", err)
			os.Exit(1)
		}

		var self coordinator.PeerInfo
		var found bool
		servers := make([]raft.Server, 0, len(peers))
		for _, p := range peers {
			servers = append(servers, raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.RaftAddr)})
			if p.ID == replicaID {
				self, found = p, true
			}
		}
		if !found {
			slog.Error("coordinator replica id not found in cluster config", "replica_id", replicaID)
			os.Exit(1)
		}
		httpAddr, grpcAddr = self.RESTAddr, self.GRPCAddr

		transport, err := raft.NewTCPTransport(self.RaftAddr, nil, 3, 5*time.Second, os.Stderr)
		if err != nil {
			slog.Error("coordinator failed to create raft transport", "error", err)
			os.Exit(1)
		}

		r, err := coordinator.NewRaftNode(coordinator.RaftNodeConfig{
			LocalID:            replicaID,
			Transport:          transport,
			LogStore:           raft.NewInmemStore(),
			StableStore:        raft.NewInmemStore(),
			SnapshotStore:      raft.NewInmemSnapshotStore(),
			Servers:            servers,
			HeartbeatTimeout:   50 * time.Millisecond,
			ElectionTimeout:    50 * time.Millisecond,
			LeaderLeaseTimeout: 50 * time.Millisecond,
		})
		if err != nil {
			slog.Error("coordinator failed to start raft node", "error", err)
			os.Exit(1)
		}
		raftGate = coordinator.NewRaftGate(r, peers)

		go func() {
			consumer := eventlog.NewKafkaConsumer(brokers, eventlog.DefaultTopic)
			if err := consumer.Tail(ctx, replayOffset, func(e eventlog.Event) error {
				eventlog.ApplyEvent(baseStore, e)
				return nil
			}); err != nil {
				slog.Error("coordinator tail loop exited", "error", err)
			}
		}()

		slog.Info("coordinator raft node started", "replica_id", replicaID, "raft_addr", self.RaftAddr)
	}

	grpcLis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		slog.Error("coordinator failed to listen (gRPC)", "error", err)
		os.Exit(1)
	}
	grpcServer := coordinator.NewGRPCServer(store)
	grpcServer.SetRaftGate(raftGate)
	grpcSrv := grpc.NewServer()
	jobv1.RegisterJobServiceServer(grpcSrv, grpcServer)
	go func() {
		slog.Info("coordinator gRPC starting", "addr", grpcAddr)
		if err := grpcSrv.Serve(grpcLis); err != nil {
			slog.Error("coordinator gRPC exited", "error", err)
			os.Exit(1)
		}
	}()

	httpSrv := coordinator.NewServer(store)
	httpSrv.SetRaftGate(raftGate)
	slog.Info("coordinator HTTP starting", "addr", httpAddr)
	if err := http.ListenAndServe(httpAddr, httpSrv); err != nil {
		slog.Error("coordinator HTTP exited", "error", err)
		os.Exit(1)
	}
}
