package coordinator_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/ritvikreddygangula/forge/internal/coordinator"
)

// raftTestNode bundles one in-memory raft node for tests — no real network,
// no real ports, verified fast and non-flaky for a full 3-node cluster.
type raftTestNode struct {
	id        string
	raft      *raft.Raft
	transport *raft.InmemTransport
}

func newInMemRaftCluster(t *testing.T, n int) []*raftTestNode {
	t.Helper()
	nodes := make([]*raftTestNode, n)
	servers := make([]raft.Server, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("node%d", i+1)
		addr, transport := raft.NewInmemTransport(raft.NewInmemAddr())
		nodes[i] = &raftTestNode{id: id, transport: transport}
		servers[i] = raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(id), Address: addr}
	}
	for i := range nodes {
		for j := range nodes {
			if i != j {
				nodes[i].transport.Connect(nodes[j].transport.LocalAddr(), nodes[j].transport)
			}
		}
	}
	for i, n := range nodes {
		r, err := coordinator.NewRaftNode(coordinator.RaftNodeConfig{
			LocalID:            n.id,
			Transport:          n.transport,
			LogStore:           raft.NewInmemStore(),
			StableStore:        raft.NewInmemStore(),
			SnapshotStore:      raft.NewInmemSnapshotStore(),
			Servers:            servers,
			HeartbeatTimeout:   50 * time.Millisecond,
			ElectionTimeout:    50 * time.Millisecond,
			LeaderLeaseTimeout: 50 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewRaftNode(%s) returned error: %v", n.id, err)
		}
		nodes[i].raft = r
	}
	return nodes
}

func waitForLeader(t *testing.T, nodes []*raftTestNode, timeout time.Duration) *raftTestNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.raft.State() == raft.Leader {
				return n
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

func TestRaft_LeaderElectionAndFailover(t *testing.T) {
	nodes := newInMemRaftCluster(t, 3)
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.raft.Shutdown()
		}
	})

	start := time.Now()
	leader := waitForLeader(t, nodes, 2*time.Second)
	t.Logf("initial leader %s elected in %s", leader.id, time.Since(start))

	if err := leader.raft.Shutdown().Error(); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}

	var remaining []*raftTestNode
	for _, n := range nodes {
		if n.id != leader.id {
			remaining = append(remaining, n)
		}
	}

	start = time.Now()
	newLeader := waitForLeader(t, remaining, 2*time.Second)
	elapsed := time.Since(start)
	t.Logf("re-election after leader crash took %s (new leader %s)", elapsed, newLeader.id)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("failover took %s, expected under 500ms", elapsed)
	}
	if newLeader.id == leader.id {
		t.Fatalf("expected a different node to become leader")
	}
}
