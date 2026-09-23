package coordinator_test

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// TestRaft_NetworkPartition_MajorityElectsNewLeader isolates the leader from
// both followers (bidirectionally, via InmemTransport.Disconnect) while
// leaving the two followers connected to each other. The majority side
// should elect a new leader; the isolated old leader shouldn't, since it can
// no longer reach a quorum of the 3-node cluster.
func TestRaft_NetworkPartition_MajorityElectsNewLeader(t *testing.T) {
	nodes := newInMemRaftCluster(t, 3)
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.raft.Shutdown()
		}
	})

	leader := waitForLeader(t, nodes, 2*time.Second)

	var majority []*raftTestNode
	for _, n := range nodes {
		if n.id != leader.id {
			majority = append(majority, n)
		}
	}

	// Partition: cut the leader off from both followers, in both directions
	// — the majority side (the two followers, still connected to each
	// other) should elect a new leader; the isolated old leader shouldn't.
	for _, other := range majority {
		leader.transport.Disconnect(other.transport.LocalAddr())
		other.transport.Disconnect(leader.transport.LocalAddr())
	}

	start := time.Now()
	newLeader := waitForLeader(t, majority, 2*time.Second)
	t.Logf("majority-side re-election after partition took %s (new leader %s)", time.Since(start), newLeader.id)

	// Give the isolated old leader the same window to (wrongly) claim
	// leadership — it can't reach a quorum, so it shouldn't be able to.
	time.Sleep(300 * time.Millisecond)
	if leader.raft.State() == raft.Leader {
		t.Fatalf("expected the isolated old leader to step down, still reports Leader")
	}
}
