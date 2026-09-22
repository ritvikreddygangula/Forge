package coordinator

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"
)

// noopFSM is intentionally empty. This project uses Raft purely for leader
// election among coordinator replicas — job data is already durable and
// shared via the Redpanda event log every replica tails (Part 3, extended to
// a continuous tail in this Part). Raft still requires something satisfying
// FSM for its own log/consensus bookkeeping. See the plan doc's "Why Raft's
// role here is narrow" section for the full reasoning.
type noopFSM struct{}

func (noopFSM) Apply(*raft.Log) interface{}         { return nil }
func (noopFSM) Snapshot() (raft.FSMSnapshot, error) { return noopSnapshot{}, nil }
func (noopFSM) Restore(rc io.ReadCloser) error      { return rc.Close() }

type noopSnapshot struct{}

func (noopSnapshot) Persist(sink raft.SnapshotSink) error { return sink.Close() }
func (noopSnapshot) Release()                             {}

// RaftNodeConfig holds everything needed to construct this replica's
// raft.Raft instance. Tests pass in-memory transport/stores; the real
// coordinator binary passes TCP transport and boltdb-backed stores (Task
// 4.5) — NewRaftNode itself is identical either way.
type RaftNodeConfig struct {
	LocalID       string
	Transport     raft.Transport
	LogStore      raft.LogStore
	StableStore   raft.StableStore
	SnapshotStore raft.SnapshotStore
	Servers       []raft.Server // full cluster membership, including self

	// Tuned for sub-500ms failover on a local/LAN deployment — see the plan
	// doc's "Why the tuned timeouts" section. Zero value on any field falls
	// back to raft.DefaultConfig()'s own default for that field.
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
}

func NewRaftNode(cfg RaftNodeConfig) (*raft.Raft, error) {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.LocalID)
	if cfg.HeartbeatTimeout > 0 {
		raftConfig.HeartbeatTimeout = cfg.HeartbeatTimeout
	}
	if cfg.ElectionTimeout > 0 {
		raftConfig.ElectionTimeout = cfg.ElectionTimeout
	}
	if cfg.LeaderLeaseTimeout > 0 {
		raftConfig.LeaderLeaseTimeout = cfg.LeaderLeaseTimeout
	}

	r, err := raft.NewRaft(raftConfig, noopFSM{}, cfg.LogStore, cfg.StableStore, cfg.SnapshotStore, cfg.Transport)
	if err != nil {
		return nil, fmt.Errorf("failed to construct raft node: %w", err)
	}

	// Safe to call from every replica on every startup — hashicorp/raft only
	// actually seeds the cluster once, ever; every later call (including on
	// every restart of any replica) returns ErrCantBootstrap, meaning
	// "already bootstrapped," not a real failure.
	future := r.BootstrapCluster(raft.Configuration{Servers: cfg.Servers})
	if err := future.Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
		return nil, fmt.Errorf("failed to bootstrap raft cluster: %w", err)
	}

	return r, nil
}
