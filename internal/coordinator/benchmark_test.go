package coordinator_test

import (
	"sort"
	"testing"
	"time"
)

// TestRaft_FailoverBenchmark runs the process-kill failover scenario 30
// times back to back, recording each trial's re-election latency, then
// reports median/p99 and the pass count against the 500ms target. Per this
// project's resume-honesty rule (CLAUDE.md): whatever this measures is what
// goes in PROGRESS.md — never a number written down before it's been run.
func TestRaft_FailoverBenchmark(t *testing.T) {
	const trials = 30
	latencies := make([]time.Duration, 0, trials)

	for i := 0; i < trials; i++ {
		func() {
			nodes := newInMemRaftCluster(t, 3)
			defer func() {
				for _, n := range nodes {
					_ = n.raft.Shutdown()
				}
			}()

			leader := waitForLeader(t, nodes, 2*time.Second)
			var remaining []*raftTestNode
			for _, n := range nodes {
				if n.id != leader.id {
					remaining = append(remaining, n)
				}
			}

			start := time.Now()
			_ = leader.raft.Shutdown()
			waitForLeader(t, remaining, 2*time.Second)
			latencies = append(latencies, time.Since(start))
		}()
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	median := latencies[len(latencies)/2]
	p99 := latencies[int(float64(len(latencies))*0.99)]
	passCount := 0
	for _, l := range latencies {
		if l <= 500*time.Millisecond {
			passCount++
		}
	}
	t.Logf("failover benchmark: %d trials, median=%s, p99=%s, min=%s, max=%s, %d/%d under 500ms",
		trials, median, p99, latencies[0], latencies[len(latencies)-1], passCount, trials)

	if passCount != trials {
		t.Fatalf("expected all %d trials under 500ms, got %d", trials, passCount)
	}
}
