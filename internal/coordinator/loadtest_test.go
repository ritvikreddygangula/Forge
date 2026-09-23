//go:build integration

package coordinator_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
	"github.com/ritvikreddygangula/forge/internal/coordinator"
	"github.com/ritvikreddygangula/forge/internal/eventlog"
	"github.com/ritvikreddygangula/forge/internal/job"
	"github.com/ritvikreddygangula/forge/internal/worker"
)

// loadTestTopic returns a unique topic name per test run, so this test never
// sees jobs left behind by a prior run against the same long-lived dev
// broker (same reasoning as eventlog's own testTopic, package-local here
// since Go doesn't share unexported test helpers across packages).
func loadTestTopic(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("job-events-loadtest-%d", time.Now().UnixNano())
}

// TestLoadTest_RealWorkersRealDocker starts one real coordinator (REST +
// gRPC, real eventlog.Store against real Redpanda) and numWorkers real
// worker.Loops running real docker run execution, submits numJobs real jobs,
// and measures actual wall-clock throughput. No synthetic no-op job type to
// inflate the number — whatever this measures with real container execution
// is what's real (see docs/plans/part-5-scheduling.md's Task 5.6).
func TestLoadTest_RealWorkersRealDocker(t *testing.T) {
	const numWorkers = 25
	const numJobs = 500 // modest and real beats large and synthetic

	brokers := []string{"localhost:9092"}
	topic := loadTestTopic(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := eventlog.EnsureTopic(ctx, brokers, topic); err != nil {
		t.Fatalf("EnsureTopic returned error: %v", err)
	}
	producer := eventlog.NewKafkaProducer(brokers, topic)
	defer func() { _ = producer.Close() }()
	store := eventlog.NewStore(job.NewMemoryStore(), producer)

	registry := coordinator.NewWorkerRegistry()
	grpcServer := coordinator.NewGRPCServer(store)
	grpcServer.SetWorkerRegistry(registry)
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	jobv1.RegisterJobServiceServer(grpcSrv, grpcServer)
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.Stop()

	restSrv := httptest.NewServer(coordinator.NewServer(store))
	defer restSrv.Close()

	// Submit all jobs up front.
	ids := make([]string, numJobs)
	for i := 0; i < numJobs; i++ {
		resp, err := http.Post(restSrv.URL+"/jobs", "application/json",
			strings.NewReader(`{"image":"alpine:3.19","command":["true"],"timeout_seconds":30}`))
		if err != nil {
			t.Fatalf("submit failed: %v", err)
		}
		var got map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
			t.Fatalf("failed to decode submit response: %v", err)
		}
		_ = resp.Body.Close()
		ids[i] = got["id"].(string)
	}

	// workCtx is shared by every worker and is cancelled the moment all jobs
	// are observed complete (see the polling loop below) — NOT after a fixed
	// duration. RunOnce returns nil, not an error, when it polls an empty
	// queue, so without this every worker would spin at full speed re-polling
	// gRPC for the rest of a fixed timeout even after the real work was long
	// done, making "elapsed" measure idle spinning instead of throughput.
	workCtx, workCancel := context.WithCancel(ctx)
	defer workCancel()

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := worker.NewLoop(lis.Addr().String())
			if err != nil {
				t.Errorf("failed to build worker: %v", err)
				return
			}
			defer func() { _ = l.Close() }()
			for workCtx.Err() == nil {
				if err := l.RunOnce(workCtx); err != nil {
					return
				}
			}
		}()
	}

	countCompleted := func() int {
		completed := 0
		for _, id := range ids {
			j, err := store.Get(id)
			if err == nil && j.Status == job.StatusSucceeded {
				completed++
			}
		}
		return completed
	}

	const pollFor = 4 * time.Minute
	deadline := time.Now().Add(pollFor)
	for countCompleted() < numJobs && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	elapsed := time.Since(start)
	workCancel()
	wg.Wait()

	completed := countCompleted()
	rate := float64(completed) / elapsed.Seconds()
	t.Logf("load test: %d/%d jobs completed by %d workers in %s (%.1f jobs/sec)",
		completed, numJobs, numWorkers, elapsed, rate)
	if completed != numJobs {
		t.Fatalf("expected all %d jobs to complete, got %d", numJobs, completed)
	}
}
