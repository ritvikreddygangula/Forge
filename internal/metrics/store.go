// Package metrics wraps job.Store and worker execution with Prometheus
// metrics, recorded into the default registry via promauto.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ritvikreddygangula/forge/internal/job"
)

var (
	JobsCreatedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_created_total", Help: "Total number of jobs submitted.",
	})
	JobsCompletedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jobs_completed_total", Help: "Total number of jobs completed, by final status.",
	}, []string{"status"})
	JobsCancelledTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_cancelled_total", Help: "Total number of jobs cancelled while queued.",
	})
	JobsReassignedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_reassigned_total", Help: "Total number of jobs reassigned away from a dead worker.",
	})
	JobDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "job_duration_seconds", Help: "End-to-end job duration from creation to completion.",
		Buckets: prometheus.DefBuckets,
	})

	WorkerJobsExecutedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "worker_jobs_executed_total", Help: "Total number of jobs this worker has executed, by outcome.",
	}, []string{"status"})
	WorkerExecutionDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "worker_execution_duration_seconds", Help: "Real docker run execution duration.",
		Buckets: prometheus.DefBuckets,
	})
)

// Store wraps a job.Store and records Prometheus metrics after each
// successful mutation — the same decorator shape eventlog.Store already
// established for publishing events. See docs/plans/part-8-observability.md
// for why only the raft leader's counters move in cluster mode.
type Store struct {
	inner job.Store
}

func NewStore(inner job.Store) *Store { return &Store{inner: inner} }

func (s *Store) Create(image string, command []string, timeoutSeconds int) (*job.Job, error) {
	j, err := s.inner.Create(image, command, timeoutSeconds)
	if err != nil {
		return nil, err
	}
	JobsCreatedTotal.Inc()
	return j, nil
}

func (s *Store) Get(id string) (*job.Job, error) { return s.inner.Get(id) }

func (s *Store) ClaimNext(workerID string) (*job.Job, error) { return s.inner.ClaimNext(workerID) }

func (s *Store) Complete(id string, status job.Status, stdout, stderr string, exitCode int) error {
	before, getErr := s.inner.Get(id)
	if err := s.inner.Complete(id, status, stdout, stderr, exitCode); err != nil {
		return err
	}
	JobsCompletedTotal.WithLabelValues(string(status)).Inc()
	if getErr == nil {
		JobDurationSeconds.Observe(time.Since(before.CreatedAt).Seconds())
	}
	return nil
}

func (s *Store) RequeueRunning(workerID string) ([]*job.Job, error) {
	requeued, err := s.inner.RequeueRunning(workerID)
	if err == nil {
		JobsReassignedTotal.Add(float64(len(requeued)))
	}
	return requeued, err
}

func (s *Store) Cancel(id string) (*job.Job, error) {
	j, err := s.inner.Cancel(id)
	if err != nil {
		return nil, err
	}
	JobsCancelledTotal.Inc()
	return j, nil
}
