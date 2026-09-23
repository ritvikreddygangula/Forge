package metrics_test

import (
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ritvikreddygangula/forge/internal/job"
	"github.com/ritvikreddygangula/forge/internal/metrics"
)

// histogramSampleCount reads the total number of observations a histogram
// has recorded so far — testutil.CollectAndCount counts metric *series*
// (always 1 for a histogram), not observations, so it can't tell us this.
func histogramSampleCount(t *testing.T) uint64 {
	t.Helper()
	var m dto.Metric
	if err := metrics.JobDurationSeconds.Write(&m); err != nil {
		t.Fatalf("failed to write histogram metric: %v", err)
	}
	return m.GetHistogram().GetSampleCount()
}

func TestMetricsStore_Create_IncrementsJobsCreatedTotal(t *testing.T) {
	inner := job.NewMemoryStore()
	s := metrics.NewStore(inner)
	before := testutil.ToFloat64(metrics.JobsCreatedTotal)

	if _, err := s.Create("alpine", []string{"true"}, 10); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}

	if after := testutil.ToFloat64(metrics.JobsCreatedTotal); after != before+1 {
		t.Fatalf("expected jobs_created_total to increment by 1, got %v -> %v", before, after)
	}
}

func TestMetricsStore_Complete_IncrementsJobsCompletedTotalByStatus(t *testing.T) {
	inner := job.NewMemoryStore()
	s := metrics.NewStore(inner)
	created, _ := s.Create("alpine", []string{"true"}, 10)
	if _, err := inner.ClaimNext("worker-1"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	before := testutil.ToFloat64(metrics.JobsCompletedTotal.WithLabelValues("succeeded"))

	if err := s.Complete(created.ID, job.StatusSucceeded, "ok\n", "", 0); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}

	if after := testutil.ToFloat64(metrics.JobsCompletedTotal.WithLabelValues("succeeded")); after != before+1 {
		t.Fatalf("expected jobs_completed_total{status=succeeded} to increment by 1, got %v -> %v", before, after)
	}
}

func TestMetricsStore_Cancel_IncrementsJobsCancelledTotal(t *testing.T) {
	inner := job.NewMemoryStore()
	s := metrics.NewStore(inner)
	created, _ := s.Create("alpine", []string{"true"}, 10)
	before := testutil.ToFloat64(metrics.JobsCancelledTotal)

	if _, err := s.Cancel(created.ID); err != nil {
		t.Fatalf("Cancel returned error: %v", err)
	}

	if after := testutil.ToFloat64(metrics.JobsCancelledTotal); after != before+1 {
		t.Fatalf("expected jobs_cancelled_total to increment by 1, got %v -> %v", before, after)
	}
}

func TestMetricsStore_Cancel_RejectedCancelDoesNotIncrement(t *testing.T) {
	inner := job.NewMemoryStore()
	s := metrics.NewStore(inner)
	created, _ := s.Create("alpine", []string{"true"}, 10)
	if _, err := inner.ClaimNext("worker-1"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	before := testutil.ToFloat64(metrics.JobsCancelledTotal)

	if _, err := s.Cancel(created.ID); err == nil {
		t.Fatal("expected an error cancelling a running job")
	}

	if after := testutil.ToFloat64(metrics.JobsCancelledTotal); after != before {
		t.Fatalf("expected no increment for a rejected cancel, got %v -> %v", before, after)
	}
}

func TestMetricsStore_RequeueRunning_IncrementsJobsReassignedTotal(t *testing.T) {
	inner := job.NewMemoryStore()
	s := metrics.NewStore(inner)
	if _, err := inner.Create("alpine", []string{"true"}, 10); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	if _, err := inner.ClaimNext("dead-worker"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	before := testutil.ToFloat64(metrics.JobsReassignedTotal)

	if _, err := s.RequeueRunning("dead-worker"); err != nil {
		t.Fatalf("RequeueRunning returned error: %v", err)
	}

	if after := testutil.ToFloat64(metrics.JobsReassignedTotal); after != before+1 {
		t.Fatalf("expected jobs_reassigned_total to increment by 1, got %v -> %v", before, after)
	}
}

func TestMetricsStore_Complete_ObservesJobDuration(t *testing.T) {
	inner := job.NewMemoryStore()
	s := metrics.NewStore(inner)
	created, _ := s.Create("alpine", []string{"true"}, 10)
	if _, err := inner.ClaimNext("worker-1"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	beforeCount := histogramSampleCount(t)

	if err := s.Complete(created.ID, job.StatusSucceeded, "ok\n", "", 0); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}

	if after := histogramSampleCount(t); after != beforeCount+1 {
		t.Fatalf("expected one new job_duration_seconds observation, got %d -> %d", beforeCount, after)
	}
}
