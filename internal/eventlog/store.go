package eventlog

import (
	"context"
	"fmt"
	"time"

	"github.com/ritvikreddygangula/forge/internal/job"
)

// Store wraps a job.Store and publishes an event after each successful
// mutation. The apply-then-publish order means a publish failure surfaces as
// an error from the call, but doesn't roll back the in-memory mutation that
// already happened — see the plan doc's "known limitation" note.
type Store struct {
	inner    job.Store
	producer Producer
}

func NewStore(inner job.Store, producer Producer) *Store {
	return &Store{inner: inner, producer: producer}
}

func (s *Store) Create(image string, command []string, timeoutSeconds int) (*job.Job, error) {
	j, err := s.inner.Create(image, command, timeoutSeconds)
	if err != nil {
		return nil, err
	}
	if err := s.producer.Publish(context.Background(), Event{
		Type: EventJobCreated, JobID: j.ID, Image: j.Image, Command: j.Command,
		TimeoutSeconds: j.TimeoutSeconds, Timestamp: time.Now(),
	}); err != nil {
		return nil, fmt.Errorf("job created but failed to publish event: %w", err)
	}
	return j, nil
}

func (s *Store) Get(id string) (*job.Job, error) {
	return s.inner.Get(id) // pure read — not a state transition, nothing to publish
}

func (s *Store) ClaimNext(workerID string) (*job.Job, error) {
	j, err := s.inner.ClaimNext(workerID)
	if err != nil || j == nil {
		return j, err
	}
	if err := s.producer.Publish(context.Background(), Event{
		Type: EventJobClaimed, JobID: j.ID, WorkerID: workerID, Timestamp: time.Now(),
	}); err != nil {
		return nil, fmt.Errorf("job claimed but failed to publish event: %w", err)
	}
	return j, nil
}

// RequeueRunning requeues in-memory first, then publishes one event per
// requeued job. Same known limitation as every other mutation here: a
// publish failure after the local requeue already happened isn't rolled
// back — the caller (the Reaper) gets an error and can retry on its next
// tick, since the affected job is already visible as queued locally either way.
func (s *Store) RequeueRunning(workerID string) ([]*job.Job, error) {
	requeued, err := s.inner.RequeueRunning(workerID)
	if err != nil {
		return nil, err
	}
	for _, j := range requeued {
		if pubErr := s.producer.Publish(context.Background(), Event{
			Type: EventJobRequeued, JobID: j.ID, Timestamp: time.Now(),
		}); pubErr != nil {
			return requeued, fmt.Errorf("jobs requeued locally but failed to publish for %s: %w", j.ID, pubErr)
		}
	}
	return requeued, nil
}

// Cancel rejects (no publish) if the inner store rejects it — a running or
// terminal job's rejection is a pure read from the caller's perspective, not
// a state transition, so nothing goes on the event log for it. Same
// apply-then-publish limitation as every other mutation here for the
// success path.
func (s *Store) Cancel(id string) (*job.Job, error) {
	j, err := s.inner.Cancel(id)
	if err != nil {
		return nil, err
	}
	if pubErr := s.producer.Publish(context.Background(), Event{
		Type: EventJobCancelled, JobID: id, Timestamp: time.Now(),
	}); pubErr != nil {
		return j, fmt.Errorf("job cancelled locally but failed to publish: %w", pubErr)
	}
	return j, nil
}

func (s *Store) Complete(id string, status job.Status, stdout, stderr string, exitCode int) error {
	if err := s.inner.Complete(id, status, stdout, stderr, exitCode); err != nil {
		return err
	}
	if err := s.producer.Publish(context.Background(), Event{
		Type: EventJobCompleted, JobID: id, Status: status, Stdout: stdout, Stderr: stderr,
		ExitCode: exitCode, Timestamp: time.Now(),
	}); err != nil {
		return fmt.Errorf("job completed but failed to publish event: %w", err)
	}
	return nil
}
