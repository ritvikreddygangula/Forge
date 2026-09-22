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

func (s *Store) ClaimNext() (*job.Job, error) {
	j, err := s.inner.ClaimNext()
	if err != nil || j == nil {
		return j, err
	}
	if err := s.producer.Publish(context.Background(), Event{
		Type: EventJobClaimed, JobID: j.ID, Timestamp: time.Now(),
	}); err != nil {
		return nil, fmt.Errorf("job claimed but failed to publish event: %w", err)
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
