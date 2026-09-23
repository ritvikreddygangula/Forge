package job

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

var ErrNotFound = errors.New("job not found")

type Store interface {
	Create(image string, command []string, timeoutSeconds int) (*Job, error)
	Get(id string) (*Job, error)
	ClaimNext() (*Job, error)
	Complete(id string, status Status, stdout, stderr string, exitCode int) error
}

type MemoryStore struct {
	mu    sync.Mutex
	jobs  map[string]*Job
	order []string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{jobs: make(map[string]*Job)}
}

func (s *MemoryStore) Create(image string, command []string, timeoutSeconds int) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	j := &Job{
		ID:             uuid.NewString(),
		Image:          image,
		Command:        command,
		TimeoutSeconds: timeoutSeconds,
		Status:         StatusQueued,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	s.jobs[j.ID] = j
	s.order = append(s.order, j.ID)

	cp := *j
	return &cp, nil
}

func (s *MemoryStore) Get(id string) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *j
	return &cp, nil
}

func (s *MemoryStore) ClaimNext() (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, id := range s.order {
		j := s.jobs[id]
		if j.Status == StatusQueued {
			j.Status = StatusRunning
			j.UpdatedAt = time.Now()
			cp := *j
			return &cp, nil
		}
	}
	return nil, nil
}

// ApplyCreated inserts a job as queued. Used by a replica's continuous
// event-log tail (Part 4) to replicate a JobCreated event — unlike Create,
// the ID already exists (assigned by whichever replica was leader when the
// job was created), so no new ID is generated here. A no-op if this ID is
// already known, since a job's own leader-side write reaches this same
// store's tail loop moments after Create already applied it directly.
func (s *MemoryStore) ApplyCreated(j *Job) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[j.ID]; exists {
		return
	}
	cp := *j
	s.jobs[j.ID] = &cp
	if j.Status == StatusQueued {
		s.order = append(s.order, j.ID)
	}
}

// ApplyClaimed marks a specific job running. Unlike ClaimNext, the job ID is
// already decided (by whichever replica was leader when the claim happened)
// — this just replicates that decision. A no-op if the job is unknown or
// already past queued, so replaying an already-applied event is harmless.
func (s *MemoryStore) ApplyClaimed(id string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok || j.Status != StatusQueued {
		return
	}
	j.Status = StatusRunning
	j.UpdatedAt = at
}

// ApplyCompleted marks a specific job terminal. Same replication role as
// ApplyClaimed, for the completed transition.
func (s *MemoryStore) ApplyCompleted(id string, status Status, stdout, stderr string, exitCode int, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return
	}
	j.Status = status
	j.Stdout = stdout
	j.Stderr = stderr
	j.ExitCode = exitCode
	j.UpdatedAt = at
}

// Rebuild replaces the store's contents with exactly the given jobs and
// restores FIFO claim order for any job still StatusQueued. Used once, on
// startup, to restore state from an event-log replay — never called during
// normal request handling, so it isn't part of the Store interface.
func (s *MemoryStore) Rebuild(jobs []*Job) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs = make(map[string]*Job, len(jobs))
	s.order = nil
	for _, j := range jobs {
		cp := *j
		s.jobs[j.ID] = &cp
		if j.Status == StatusQueued {
			s.order = append(s.order, j.ID)
		}
	}
}

func (s *MemoryStore) Complete(id string, status Status, stdout, stderr string, exitCode int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return ErrNotFound
	}
	j.Status = status
	j.Stdout = stdout
	j.Stderr = stderr
	j.ExitCode = exitCode
	j.UpdatedAt = time.Now()
	return nil
}
