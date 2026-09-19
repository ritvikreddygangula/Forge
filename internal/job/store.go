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
