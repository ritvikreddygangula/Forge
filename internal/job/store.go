package job

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

var ErrNotFound = errors.New("job not found")
var ErrNotCancellable = errors.New("job is not cancellable (already running or terminal)")

type Store interface {
	Create(image string, command []string, timeoutSeconds int) (*Job, error)
	Get(id string) (*Job, error)
	ClaimNext(workerID string) (*Job, error)
	Complete(id string, status Status, stdout, stderr string, exitCode int) error
	RequeueRunning(workerID string) ([]*Job, error)
	Cancel(id string) (*Job, error)
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

func (s *MemoryStore) ClaimNext(workerID string) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, id := range s.order {
		j := s.jobs[id]
		if j.Status == StatusQueued {
			j.Status = StatusRunning
			j.WorkerID = workerID
			j.UpdatedAt = time.Now()
			cp := *j
			return &cp, nil
		}
	}
	return nil, nil
}

// RequeueRunning finds any job still Running under the given worker ID and
// moves it back to Queued — used when a worker has stopped heartbeating and
// its in-flight job needs to be picked up by someone else. Goes to the back
// of the FIFO queue, the same position a fresh Create would leave it in.
func (s *MemoryStore) RequeueRunning(workerID string) ([]*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var requeued []*Job
	for _, j := range s.jobs {
		if j.Status == StatusRunning && j.WorkerID == workerID {
			j.Status = StatusQueued
			j.WorkerID = ""
			j.UpdatedAt = time.Now()
			s.order = append(s.order, j.ID)
			cp := *j
			requeued = append(requeued, &cp)
		}
	}
	return requeued, nil
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

// ApplyClaimed marks a specific job running under a specific worker. Unlike
// ClaimNext, the job ID and worker are already decided (by whichever
// replica was leader when the claim happened) — this just replicates that
// decision. A no-op if the job is unknown or already past queued, so
// replaying an already-applied event is harmless.
func (s *MemoryStore) ApplyClaimed(id string, workerID string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok || j.Status != StatusQueued {
		return
	}
	j.Status = StatusRunning
	j.WorkerID = workerID
	j.UpdatedAt = at
}

// ApplyRequeued marks a specific job queued again, clearing its worker.
// Same replication role as ApplyClaimed, for the requeued transition — a
// no-op if the job is unknown or not currently running.
func (s *MemoryStore) ApplyRequeued(id string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok || j.Status != StatusRunning {
		return
	}
	j.Status = StatusQueued
	j.WorkerID = ""
	j.UpdatedAt = at
	s.order = append(s.order, id)
}

// ApplyCancelled marks a specific job cancelled. Same replication role as
// ApplyRequeued — a no-op if the job is unknown or no longer queued, so
// replaying an already-applied (or superseded) event is harmless.
func (s *MemoryStore) ApplyCancelled(id string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok || j.Status != StatusQueued {
		return
	}
	j.Status = StatusCancelled
	j.UpdatedAt = at
	for i, oid := range s.order {
		if oid == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
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

// Cancel only succeeds while a job is still Queued — a job already claimed
// by a worker can't be stopped mid-execution under this project's pull-based
// model (see docs/plans/part-6-rest-mcp.md for why that's a stated scope
// decision, not a missing feature). Removing it from s.order here is what
// stops a later ClaimNext from ever handing it to a worker — the job stays
// in s.jobs (so Get still finds it, now Cancelled), just no longer queued.
func (s *MemoryStore) Cancel(id string) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	if j.Status != StatusQueued {
		return nil, ErrNotCancellable
	}
	j.Status = StatusCancelled
	j.UpdatedAt = time.Now()

	for i, oid := range s.order {
		if oid == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}

	cp := *j
	return &cp, nil
}
