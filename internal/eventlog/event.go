package eventlog

import (
	"time"

	"github.com/ritvikreddygangula/forge/internal/job"
)

type EventType string

const (
	EventJobCreated   EventType = "job_created"
	EventJobClaimed   EventType = "job_claimed"
	EventJobCompleted EventType = "job_completed"
)

// Event is the durable, replayable record of one job-state transition.
// Fields are shared across event types rather than a discriminated payload —
// simpler to (de)serialize, and each Type only reads the fields it needs.
type Event struct {
	Type      EventType `json:"type"`
	JobID     string    `json:"job_id"`
	Timestamp time.Time `json:"timestamp"`

	// job_created fields
	Image          string   `json:"image,omitempty"`
	Command        []string `json:"command,omitempty"`
	TimeoutSeconds int      `json:"timeout_seconds,omitempty"`

	// job_completed fields
	Status   job.Status `json:"status,omitempty"`
	Stdout   string     `json:"stdout,omitempty"`
	Stderr   string     `json:"stderr,omitempty"`
	ExitCode int        `json:"exit_code,omitempty"`
}

// Rebuild folds a sequence of events, in log order, into final Job states.
// A job's EventJobCreated must appear before its EventJobClaimed/
// EventJobCompleted for that job to reconstruct correctly — true by
// construction, since eventlog.Store always publishes in that order.
// Returned jobs preserve creation order, so a caller restoring queue state
// (job.MemoryStore.Rebuild) gets correct FIFO order for free.
func Rebuild(events []Event) []*job.Job {
	jobs := make(map[string]*job.Job)
	var order []string

	for _, e := range events {
		switch e.Type {
		case EventJobCreated:
			j := &job.Job{
				ID:             e.JobID,
				Image:          e.Image,
				Command:        e.Command,
				TimeoutSeconds: e.TimeoutSeconds,
				Status:         job.StatusQueued,
				CreatedAt:      e.Timestamp,
				UpdatedAt:      e.Timestamp,
			}
			jobs[e.JobID] = j
			order = append(order, e.JobID)
		case EventJobClaimed:
			if j, ok := jobs[e.JobID]; ok {
				j.Status = job.StatusRunning
				j.UpdatedAt = e.Timestamp
			}
		case EventJobCompleted:
			if j, ok := jobs[e.JobID]; ok {
				j.Status = e.Status
				j.Stdout = e.Stdout
				j.Stderr = e.Stderr
				j.ExitCode = e.ExitCode
				j.UpdatedAt = e.Timestamp
			}
		}
	}

	result := make([]*job.Job, 0, len(order))
	for _, id := range order {
		result = append(result, jobs[id])
	}
	return result
}

// ApplyEvent applies one event directly onto a live store — the incremental
// counterpart to Rebuild (which folds a whole batch into a fresh []*job.Job
// at startup). Used by the continuous tail loop (Part 4), one event at a
// time, as new writes arrive from whichever replica is currently leader.
func ApplyEvent(store *job.MemoryStore, e Event) {
	switch e.Type {
	case EventJobCreated:
		store.ApplyCreated(&job.Job{
			ID: e.JobID, Image: e.Image, Command: e.Command, TimeoutSeconds: e.TimeoutSeconds,
			Status: job.StatusQueued, CreatedAt: e.Timestamp, UpdatedAt: e.Timestamp,
		})
	case EventJobClaimed:
		store.ApplyClaimed(e.JobID, e.Timestamp)
	case EventJobCompleted:
		store.ApplyCompleted(e.JobID, e.Status, e.Stdout, e.Stderr, e.ExitCode, e.Timestamp)
	}
}
