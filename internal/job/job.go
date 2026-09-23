package job

import "time"

type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

type Job struct {
	ID             string
	Image          string
	Command        []string
	TimeoutSeconds int
	Status         Status
	WorkerID       string // which worker holds/held this job; empty until claimed
	Stdout         string
	Stderr         string
	ExitCode       int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
