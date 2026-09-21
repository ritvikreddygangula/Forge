package job

import "time"

type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

type Job struct {
	ID             string
	Image          string
	Command        []string
	TimeoutSeconds int
	Status         Status
	Stdout         string
	Stderr         string
	ExitCode       int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
