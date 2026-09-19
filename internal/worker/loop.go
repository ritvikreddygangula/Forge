package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/ritvikreddygangula/forge/internal/job"
)

type Loop struct {
	CoordinatorURL string
	HTTPClient     *http.Client
	PollInterval   time.Duration
	Execute        func(ctx context.Context, image string, command []string, timeoutSeconds int) (ExecResult, error)
}

func NewLoop(coordinatorURL string) *Loop {
	return &Loop{
		CoordinatorURL: coordinatorURL,
		HTTPClient:     &http.Client{Timeout: 10 * time.Second},
		PollInterval:   2 * time.Second,
		Execute:        RunJob,
	}
}

type polledJob struct {
	ID             string   `json:"id"`
	Image          string   `json:"image"`
	Command        []string `json:"command"`
	TimeoutSeconds int      `json:"timeout_seconds"`
}

type pollResponse struct {
	Job *polledJob `json:"job"`
}

func (l *Loop) pollOnce(ctx context.Context) (*polledJob, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, l.CoordinatorURL+"/internal/worker/poll", nil)
	if err != nil {
		return nil, err
	}
	resp, err := l.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	var pr pollResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return nil, err
	}
	return pr.Job, nil
}

type reportResultRequest struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exit_code"`
}

func (l *Loop) reportResult(ctx context.Context, id, status string, result ExecResult) error {
	payload, err := json.Marshal(reportResultRequest{
		ID:       id,
		Status:   status,
		Stdout:   result.Stdout,
		Stderr:   result.Stderr,
		ExitCode: result.ExitCode,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, l.CoordinatorURL+"/internal/worker/result", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := l.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected status reporting result: %d", resp.StatusCode)
	}
	return nil
}

func (l *Loop) RunOnce(ctx context.Context) error {
	j, err := l.pollOnce(ctx)
	if err != nil {
		return fmt.Errorf("poll failed: %w", err)
	}
	if j == nil {
		return nil
	}

	slog.Info("job claimed", "id", j.ID, "image", j.Image)

	result, execErr := l.Execute(ctx, j.Image, j.Command, j.TimeoutSeconds)
	status := string(job.StatusSucceeded)
	if execErr != nil || result.ExitCode != 0 {
		status = string(job.StatusFailed)
	}
	if execErr != nil {
		result.Stderr = result.Stderr + "\n" + execErr.Error()
	}

	if err := l.reportResult(ctx, j.ID, status, result); err != nil {
		return fmt.Errorf("report failed: %w", err)
	}
	slog.Info("job reported", "id", j.ID, "status", status)
	return nil
}

func (l *Loop) Run(ctx context.Context) {
	ticker := time.NewTicker(l.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := l.RunOnce(ctx); err != nil {
				slog.Error("worker loop iteration failed", "error", err)
			}
		}
	}
}
