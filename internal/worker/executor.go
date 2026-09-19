package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"
)

type ExecResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

func RunJob(ctx context.Context, image string, command []string, timeoutSeconds int) (ExecResult, error) {
	timeout := time.Duration(timeoutSeconds) * time.Second
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	args := append([]string{"run", "--rm", image}, command...)
	cmd := exec.CommandContext(ctx, "docker", args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	runErr := cmd.Run()
	result := ExecResult{Stdout: stdout.String(), Stderr: stderr.String()}

	if ctx.Err() == context.DeadlineExceeded {
		return result, fmt.Errorf("job timed out after %s", timeout)
	}
	if runErr == nil {
		return result, nil
	}

	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		result.ExitCode = exitErr.ExitCode()
		return result, nil // non-zero exit is a valid job outcome, not a Go error
	}

	return result, fmt.Errorf("failed to run docker: %w", runErr)
}
