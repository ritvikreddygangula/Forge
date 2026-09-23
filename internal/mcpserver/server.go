// Package mcpserver exposes the same 4 actions as the REST API as a thin MCP
// wrapper — direct calls into job.Store, no new engine, no leader forwarding
// (single-instance use only; see docs/plans/part-6-rest-mcp.md).
package mcpserver

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ritvikreddygangula/forge/internal/job"
)

// New builds an MCP server exposing submit_job, get_job_status, stream_logs,
// and cancel_job, each a thin wrapper over the given job.Store.
func New(store job.Store) *mcp.Server {
	s := mcp.NewServer(&mcp.Implementation{Name: "forge", Version: "v1.0.0"}, nil)

	mcp.AddTool(s, &mcp.Tool{Name: "submit_job", Description: "Submit a job for execution"}, submitJob(store))
	mcp.AddTool(s, &mcp.Tool{Name: "get_job_status", Description: "Get a job's current status"}, getJobStatus(store))
	mcp.AddTool(s, &mcp.Tool{Name: "stream_logs", Description: "Get a job's captured stdout/stderr"}, streamLogs(store))
	mcp.AddTool(s, &mcp.Tool{Name: "cancel_job", Description: "Cancel a job while it's still queued"}, cancelJob(store))

	return s
}

type submitJobArgs struct {
	Image          string   `json:"image" jsonschema:"the container image to run"`
	Command        []string `json:"command" jsonschema:"the command to run inside the container"`
	TimeoutSeconds int      `json:"timeout_seconds,omitempty" jsonschema:"execution timeout in seconds, default 300"`
}

type jobOut struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func submitJob(store job.Store) mcp.ToolHandlerFor[submitJobArgs, jobOut] {
	return func(_ context.Context, _ *mcp.CallToolRequest, args submitJobArgs) (*mcp.CallToolResult, jobOut, error) {
		if args.TimeoutSeconds <= 0 {
			args.TimeoutSeconds = 300
		}
		j, err := store.Create(args.Image, args.Command, args.TimeoutSeconds)
		if err != nil {
			return nil, jobOut{}, err
		}
		return nil, jobOut{ID: j.ID, Status: string(j.Status)}, nil
	}
}

type jobIDArgs struct {
	ID string `json:"id" jsonschema:"the job id"`
}

func getJobStatus(store job.Store) mcp.ToolHandlerFor[jobIDArgs, jobOut] {
	return func(_ context.Context, _ *mcp.CallToolRequest, args jobIDArgs) (*mcp.CallToolResult, jobOut, error) {
		j, err := store.Get(args.ID)
		if err != nil {
			return nil, jobOut{}, err
		}
		return nil, jobOut{ID: j.ID, Status: string(j.Status)}, nil
	}
}

func streamLogs(store job.Store) mcp.ToolHandlerFor[jobIDArgs, any] {
	return func(_ context.Context, _ *mcp.CallToolRequest, args jobIDArgs) (*mcp.CallToolResult, any, error) {
		j, err := store.Get(args.ID)
		if err != nil {
			return nil, nil, err
		}
		text := fmt.Sprintf("--- stdout ---\n%s\n--- stderr ---\n%s\n", j.Stdout, j.Stderr)
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}, nil, nil
	}
}

func cancelJob(store job.Store) mcp.ToolHandlerFor[jobIDArgs, any] {
	return func(_ context.Context, _ *mcp.CallToolRequest, args jobIDArgs) (*mcp.CallToolResult, any, error) {
		// ToolHandlerFor auto-wraps a returned error into CallToolResult with
		// IsError set, so there's no need to hand-construct one here.
		if _, err := store.Cancel(args.ID); err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "cancelled"}}}, nil, nil
	}
}
