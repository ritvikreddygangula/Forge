package mcpserver_test

import (
	"context"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ritvikreddygangula/forge/internal/job"
	"github.com/ritvikreddygangula/forge/internal/mcpserver"
)

func connectedClient(t *testing.T, store job.Store) *mcp.ClientSession {
	t.Helper()
	server := mcpserver.New(store)
	clientTransport, serverTransport := mcp.NewInMemoryTransports()

	go func() {
		_ = server.Run(context.Background(), serverTransport)
	}()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client"}, nil)
	session, err := client.Connect(context.Background(), clientTransport, nil)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func TestMCP_SubmitJob_CreatesQueuedJob(t *testing.T) {
	store := job.NewMemoryStore()
	session := connectedClient(t, store)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "submit_job",
		Arguments: map[string]any{"image": "alpine", "command": []string{"true"}, "timeout_seconds": 10},
	})
	if err != nil {
		t.Fatalf("CallTool returned error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success, got error result: %+v", res)
	}
}

func TestMCP_GetJobStatus_ReturnsCurrentStatus(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	session := connectedClient(t, store)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "get_job_status",
		Arguments: map[string]any{"id": created.ID},
	})
	if err != nil {
		t.Fatalf("CallTool returned error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success, got error result: %+v", res)
	}
}

func TestMCP_GetJobStatus_UnknownIDReturnsErrorResult(t *testing.T) {
	store := job.NewMemoryStore()
	session := connectedClient(t, store)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "get_job_status",
		Arguments: map[string]any{"id": "does-not-exist"},
	})
	if err != nil {
		t.Fatalf("CallTool returned error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected an error result for an unknown job id")
	}
}

func TestMCP_CancelJob_CancelsQueuedJob(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	session := connectedClient(t, store)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "cancel_job",
		Arguments: map[string]any{"id": created.ID},
	})
	if err != nil {
		t.Fatalf("CallTool returned error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success, got error result: %+v", res)
	}
	got, err := store.Get(created.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusCancelled {
		t.Fatalf("expected cancelled, got %+v", got)
	}
}

func TestMCP_CancelJob_RunningJobReturnsErrorResult(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	if _, err := store.ClaimNext("worker-1"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	session := connectedClient(t, store)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "cancel_job",
		Arguments: map[string]any{"id": created.ID},
	})
	if err != nil {
		t.Fatalf("CallTool returned error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected an error result for cancelling a running job")
	}
}

func TestMCP_StreamLogs_ReturnsStdoutAndStderr(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	if _, err := store.ClaimNext("worker-1"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if err := store.Complete(created.ID, job.StatusSucceeded, "out\n", "err\n", 0); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	session := connectedClient(t, store)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "stream_logs",
		Arguments: map[string]any{"id": created.ID},
	})
	if err != nil {
		t.Fatalf("CallTool returned error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success, got error result: %+v", res)
	}
	text, ok := res.Content[0].(*mcp.TextContent)
	if !ok || !strings.Contains(text.Text, "out\n") || !strings.Contains(text.Text, "err\n") {
		t.Fatalf("expected stdout+stderr in text content, got %+v", res.Content)
	}
}
