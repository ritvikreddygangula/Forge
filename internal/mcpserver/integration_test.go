package mcpserver_test

import (
	"context"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/ritvikreddygangula/forge/internal/job"
)

// TestMCP_EndToEnd_SubmitStatusLogsCancel walks all 4 tools in one session
// against a single store, the same "submit through cancel" scenario Task
// 6.5 asks for on the MCP surface.
func TestMCP_EndToEnd_SubmitStatusLogsCancel(t *testing.T) {
	store := job.NewMemoryStore()
	session := connectedClient(t, store)
	ctx := context.Background()

	submitRes, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "submit_job",
		Arguments: map[string]any{"image": "alpine", "command": []string{"true"}, "timeout_seconds": 10},
	})
	if err != nil {
		t.Fatalf("submit_job returned error: %v", err)
	}
	if submitRes.IsError {
		t.Fatalf("submit_job returned an error result: %+v", submitRes)
	}
	out, ok := submitRes.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("expected structured content, got %+v", submitRes.StructuredContent)
	}
	id, _ := out["id"].(string)
	if id == "" {
		t.Fatalf("expected a non-empty job id, got %+v", out)
	}

	statusRes, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "get_job_status",
		Arguments: map[string]any{"id": id},
	})
	if err != nil {
		t.Fatalf("get_job_status returned error: %v", err)
	}
	statusOut, _ := statusRes.StructuredContent.(map[string]any)
	if statusOut["status"] != "queued" {
		t.Fatalf("expected queued, got %+v", statusOut)
	}

	// Claim and complete it directly on the store — this test is about the
	// MCP surface, not execution (same split as the REST/gRPC integration
	// test, which uses a fake Execute for the same reason).
	if _, err := store.ClaimNext("worker-1"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if err := store.Complete(id, job.StatusSucceeded, "mcp output\n", "", 0); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}

	logsRes, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "stream_logs",
		Arguments: map[string]any{"id": id},
	})
	if err != nil {
		t.Fatalf("stream_logs returned error: %v", err)
	}
	text, ok := logsRes.Content[0].(*mcp.TextContent)
	if !ok || !strings.Contains(text.Text, "mcp output") {
		t.Fatalf("expected job output in logs, got %+v", logsRes.Content)
	}

	// A completed job can no longer be cancelled.
	cancelRes, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "cancel_job",
		Arguments: map[string]any{"id": id},
	})
	if err != nil {
		t.Fatalf("cancel_job returned error: %v", err)
	}
	if !cancelRes.IsError {
		t.Fatal("expected an error result for cancelling a completed job")
	}

	// A second, still-queued job can be cancelled.
	submitRes2, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "submit_job",
		Arguments: map[string]any{"image": "alpine", "command": []string{"true"}, "timeout_seconds": 10},
	})
	if err != nil {
		t.Fatalf("second submit_job returned error: %v", err)
	}
	out2, _ := submitRes2.StructuredContent.(map[string]any)
	id2, _ := out2["id"].(string)

	cancelRes2, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "cancel_job",
		Arguments: map[string]any{"id": id2},
	})
	if err != nil {
		t.Fatalf("second cancel_job returned error: %v", err)
	}
	if cancelRes2.IsError {
		t.Fatalf("expected success cancelling a queued job, got error result: %+v", cancelRes2)
	}

	got, err := store.Get(id2)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusCancelled {
		t.Fatalf("expected cancelled, got %+v", got)
	}
}
