package coordinator_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	jobv1 "github.com/ritvikreddygangula/forge/api/proto/gen/jobv1"
	"github.com/ritvikreddygangula/forge/internal/coordinator"
	"github.com/ritvikreddygangula/forge/internal/job"
	"github.com/ritvikreddygangula/forge/internal/worker"
)

// TestEndToEnd_SubmitPollExecuteReport automates the manual verification from
// Part 2 Task 2.4: a real coordinator serving both REST and gRPC, a real
// worker.Loop dialing the gRPC port with a fake Execute (this test is about
// the transport, not the executor — internal/worker/executor_test.go already
// covers real Docker execution under the `integration` build tag).
func TestEndToEnd_SubmitPollExecuteReport(t *testing.T) {
	store := job.NewMemoryStore()

	restSrv := httptest.NewServer(coordinator.NewServer(store))
	defer restSrv.Close()

	grpcLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	jobv1.RegisterJobServiceServer(grpcSrv, coordinator.NewGRPCServer(store))
	go func() { _ = grpcSrv.Serve(grpcLis) }()
	defer grpcSrv.Stop()

	l, err := worker.NewLoop(grpcLis.Addr().String())
	if err != nil {
		t.Fatalf("failed to build worker loop: %v", err)
	}
	defer func() { _ = l.Close() }()
	l.Execute = func(ctx context.Context, image string, command []string, timeoutSeconds int) (worker.ExecResult, error) {
		return worker.ExecResult{Stdout: "hello from test\n", ExitCode: 0}, nil
	}

	body := `{"image":"alpine","command":["echo","hi"],"timeout_seconds":10}`
	resp, err := http.Post(restSrv.URL+"/jobs", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	var submitted map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&submitted); err != nil {
		t.Fatalf("failed to decode submit response: %v", err)
	}
	_ = resp.Body.Close()
	id, _ := submitted["id"].(string)
	if id == "" {
		t.Fatalf("expected a non-empty id in submit response, got %+v", submitted)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := l.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	getResp, err := http.Get(restSrv.URL + "/jobs/" + id)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	defer func() { _ = getResp.Body.Close() }()
	var got map[string]any
	if err := json.NewDecoder(getResp.Body).Decode(&got); err != nil {
		t.Fatalf("failed to decode get response: %v", err)
	}

	if got["status"] != "succeeded" {
		t.Fatalf("expected status succeeded, got %v", got["status"])
	}
	if got["stdout"] != "hello from test\n" {
		t.Fatalf("expected stdout %q, got %v", "hello from test\n", got["stdout"])
	}
}

// TestEndToEnd_SubmitLogsStreamCancel walks the rest of the REST surface
// Task 6.3 added: a completed job's SSE log stream, and DELETE's two
// outcomes (cancels a queued job, rejects an already-terminal one).
func TestEndToEnd_SubmitLogsStreamCancel(t *testing.T) {
	store := job.NewMemoryStore()
	restSrv := httptest.NewServer(coordinator.NewServer(store))
	defer restSrv.Close()

	grpcLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	jobv1.RegisterJobServiceServer(grpcSrv, coordinator.NewGRPCServer(store))
	go func() { _ = grpcSrv.Serve(grpcLis) }()
	defer grpcSrv.Stop()

	l, err := worker.NewLoop(grpcLis.Addr().String())
	if err != nil {
		t.Fatalf("failed to build worker loop: %v", err)
	}
	defer func() { _ = l.Close() }()
	l.Execute = func(ctx context.Context, image string, command []string, timeoutSeconds int) (worker.ExecResult, error) {
		return worker.ExecResult{Stdout: "streamed\n", ExitCode: 0}, nil
	}

	submitResp, err := http.Post(restSrv.URL+"/jobs", "application/json",
		strings.NewReader(`{"image":"alpine","command":["true"],"timeout_seconds":10}`))
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	var submitted map[string]any
	if err := json.NewDecoder(submitResp.Body).Decode(&submitted); err != nil {
		t.Fatalf("failed to decode submit response: %v", err)
	}
	_ = submitResp.Body.Close()
	id, _ := submitted["id"].(string)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := l.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}

	streamResp, err := http.Get(restSrv.URL + "/jobs/" + id + "/logs/stream")
	if err != nil {
		t.Fatalf("stream request failed: %v", err)
	}
	streamBody, err := io.ReadAll(streamResp.Body)
	_ = streamResp.Body.Close()
	if err != nil {
		t.Fatalf("failed to read stream body: %v", err)
	}
	if streamResp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("expected text/event-stream, got %q", streamResp.Header.Get("Content-Type"))
	}
	if !strings.Contains(string(streamBody), "event: stdout") || !strings.Contains(string(streamBody), "streamed") {
		t.Fatalf("expected an stdout SSE event with the job's output, got %q", streamBody)
	}

	// A completed job can no longer be cancelled.
	req, err := http.NewRequest(http.MethodDelete, restSrv.URL+"/jobs/"+id, nil)
	if err != nil {
		t.Fatalf("failed to build DELETE request: %v", err)
	}
	delResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE request failed: %v", err)
	}
	_ = delResp.Body.Close()
	if delResp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 for cancelling a completed job, got %d", delResp.StatusCode)
	}

	// A second, still-queued job can be cancelled.
	submitResp2, err := http.Post(restSrv.URL+"/jobs", "application/json",
		strings.NewReader(`{"image":"alpine","command":["true"],"timeout_seconds":10}`))
	if err != nil {
		t.Fatalf("second submit failed: %v", err)
	}
	var submitted2 map[string]any
	if err := json.NewDecoder(submitResp2.Body).Decode(&submitted2); err != nil {
		t.Fatalf("failed to decode second submit response: %v", err)
	}
	_ = submitResp2.Body.Close()
	id2, _ := submitted2["id"].(string)

	req2, err := http.NewRequest(http.MethodDelete, restSrv.URL+"/jobs/"+id2, nil)
	if err != nil {
		t.Fatalf("failed to build second DELETE request: %v", err)
	}
	delResp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("second DELETE request failed: %v", err)
	}
	defer func() { _ = delResp2.Body.Close() }()
	if delResp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for cancelling a queued job, got %d", delResp2.StatusCode)
	}
	var cancelled map[string]any
	if err := json.NewDecoder(delResp2.Body).Decode(&cancelled); err != nil {
		t.Fatalf("failed to decode cancel response: %v", err)
	}
	if cancelled["status"] != "cancelled" {
		t.Fatalf("expected cancelled status, got %v", cancelled["status"])
	}
}
