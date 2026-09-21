package coordinator_test

import (
	"context"
	"encoding/json"
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
