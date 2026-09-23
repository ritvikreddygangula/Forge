package coordinator_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ritvikreddygangula/forge/internal/coordinator"
	"github.com/ritvikreddygangula/forge/internal/job"
)

func TestHandleSubmitJob(t *testing.T) {
	srv := coordinator.NewServer(job.NewMemoryStore())

	body := []byte(`{"image":"python:3.11","command":["pytest","tests/"],"timeout_seconds":60}`)
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}
	if got["status"] != "queued" {
		t.Fatalf("expected status queued, got %v", got["status"])
	}
	if got["id"] == "" || got["id"] == nil {
		t.Fatalf("expected non-empty id in response, got %v", got["id"])
	}
}

func TestHandleSubmitJob_MissingFields(t *testing.T) {
	srv := coordinator.NewServer(job.NewMemoryStore())

	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader([]byte(`{}`)))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleSubmitJob_ImageStartsWithDash(t *testing.T) {
	srv := coordinator.NewServer(job.NewMemoryStore())

	body := []byte(`{"image":"-v","command":["true"],"timeout_seconds":10}`)
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestHandleSubmitJob_ForwardsToLeaderWhenNotLeader uses a real 2-node
// in-memory raft cluster (the same helpers Task 4.2a's failover test uses)
// plus real REST servers over httptest — no mocked raft, no mocked HTTP.
// Submits against whichever node is NOT the leader and proves the job ends
// up on the leader's store, not the follower's own store.
func TestHandleSubmitJob_ForwardsToLeaderWhenNotLeader(t *testing.T) {
	nodes := newInMemRaftCluster(t, 2)
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.raft.Shutdown()
		}
	})
	leader := waitForLeader(t, nodes, 2*time.Second)

	type nodeServer struct {
		store *job.MemoryStore
		srv   *coordinator.Server
		http  *httptest.Server
	}
	servers := make(map[string]*nodeServer, len(nodes))
	peers := make([]coordinator.PeerInfo, 0, len(nodes))
	for _, n := range nodes {
		store := job.NewMemoryStore()
		srv := coordinator.NewServer(store)
		ts := httptest.NewServer(srv)
		t.Cleanup(ts.Close)
		servers[n.id] = &nodeServer{store: store, srv: srv, http: ts}
		peers = append(peers, coordinator.PeerInfo{
			ID:       n.id,
			RaftAddr: string(n.transport.LocalAddr()),
			RESTAddr: strings.TrimPrefix(ts.URL, "http://"),
		})
	}
	for _, n := range nodes {
		servers[n.id].srv.SetRaftGate(coordinator.NewRaftGate(n.raft, peers))
	}

	var follower *nodeServer
	for _, n := range nodes {
		if n.id != leader.id {
			follower = servers[n.id]
		}
	}

	resp, err := http.Post(follower.http.URL+"/jobs", "application/json",
		strings.NewReader(`{"image":"alpine","command":["true"],"timeout_seconds":10}`))
	if err != nil {
		t.Fatalf("POST to follower failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 (forwarded to leader), got %d", resp.StatusCode)
	}
	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}
	id, _ := got["id"].(string)
	if id == "" {
		t.Fatal("expected non-empty id in response")
	}

	if _, err := servers[leader.id].store.Get(id); err != nil {
		t.Fatalf("expected job to exist on leader's store: %v", err)
	}
	if _, err := follower.store.Get(id); err == nil {
		t.Fatal("expected job to NOT exist on the follower's own store")
	}
}

func TestHandleGetJob(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	srv := coordinator.NewServer(store)

	req := httptest.NewRequest(http.MethodGet, "/jobs/"+created.ID, nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}
	if got["id"] != created.ID {
		t.Fatalf("expected id %s, got %v", created.ID, got["id"])
	}
}

func TestHandleGetJob_NotFound(t *testing.T) {
	srv := coordinator.NewServer(job.NewMemoryStore())

	req := httptest.NewRequest(http.MethodGet, "/jobs/does-not-exist", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestHandleGetJob_StoreError(t *testing.T) {
	srv := coordinator.NewServer(failingStore{})

	req := httptest.NewRequest(http.MethodGet, "/jobs/some-id", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}

func TestHandleGetJobLogs_NotFound(t *testing.T) {
	srv := coordinator.NewServer(job.NewMemoryStore())

	req := httptest.NewRequest(http.MethodGet, "/jobs/does-not-exist/logs", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestHandleGetJobLogs_StoreError(t *testing.T) {
	srv := coordinator.NewServer(failingStore{})

	req := httptest.NewRequest(http.MethodGet, "/jobs/some-id/logs", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}

func TestHandleGetJobLogs(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	if _, err := store.ClaimNext("worker-1"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if err := store.Complete(created.ID, job.StatusSucceeded, "hello\n", "", 0); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	srv := coordinator.NewServer(store)

	req := httptest.NewRequest(http.MethodGet, "/jobs/"+created.ID+"/logs", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte("hello")) {
		t.Fatalf("expected logs to contain job stdout, got %q", rec.Body.String())
	}
}

func TestDELETE_Jobs_CancelsQueuedJob(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	srv := coordinator.NewServer(store)

	req := httptest.NewRequest(http.MethodDelete, "/jobs/"+created.ID, nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	got, err := store.Get(created.ID)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != job.StatusCancelled {
		t.Fatalf("expected cancelled, got %+v", got)
	}
}

func TestDELETE_Jobs_RunningJobReturns409(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	if _, err := store.ClaimNext("worker-1"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	srv := coordinator.NewServer(store)

	req := httptest.NewRequest(http.MethodDelete, "/jobs/"+created.ID, nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestDELETE_Jobs_UnknownIDReturns404(t *testing.T) {
	srv := coordinator.NewServer(job.NewMemoryStore())
	req := httptest.NewRequest(http.MethodDelete, "/jobs/does-not-exist", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestGET_JobsLogsStream_SendsSSEChunks(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"echo", "hi"}, 10)
	if _, err := store.ClaimNext("worker-1"); err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if err := store.Complete(created.ID, job.StatusSucceeded, "out\n", "err\n", 0); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	srv := coordinator.NewServer(store)

	req := httptest.NewRequest(http.MethodGet, "/jobs/"+created.ID+"/logs/stream", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("expected text/event-stream, got %q", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: stdout") || !strings.Contains(body, "out\n") {
		t.Fatalf("expected an stdout SSE event, got %q", body)
	}
	if !strings.Contains(body, "event: stderr") || !strings.Contains(body, "err\n") {
		t.Fatalf("expected an stderr SSE event, got %q", body)
	}
}
