package coordinator_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ritvikreddygangula/forge/internal/coordinator"
	"github.com/ritvikreddygangula/forge/internal/job"
)

func TestHandleWorkerPoll_EmptyQueue(t *testing.T) {
	srv := coordinator.NewServer(job.NewMemoryStore())

	req := httptest.NewRequest(http.MethodGet, "/internal/worker/poll", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	var got map[string]any
	json.Unmarshal(rec.Body.Bytes(), &got)
	if got["job"] != nil {
		t.Fatalf("expected job: null when queue empty, got %v", got["job"])
	}
}

func TestHandleWorkerPoll_ClaimsQueuedJob(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	srv := coordinator.NewServer(store)

	req := httptest.NewRequest(http.MethodGet, "/internal/worker/poll", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	var got map[string]any
	json.Unmarshal(rec.Body.Bytes(), &got)
	claimed := got["job"].(map[string]any)
	if claimed["id"] != created.ID {
		t.Fatalf("expected claimed job id %s, got %v", created.ID, claimed["id"])
	}
	if claimed["status"] != "running" {
		t.Fatalf("expected claimed job status running, got %v", claimed["status"])
	}
}

func TestHandleWorkerResult_Success(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	store.ClaimNext()
	srv := coordinator.NewServer(store)

	body := []byte(`{"id":"` + created.ID + `","status":"succeeded","stdout":"ok\n","stderr":"","exit_code":0}`)
	req := httptest.NewRequest(http.MethodPost, "/internal/worker/result", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
	}

	got, _ := store.Get(created.ID)
	if got.Status != job.StatusSucceeded {
		t.Fatalf("expected status succeeded, got %s", got.Status)
	}
}

func TestHandleWorkerResult_InvalidStatus(t *testing.T) {
	store := job.NewMemoryStore()
	created, _ := store.Create("alpine", []string{"true"}, 10)
	srv := coordinator.NewServer(store)

	body := []byte(`{"id":"` + created.ID + `","status":"bogus"}`)
	req := httptest.NewRequest(http.MethodPost, "/internal/worker/result", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}
