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
	if _, err := store.ClaimNext(); err != nil {
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
