package coordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/ritvikreddygangula/forge/internal/job"
)

type Server struct {
	store job.Store
	mux   *http.ServeMux
}

func NewServer(store job.Store) *Server {
	s := &Server{store: store, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /jobs", s.handleSubmitJob)
	s.mux.HandleFunc("GET /jobs/{id}", s.handleGetJob)
	s.mux.HandleFunc("GET /jobs/{id}/logs", s.handleGetJobLogs)
	s.mux.HandleFunc("GET /internal/worker/poll", s.handleWorkerPoll)
	s.mux.HandleFunc("POST /internal/worker/result", s.handleWorkerResult)
}

type submitJobRequest struct {
	Image          string   `json:"image"`
	Command        []string `json:"command"`
	TimeoutSeconds int      `json:"timeout_seconds"`
}

type jobResponse struct {
	ID             string   `json:"id"`
	Image          string   `json:"image"`
	Command        []string `json:"command"`
	TimeoutSeconds int      `json:"timeout_seconds"`
	Status         string   `json:"status"`
	ExitCode       int      `json:"exit_code"`
	Stdout         string   `json:"stdout,omitempty"`
	Stderr         string   `json:"stderr,omitempty"`
}

func toJobResponse(j *job.Job) jobResponse {
	return jobResponse{
		ID:             j.ID,
		Image:          j.Image,
		Command:        j.Command,
		TimeoutSeconds: j.TimeoutSeconds,
		Status:         string(j.Status),
		ExitCode:       j.ExitCode,
		Stdout:         j.Stdout,
		Stderr:         j.Stderr,
	}
}

func (s *Server) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var req submitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.Image == "" || len(req.Command) == 0 {
		http.Error(w, "image and command are required", http.StatusBadRequest)
		return
	}
	if req.TimeoutSeconds <= 0 {
		req.TimeoutSeconds = 300
	}

	j, err := s.store.Create(req.Image, req.Command, req.TimeoutSeconds)
	if err != nil {
		http.Error(w, "failed to create job", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(toJobResponse(j))
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, err := s.store.Get(id)
	if err != nil {
		if errors.Is(err, job.ErrNotFound) {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get job", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(toJobResponse(j))
}

func (s *Server) handleGetJobLogs(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, err := s.store.Get(id)
	if err != nil {
		if errors.Is(err, job.ErrNotFound) {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get job", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "--- stdout ---\n%s\n--- stderr ---\n%s\n", j.Stdout, j.Stderr)
}
