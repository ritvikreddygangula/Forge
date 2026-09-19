package coordinator

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/ritvikreddygangula/forge/internal/job"
)

type pollResponse struct {
	Job *jobResponse `json:"job"`
}

func (s *Server) handleWorkerPoll(w http.ResponseWriter, r *http.Request) {
	j, err := s.store.ClaimNext()
	if err != nil {
		http.Error(w, "failed to claim job", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if j == nil {
		json.NewEncoder(w).Encode(pollResponse{})
		return
	}
	resp := toJobResponse(j)
	json.NewEncoder(w).Encode(pollResponse{Job: &resp})
}

type reportResultRequest struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exit_code"`
}

func (s *Server) handleWorkerResult(w http.ResponseWriter, r *http.Request) {
	var req reportResultRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	status := job.Status(req.Status)
	if status != job.StatusSucceeded && status != job.StatusFailed {
		http.Error(w, "status must be succeeded or failed", http.StatusBadRequest)
		return
	}

	if err := s.store.Complete(req.ID, status, req.Stdout, req.Stderr, req.ExitCode); err != nil {
		if errors.Is(err, job.ErrNotFound) {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to complete job", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
