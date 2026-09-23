package coordinator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/ritvikreddygangula/forge/internal/job"
)

type Server struct {
	store    job.Store
	mux      *http.ServeMux
	raftGate *RaftGate // nil in single-node mode; see raft.go
}

func NewServer(store job.Store) *Server {
	s := &Server{store: store, mux: http.NewServeMux()}
	s.routes()
	return s
}

// SetRaftGate wires this replica's leader-election state into the server.
// Called only when running as part of a raft cluster (Task 4.2c); a Server
// with no gate set behaves exactly as it did before this Part — every write
// handler treats a nil gate as "always leader."
func (s *Server) SetRaftGate(g *RaftGate) { s.raftGate = g }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /jobs", s.handleSubmitJob)
	s.mux.HandleFunc("GET /jobs/{id}", s.handleGetJob)
	s.mux.HandleFunc("GET /jobs/{id}/logs", s.handleGetJobLogs)
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
	if !s.raftGate.IsLeader() {
		s.forwardToLeader(w, r)
		return
	}

	var req submitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.Image == "" || len(req.Command) == 0 {
		http.Error(w, "image and command are required", http.StatusBadRequest)
		return
	}
	if strings.HasPrefix(req.Image, "-") {
		http.Error(w, "image must not start with '-'", http.StatusBadRequest)
		return
	}
	if req.TimeoutSeconds <= 0 {
		req.TimeoutSeconds = 300
	}

	j, err := s.store.Create(req.Image, req.Command, req.TimeoutSeconds)
	if err != nil {
		slog.Error("failed to create job", "error", err)
		http.Error(w, "failed to create job", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(toJobResponse(j)); err != nil {
		slog.Error("failed to encode response", "error", err)
	}
}

// forwardToLeader proxies a write request to whichever replica raft says is
// currently leader. Used when this replica isn't it — the caller (an
// external client or a worker) never needs to know or care which of the 3
// REST ports is actually the leader at any given moment.
func (s *Server) forwardToLeader(w http.ResponseWriter, r *http.Request) {
	leader, ok := s.raftGate.Leader()
	if !ok {
		http.Error(w, "no raft leader elected, try again shortly", http.StatusServiceUnavailable)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, "http://"+leader.RESTAddr+r.URL.Path, bytes.NewReader(body))
	if err != nil {
		http.Error(w, "failed to build forwarded request", http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", r.Header.Get("Content-Type"))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, "failed to forward request to leader", http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		slog.Error("failed to copy forwarded response body", "error", err)
	}
}

// handleGetJob and handleGetJobLogs below are deliberately NOT leader-gated
// — any replica serves reads from its own local, eventually-consistent copy
// (kept in sync by the continuous event-log tail). A client that just wrote
// via one replica and immediately reads via another may briefly see stale
// data until that replica's tail loop catches up — not read-your-writes
// consistent across replicas, documented here rather than hidden.
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
	if err := json.NewEncoder(w).Encode(toJobResponse(j)); err != nil {
		slog.Error("failed to encode response", "error", err)
	}
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
	if _, err := fmt.Fprintf(w, "--- stdout ---\n%s\n--- stderr ---\n%s\n", j.Stdout, j.Stderr); err != nil {
		slog.Error("failed to write response", "error", err)
	}
}
