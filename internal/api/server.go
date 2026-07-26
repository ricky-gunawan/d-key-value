package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/ricky-gunawan/d-key-value/internal/raft"
)

type Server struct {
	node *raft.Node
}

func NewServer(node *raft.Node) http.Handler {
	s := &Server{node: node}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft/request-vote", s.requestVote)
	mux.HandleFunc("POST /raft/append-entries", s.appendEntries)
	mux.HandleFunc("GET /v1/status", s.status)
	mux.HandleFunc("PUT /v1/kv/{key}", s.put)
	mux.HandleFunc("GET /v1/kv/{key}", s.get)
	mux.HandleFunc("DELETE /v1/kv/{key}", s.delete)
	return mux
}

func (s *Server) requestVote(w http.ResponseWriter, r *http.Request) {
	var request raft.RequestVoteRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	response, err := s.node.HandleRequestVote(request)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error(), "", "")
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) appendEntries(w http.ResponseWriter, r *http.Request) {
	var request raft.AppendEntriesRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	response, err := s.node.HandleAppendEntries(request)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error(), "", "")
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) status(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.node.Status())
}

func (s *Server) put(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Value string `json:"value"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	key := r.PathValue("key")
	if _, ok := s.propose(w, r, raft.Command{Operation: raft.Put, Key: key, Value: body.Value}); ok {
		writeJSON(w, http.StatusOK, map[string]string{"key": key, "status": "stored"})
	}
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	result, ok := s.propose(w, r, raft.Command{Operation: raft.Read, Key: key})
	if !ok {
		return
	}
	if !result.Found {
		writeError(w, http.StatusNotFound, "key not found", "", "")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": result.Value})
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if _, ok := s.propose(w, r, raft.Command{Operation: raft.Delete, Key: key}); ok {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *Server) propose(w http.ResponseWriter, r *http.Request, command raft.Command) (raft.ApplyResult, bool) {
	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
	defer cancel()
	result, err := s.node.Propose(ctx, command)
	if err != nil {
		var notLeader *raft.NotLeaderError
		if errors.As(err, &notLeader) {
			writeError(w, http.StatusServiceUnavailable, err.Error(), notLeader.LeaderID, notLeader.LeaderURL)
			return raft.ApplyResult{}, false
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			writeError(w, http.StatusGatewayTimeout, "operation did not reach a quorum before the deadline", "", "")
			return raft.ApplyResult{}, false
		}
		writeError(w, http.StatusInternalServerError, err.Error(), "", "")
		return raft.ApplyResult{}, false
	}
	return result, true
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error(), "", "")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message, leaderID, leaderURL string) {
	body := struct {
		Error     string `json:"error"`
		LeaderID  string `json:"leader_id,omitempty"`
		LeaderURL string `json:"leader_url,omitempty"`
	}{Error: message, LeaderID: leaderID, LeaderURL: leaderURL}
	writeJSON(w, status, body)
}
