package raft

import "errors"

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

type Operation string

const (
	Put    Operation = "put"
	Delete Operation = "delete"
	Read   Operation = "read"
	Noop   Operation = "noop"
)

type Command struct {
	Operation Operation `json:"operation"`
	Key       string    `json:"key,omitempty"`
	Value     string    `json:"value,omitempty"`
}

type Entry struct {
	Index   uint64  `json:"index"`
	Term    uint64  `json:"term"`
	Command Command `json:"command"`
}

type ApplyResult struct {
	Value string
	Found bool
}

type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

type AppendEntriesRequest struct {
	Term         uint64  `json:"term"`
	LeaderID     string  `json:"leader_id"`
	PrevLogIndex uint64  `json:"prev_log_index"`
	PrevLogTerm  uint64  `json:"prev_log_term"`
	Entries      []Entry `json:"entries"`
	LeaderCommit uint64  `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	MatchIndex    uint64 `json:"match_index,omitempty"`
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
}

type Status struct {
	ID          string `json:"id"`
	Role        Role   `json:"role"`
	Term        uint64 `json:"term"`
	LeaderID    string `json:"leader_id,omitempty"`
	CommitIndex uint64 `json:"commit_index"`
	LastApplied uint64 `json:"last_applied"`
	LastLog     uint64 `json:"last_log_index"`
}

type NotLeaderError struct {
	LeaderID  string
	LeaderURL string
}

func (e *NotLeaderError) Error() string { return "node is not the leader" }

var ErrStopped = errors.New("raft node stopped")
