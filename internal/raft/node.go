package raft

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	heartbeatInterval = 100 * time.Millisecond
	electionMin       = 350 * time.Millisecond
	electionJitter    = 300 * time.Millisecond
)

type Node struct {
	mu sync.Mutex

	id        string
	peers     map[string]string
	statePath string
	logger    *log.Logger
	client    *http.Client

	persistent  persistentState
	role        Role
	leaderID    string
	lastApplied uint64
	kv          map[string]string
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	waiters     map[uint64]chan proposalResult

	electionReset   time.Time
	electionTimeout time.Duration
	stopCh          chan struct{}
	stopOnce        sync.Once
}

type proposalResult struct {
	result ApplyResult
	err    error
}

func NewNode(id string, peers map[string]string, statePath string, logger *log.Logger) (*Node, error) {
	if id == "" {
		return nil, fmt.Errorf("node id is required")
	}
	if _, ok := peers[id]; !ok {
		return nil, fmt.Errorf("node %q is missing from peers", id)
	}
	state, err := loadState(statePath)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = log.Default()
	}
	n := &Node{
		id: id, peers: clonePeers(peers), statePath: statePath, logger: logger,
		client:     &http.Client{Timeout: 250 * time.Millisecond},
		persistent: state, role: Follower, kv: make(map[string]string),
		nextIndex: make(map[string]uint64), matchIndex: make(map[string]uint64),
		waiters: make(map[uint64]chan proposalResult), stopCh: make(chan struct{}),
	}
	n.resetElectionTimerLocked()
	n.applyCommittedLocked()
	return n, nil
}

func clonePeers(peers map[string]string) map[string]string {
	result := make(map[string]string, len(peers))
	for id, url := range peers {
		result[id] = strings.TrimRight(url, "/")
	}
	return result
}

func (n *Node) Start() {
	go n.electionLoop()
	go n.heartbeatLoop()
}

func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)
		n.mu.Lock()
		n.failWaitersLocked(ErrStopped)
		n.mu.Unlock()
	})
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{ID: n.id, Role: n.role, Term: n.persistent.CurrentTerm,
		LeaderID: n.leaderID, CommitIndex: n.persistent.CommitIndex,
		LastApplied: n.lastApplied, LastLog: n.lastLogIndexLocked()}
}

func (n *Node) LeaderURL() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.peers[n.leaderID]
}

func (n *Node) Propose(ctx context.Context, command Command) (ApplyResult, error) {
	n.mu.Lock()
	if n.role != Leader {
		err := &NotLeaderError{LeaderID: n.leaderID, LeaderURL: n.peers[n.leaderID]}
		n.mu.Unlock()
		return ApplyResult{}, err
	}
	index := n.lastLogIndexLocked() + 1
	entry := Entry{Index: index, Term: n.persistent.CurrentTerm, Command: command}
	n.persistent.Log = append(n.persistent.Log, entry)
	if err := n.persistLocked(); err != nil {
		n.persistent.Log = n.persistent.Log[:len(n.persistent.Log)-1]
		n.mu.Unlock()
		return ApplyResult{}, err
	}
	waiter := make(chan proposalResult, 1)
	n.waiters[index] = waiter
	n.matchIndex[n.id] = index
	n.advanceCommitLocked()
	n.mu.Unlock()

	n.replicateAll()
	select {
	case result := <-waiter:
		return result.result, result.err
	case <-ctx.Done():
		n.mu.Lock()
		delete(n.waiters, index)
		n.mu.Unlock()
		return ApplyResult{}, ctx.Err()
	case <-n.stopCh:
		return ApplyResult{}, ErrStopped
	}
}

func (n *Node) HandleRequestVote(req RequestVoteRequest) (RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term < n.persistent.CurrentTerm {
		return RequestVoteResponse{Term: n.persistent.CurrentTerm}, nil
	}
	if req.Term > n.persistent.CurrentTerm {
		n.becomeFollowerLocked(req.Term, "")
		if err := n.persistLocked(); err != nil {
			return RequestVoteResponse{}, err
		}
	}
	upToDate := req.LastLogTerm > n.lastLogTermLocked() ||
		(req.LastLogTerm == n.lastLogTermLocked() && req.LastLogIndex >= n.lastLogIndexLocked())
	canVote := n.persistent.VotedFor == "" || n.persistent.VotedFor == req.CandidateID
	if canVote && upToDate {
		n.persistent.VotedFor = req.CandidateID
		n.resetElectionTimerLocked()
		if err := n.persistLocked(); err != nil {
			return RequestVoteResponse{}, err
		}
		return RequestVoteResponse{Term: n.persistent.CurrentTerm, VoteGranted: true}, nil
	}
	return RequestVoteResponse{Term: n.persistent.CurrentTerm}, nil
}

func (n *Node) HandleAppendEntries(req AppendEntriesRequest) (AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term < n.persistent.CurrentTerm {
		return AppendEntriesResponse{Term: n.persistent.CurrentTerm, ConflictIndex: n.lastLogIndexLocked() + 1}, nil
	}
	changed := false
	if req.Term > n.persistent.CurrentTerm {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
		changed = true
	} else if n.role != Follower {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
	}
	n.leaderID = req.LeaderID
	n.resetElectionTimerLocked()

	if req.PrevLogIndex > n.lastLogIndexLocked() {
		if changed {
			if err := n.persistLocked(); err != nil {
				return AppendEntriesResponse{}, err
			}
		}
		return AppendEntriesResponse{Term: n.persistent.CurrentTerm, ConflictIndex: n.lastLogIndexLocked() + 1}, nil
	}
	if req.PrevLogIndex > 0 && n.persistent.Log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
		conflictTerm := n.persistent.Log[req.PrevLogIndex-1].Term
		conflictIndex := req.PrevLogIndex
		for conflictIndex > 1 && n.persistent.Log[conflictIndex-2].Term == conflictTerm {
			conflictIndex--
		}
		if changed {
			if err := n.persistLocked(); err != nil {
				return AppendEntriesResponse{}, err
			}
		}
		return AppendEntriesResponse{Term: n.persistent.CurrentTerm, ConflictIndex: conflictIndex}, nil
	}

	for i, entry := range req.Entries {
		if entry.Index <= n.lastLogIndexLocked() {
			if n.persistent.Log[entry.Index-1].Term == entry.Term {
				continue
			}
			n.persistent.Log = n.persistent.Log[:entry.Index-1]
			n.persistent.Log = append(n.persistent.Log, req.Entries[i:]...)
			changed = true
			break
		}
		n.persistent.Log = append(n.persistent.Log, req.Entries[i:]...)
		changed = true
		break
	}
	if req.LeaderCommit > n.persistent.CommitIndex {
		n.persistent.CommitIndex = min(req.LeaderCommit, n.lastLogIndexLocked())
		changed = true
	}
	if changed {
		if err := n.persistLocked(); err != nil {
			return AppendEntriesResponse{}, err
		}
	}
	n.applyCommittedLocked()
	return AppendEntriesResponse{Term: n.persistent.CurrentTerm, Success: true,
		MatchIndex: req.PrevLogIndex + uint64(len(req.Entries))}, nil
}

func (n *Node) electionLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.mu.Lock()
			due := n.role != Leader && time.Since(n.electionReset) >= n.electionTimeout
			n.mu.Unlock()
			if due {
				n.startElection()
			}
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.replicateAll()
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	if n.role == Leader || time.Since(n.electionReset) < n.electionTimeout {
		n.mu.Unlock()
		return
	}
	n.role = Candidate
	n.leaderID = ""
	n.persistent.CurrentTerm++
	term := n.persistent.CurrentTerm
	n.persistent.VotedFor = n.id
	n.resetElectionTimerLocked()
	lastIndex, lastTerm := n.lastLogIndexLocked(), n.lastLogTermLocked()
	if err := n.persistLocked(); err != nil {
		n.logger.Printf("persist election state: %v", err)
		n.mu.Unlock()
		return
	}
	votes := 1
	majority := len(n.peers)/2 + 1
	if votes >= majority {
		n.becomeLeaderLocked()
		n.mu.Unlock()
		n.replicateAll()
		return
	}
	n.mu.Unlock()

	var voteMu sync.Mutex
	for peerID, peerURL := range n.peers {
		if peerID == n.id {
			continue
		}
		go func(url string) {
			req := RequestVoteRequest{Term: term, CandidateID: n.id, LastLogIndex: lastIndex, LastLogTerm: lastTerm}
			var resp RequestVoteResponse
			if err := n.postJSON(url+"/raft/request-vote", req, &resp); err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if resp.Term > n.persistent.CurrentTerm {
				n.becomeFollowerLocked(resp.Term, "")
				if err := n.persistLocked(); err != nil {
					n.logger.Printf("persist newer term: %v", err)
				}
				return
			}
			if n.role != Candidate || n.persistent.CurrentTerm != term || !resp.VoteGranted {
				return
			}
			voteMu.Lock()
			votes++
			won := votes >= majority
			voteMu.Unlock()
			if won && n.role == Candidate {
				n.becomeLeaderLocked()
				go n.replicateAll()
			}
		}(peerURL)
	}
}

func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leaderID = n.id
	last := n.lastLogIndexLocked()
	for peerID := range n.peers {
		n.nextIndex[peerID] = last + 1
		n.matchIndex[peerID] = 0
	}
	n.matchIndex[n.id] = last
	n.logger.Printf("node %s became leader for term %d", n.id, n.persistent.CurrentTerm)
}

func (n *Node) becomeFollowerLocked(term uint64, leaderID string) {
	wasLeader := n.role == Leader
	if term > n.persistent.CurrentTerm {
		n.persistent.CurrentTerm = term
		n.persistent.VotedFor = ""
	}
	n.role = Follower
	n.leaderID = leaderID
	n.resetElectionTimerLocked()
	if wasLeader || len(n.waiters) > 0 {
		n.failWaitersLocked(&NotLeaderError{LeaderID: leaderID, LeaderURL: n.peers[leaderID]})
	}
}

func (n *Node) replicateAll() {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	peers := make([]string, 0, len(n.peers)-1)
	for peerID := range n.peers {
		if peerID != n.id {
			peers = append(peers, peerID)
		}
	}
	n.mu.Unlock()
	for _, peerID := range peers {
		go n.replicatePeer(peerID)
	}
}

func (n *Node) replicatePeer(peerID string) {
	for attempts := 0; attempts < 4; attempts++ {
		n.mu.Lock()
		if n.role != Leader {
			n.mu.Unlock()
			return
		}
		term := n.persistent.CurrentTerm
		next := n.nextIndex[peerID]
		if next == 0 {
			next = n.lastLogIndexLocked() + 1
		}
		prev := next - 1
		prevTerm := uint64(0)
		if prev > 0 {
			prevTerm = n.persistent.Log[prev-1].Term
		}
		entries := append([]Entry(nil), n.persistent.Log[next-1:]...)
		req := AppendEntriesRequest{Term: term, LeaderID: n.id, PrevLogIndex: prev,
			PrevLogTerm: prevTerm, Entries: entries, LeaderCommit: n.persistent.CommitIndex}
		url := n.peers[peerID]
		n.mu.Unlock()

		var resp AppendEntriesResponse
		if err := n.postJSON(url+"/raft/append-entries", req, &resp); err != nil {
			return
		}
		n.mu.Lock()
		if resp.Term > n.persistent.CurrentTerm {
			n.becomeFollowerLocked(resp.Term, "")
			if err := n.persistLocked(); err != nil {
				n.logger.Printf("persist newer term: %v", err)
			}
			n.mu.Unlock()
			return
		}
		if n.role != Leader || n.persistent.CurrentTerm != term {
			n.mu.Unlock()
			return
		}
		if resp.Success {
			if resp.MatchIndex > n.matchIndex[peerID] {
				n.matchIndex[peerID] = resp.MatchIndex
			}
			if resp.MatchIndex+1 > n.nextIndex[peerID] {
				n.nextIndex[peerID] = resp.MatchIndex + 1
			}
			n.advanceCommitLocked()
			n.mu.Unlock()
			return
		}
		// A newer concurrent replication may already have succeeded. Do not let
		// this older failure move that peer's progress backward.
		if req.PrevLogIndex < n.matchIndex[peerID] {
			n.mu.Unlock()
			return
		}
		if resp.ConflictIndex > 0 {
			n.nextIndex[peerID] = resp.ConflictIndex
		} else if n.nextIndex[peerID] > 1 {
			n.nextIndex[peerID]--
		}
		n.mu.Unlock()
	}
}

func (n *Node) advanceCommitLocked() {
	for index := n.lastLogIndexLocked(); index > n.persistent.CommitIndex; index-- {
		if n.persistent.Log[index-1].Term != n.persistent.CurrentTerm {
			continue
		}
		votes := 0
		for peerID := range n.peers {
			if peerID == n.id || n.matchIndex[peerID] >= index {
				votes++
			}
		}
		if votes >= len(n.peers)/2+1 {
			n.persistent.CommitIndex = index
			if err := n.persistLocked(); err != nil {
				n.logger.Printf("persist commit index: %v", err)
				return
			}
			n.applyCommittedLocked()
			return
		}
	}
}

func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.persistent.CommitIndex {
		entry := n.persistent.Log[n.lastApplied]
		var result ApplyResult
		switch entry.Command.Operation {
		case Put:
			n.kv[entry.Command.Key] = entry.Command.Value
		case Delete:
			delete(n.kv, entry.Command.Key)
		case Read:
			result.Value, result.Found = n.kv[entry.Command.Key]
		case Noop:
		}
		n.lastApplied = entry.Index
		if waiter, ok := n.waiters[entry.Index]; ok {
			waiter <- proposalResult{result: result}
			delete(n.waiters, entry.Index)
		}
	}
}

func (n *Node) failWaitersLocked(err error) {
	for index, waiter := range n.waiters {
		waiter <- proposalResult{err: err}
		delete(n.waiters, index)
	}
}

func (n *Node) resetElectionTimerLocked() {
	var b [2]byte
	_, _ = cryptorand.Read(b[:])
	jitter := time.Duration(binary.LittleEndian.Uint16(b[:])%uint16(electionJitter/time.Millisecond)) * time.Millisecond
	n.electionTimeout = electionMin + jitter
	n.electionReset = time.Now()
}

func (n *Node) lastLogIndexLocked() uint64 { return uint64(len(n.persistent.Log)) }

func (n *Node) lastLogTermLocked() uint64 {
	if len(n.persistent.Log) == 0 {
		return 0
	}
	return n.persistent.Log[len(n.persistent.Log)-1].Term
}

func (n *Node) persistLocked() error { return saveState(n.statePath, n.persistent) }

func (n *Node) postJSON(url string, request, response any) error {
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("peer returned %s", resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(response)
}
