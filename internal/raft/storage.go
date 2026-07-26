package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type persistentState struct {
	CurrentTerm uint64  `json:"current_term"`
	VotedFor    string  `json:"voted_for,omitempty"`
	Log         []Entry `json:"log"`
	CommitIndex uint64  `json:"commit_index"`
}

func loadState(path string) (persistentState, error) {
	var state persistentState
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return state, fmt.Errorf("read raft state: %w", err)
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return state, fmt.Errorf("decode raft state: %w", err)
	}
	for i, entry := range state.Log {
		if entry.Index != uint64(i+1) {
			return state, fmt.Errorf("invalid log index %d at position %d", entry.Index, i)
		}
	}
	if state.CommitIndex > uint64(len(state.Log)) {
		return state, fmt.Errorf("commit index %d is past log end %d", state.CommitIndex, len(state.Log))
	}
	return state, nil
}

func saveState(path string, state persistentState) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode raft state: %w", err)
	}
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open temporary raft state: %w", err)
	}
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return fmt.Errorf("write raft state: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("close raft state: %w", closeErr)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace raft state: %w", err)
	}
	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open raft state directory: %w", err)
	}
	err = directory.Sync()
	closeErr = directory.Close()
	if err != nil {
		return fmt.Errorf("sync raft state directory: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("close raft state directory: %w", closeErr)
	}
	return nil
}
