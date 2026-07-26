package raft

import (
	"io"
	"log"
	"path/filepath"
	"reflect"
	"testing"
)

func TestStateRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.json")
	want := persistentState{
		CurrentTerm: 4,
		VotedFor:    "n2",
		CommitIndex: 1,
		Log: []Entry{{
			Index: 1,
			Term:  3,
			Command: Command{
				Operation: Put,
				Key:       "language",
				Value:     "go",
			},
		}},
	}
	if err := saveState(path, want); err != nil {
		t.Fatalf("saveState: %v", err)
	}
	got, err := loadState(path)
	if err != nil {
		t.Fatalf("loadState: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("loaded state differs\ngot:  %#v\nwant: %#v", got, want)
	}
}

func TestLoadRejectsInvalidLogIndexes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.json")
	bad := persistentState{Log: []Entry{{Index: 2, Term: 1}}}
	if err := saveState(path, bad); err != nil {
		t.Fatalf("saveState: %v", err)
	}
	if _, err := loadState(path); err == nil {
		t.Fatal("loadState accepted a non-contiguous log")
	}
}

func TestNewNodeReplaysOnlyCommittedEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.json")
	state := persistentState{
		CurrentTerm: 2,
		CommitIndex: 1,
		Log: []Entry{
			{Index: 1, Term: 1, Command: Command{Operation: Put, Key: "committed", Value: "yes"}},
			{Index: 2, Term: 2, Command: Command{Operation: Put, Key: "uncommitted", Value: "no"}},
		},
	}
	if err := saveState(path, state); err != nil {
		t.Fatalf("saveState: %v", err)
	}
	node, err := NewNode("n1", map[string]string{"n1": "http://127.0.0.1:9001"}, path, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if got := node.kv["committed"]; got != "yes" {
		t.Fatalf("committed value = %q, want yes", got)
	}
	if _, exists := node.kv["uncommitted"]; exists {
		t.Fatal("recovery applied an uncommitted log entry")
	}
	if node.lastApplied != 1 {
		t.Fatalf("lastApplied = %d, want 1", node.lastApplied)
	}
}
