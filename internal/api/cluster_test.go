package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/ricky-gunawan/d-key-value/internal/raft"
)

type testMember struct {
	id       string
	url      string
	listener net.Listener
	node     *raft.Node
	server   *http.Server
}

func TestThreeNodeClusterKeepsServingWithOneNodeDown(t *testing.T) {
	members := newTestCluster(t, 3)
	defer stopMembers(members)

	leader := waitForLeader(t, members, 5*time.Second)
	requestJSON(t, http.MethodPut, leader.url+"/v1/kv/course", `{"value":"distributed-systems"}`, http.StatusOK)
	response := requestJSON(t, http.MethodGet, leader.url+"/v1/kv/course", "", http.StatusOK)
	if response["value"] != "distributed-systems" {
		t.Fatalf("unexpected value: %#v", response)
	}

	var stopped *testMember
	for _, member := range members {
		if member != leader {
			stopped = member
			break
		}
	}
	stopped.node.Stop()
	_ = stopped.server.Close()
	requestJSON(t, http.MethodPut, leader.url+"/v1/kv/quorum", `{"value":"two-of-three"}`, http.StatusOK)
}

func TestLeaderFailureElectsReplacementWithoutLosingCommittedData(t *testing.T) {
	members := newTestCluster(t, 3)
	defer stopMembers(members)

	leader := waitForLeader(t, members, 5*time.Second)
	requestJSON(t, http.MethodPut, leader.url+"/v1/kv/safe", `{"value":"committed"}`, http.StatusOK)
	leader.node.Stop()
	_ = leader.server.Close()

	remaining := make([]*testMember, 0, 2)
	for _, member := range members {
		if member != leader {
			remaining = append(remaining, member)
		}
	}
	newLeader := waitForLeader(t, remaining, 5*time.Second)
	response := requestJSON(t, http.MethodGet, newLeader.url+"/v1/kv/safe", "", http.StatusOK)
	if response["value"] != "committed" {
		t.Fatalf("committed value was lost: %#v", response)
	}
}

func newTestCluster(t *testing.T, size int) []*testMember {
	t.Helper()
	members := make([]*testMember, size)
	peers := make(map[string]string, size)
	for i := range members {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		id := fmt.Sprintf("n%d", i+1)
		members[i] = &testMember{id: id, url: "http://" + listener.Addr().String(), listener: listener}
		peers[id] = members[i].url
	}
	for _, member := range members {
		node, err := raft.NewNode(member.id, peers, filepath.Join(t.TempDir(), member.id, "state.json"), log.New(io.Discard, "", 0))
		if err != nil {
			t.Fatalf("new node %s: %v", member.id, err)
		}
		member.node = node
		member.server = &http.Server{Handler: NewServer(node)}
		go func(m *testMember) {
			if err := m.server.Serve(m.listener); err != nil && err != http.ErrServerClosed {
				t.Errorf("serve %s: %v", m.id, err)
			}
		}(member)
	}
	for _, member := range members {
		member.node.Start()
	}
	return members
}

func stopMembers(members []*testMember) {
	for _, member := range members {
		if member.node != nil {
			member.node.Stop()
		}
		if member.server != nil {
			_ = member.server.Close()
		}
	}
}

func waitForLeader(t *testing.T, members []*testMember, timeout time.Duration) *testMember {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []*testMember
		for _, member := range members {
			if member.node.Status().Role == raft.Leader {
				leaders = append(leaders, member)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, member := range members {
		t.Logf("%s: %+v", member.id, member.node.Status())
	}
	t.Fatal("cluster did not elect exactly one leader")
	return nil
}

func requestJSON(t *testing.T, method, url, body string, wantStatus int) map[string]string {
	t.Helper()
	request, err := http.NewRequest(method, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	if response.StatusCode != wantStatus {
		t.Fatalf("%s %s returned %d, want %d: %s", method, url, response.StatusCode, wantStatus, data)
	}
	if len(data) == 0 {
		return nil
	}
	var result map[string]string
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("decode response %q: %v", data, err)
	}
	return result
}
