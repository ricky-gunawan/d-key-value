package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ricky-gunawan/d-key-value/internal/api"
	"github.com/ricky-gunawan/d-key-value/internal/raft"
)

func main() {
	var id, listen, peersFlag, dataDir string
	flag.StringVar(&id, "id", "", "unique node id (required)")
	flag.StringVar(&listen, "listen", ":9001", "HTTP listen address")
	flag.StringVar(&peersFlag, "peers", "", "comma-separated id=url cluster members (required)")
	flag.StringVar(&dataDir, "data-dir", "data", "directory for durable Raft state")
	flag.Parse()

	peers, err := parsePeers(peersFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid -peers:", err)
		os.Exit(2)
	}
	logger := log.New(os.Stderr, "dkv ", log.LstdFlags|log.Lmicroseconds)
	node, err := raft.NewNode(id, peers, filepath.Join(dataDir, "raft-state.json"), logger)
	if err != nil {
		logger.Printf("create node: %v", err)
		os.Exit(1)
	}
	node.Start()
	defer node.Stop()

	server := &http.Server{
		Addr: listen, Handler: api.NewServer(node),
		ReadHeaderTimeout: 2 * time.Second,
	}
	go func() {
		logger.Printf("node %s listening on %s with %d members", id, listen, len(peers))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Printf("HTTP server: %v", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}

func parsePeers(value string) (map[string]string, error) {
	peers := make(map[string]string)
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		id, address, ok := strings.Cut(item, "=")
		if !ok || strings.TrimSpace(id) == "" || strings.TrimSpace(address) == "" {
			return nil, fmt.Errorf("expected id=url, got %q", item)
		}
		if !strings.HasPrefix(address, "http://") && !strings.HasPrefix(address, "https://") {
			return nil, fmt.Errorf("peer %q URL must start with http:// or https://", id)
		}
		if _, exists := peers[id]; exists {
			return nil, fmt.Errorf("duplicate node id %q", id)
		}
		peers[id] = strings.TrimRight(address, "/")
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("at least one peer is required")
	}
	return peers, nil
}
