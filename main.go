package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	httpd "github.com/harsha3330/distubuted-kv-store/http"
	"github.com/harsha3330/distubuted-kv-store/store"
)

type Config struct {
	httpAddr string
	raftAddr string
	inmem    bool
	joinAddr string
	nodeID   string
}

var config Config

const (
	DefaultHTTPAddr = "localhost:11000"
	DefaultRaftAddr = "localhost:12000"
)

func init() {
	flag.StringVar(&config.httpAddr, "haddr", DefaultHTTPAddr, "Set the HTTP bind address")
	flag.StringVar(&config.raftAddr, "raddr", DefaultRaftAddr, "Set the Raft bind address")
	flag.BoolVar(&config.inmem, "inmem", true, "Use in-memory raft")
	flag.StringVar(&config.joinAddr, "join", "", "HTTP address of an existing cluster member to join")
	flag.StringVar(&config.nodeID, "id", "", "Set the node ID")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	logger := log.New(os.Stdout, "[main] ", log.LstdFlags|log.Lmsgprefix)

	flag.Parse()
	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "No raft directory provided")
		flag.Usage()
		os.Exit(1)
	}
	raftDir := flag.Arg(0)

	if config.nodeID == "" {
		config.nodeID = config.raftAddr
	}

	raftDataDir := filepath.Join(raftDir, "raft")
	logger.Printf("starting node id=%q http=%s raft=%s data=%s inmem=%t",
		config.nodeID, config.httpAddr, config.raftAddr, raftDataDir, config.inmem)

	if err := os.MkdirAll(raftDataDir, 0755); err != nil {
		logger.Fatalf("create raft dir %q: %v", raftDataDir, err)
	}

	s := store.NewStore()
	s.RaftInmem = config.inmem
	s.RaftBind = config.raftAddr
	s.RaftDir = raftDataDir
	s.NodeID = config.nodeID

	bootstrap := config.joinAddr == ""
	logger.Printf("starting raft bootstrap=%t", bootstrap)
	if err := s.StartRaft(bootstrap); err != nil {
		logger.Fatalf("start raft: %v", err)
	}

	server := httpd.NewServer(config.httpAddr)
	server.Store = s

	go func() {
		if err := server.Start(); err != nil {
			logger.Fatalf("http server: %v", err)
		}
	}()

	if config.joinAddr != "" {
		logger.Printf("joining cluster via %s", config.joinAddr)
		if err := joinCluster(config.joinAddr, config.nodeID, config.raftAddr); err != nil {
			logger.Fatalf("join cluster: %v", err)
		}
		logger.Printf("joined cluster")
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	logger.Printf("received signal %s, shutting down", sig)

	os.Exit(0)
}

func joinCluster(leaderHTTPAddr, nodeID, raftAddr string) error {
	body, err := json.Marshal(map[string]string{
		"nodeID":   nodeID,
		"raftAddr": raftAddr,
	})
	if err != nil {
		return err
	}

	url := fmt.Sprintf("http://%s/join", leaderHTTPAddr)
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("join failed: %s: %s", resp.Status, msg)
	}
	return nil
}
