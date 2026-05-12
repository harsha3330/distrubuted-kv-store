package store

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

const (
	retainSnapshotCount = 2
)

type Node struct {
	ID      string
	Address string
}

type StoreStatus struct {
	Me        Node
	Leader    Node
	Followers []Node
}

type Command struct {
	Op    string
	Key   string
	Value string
}

type Store struct {
	mu sync.Mutex
	mp map[string]string

	raft      *raft.Raft
	RaftBind  string
	RaftInmem bool
	RaftDir   string
	NodeID    string
	logger    *log.Logger
}

func NewStore() *Store {
	return &Store{
		mp:     make(map[string]string),
		logger: log.New(os.Stderr, "[STORE] ", log.LstdFlags),
	}
}

func (s *Store) Get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.mp[key]
	if !ok {
		return "", fmt.Errorf("Key Not found")
	}
	return val, nil
}

func (s *Store) Set(key, value string) error {
	bytes, err := json.Marshal(Command{
		Op:    "set",
		Key:   key,
		Value: value,
	})
	if err != nil {
		return err
	}

	return s.raft.Apply(bytes, 10*time.Second).Error()
}

func (s *Store) Delete(key string) error {
	bytes, err := json.Marshal(Command{
		Op:  "delete",
		Key: key,
	})
	if err != nil {
		return err
	}

	return s.raft.Apply(bytes, 10*time.Second).Error()
}

func (s *Store) StartRaft(enableSingle bool) error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.NodeID)

	transport, err := raft.NewTCPTransport(s.RaftBind, nil, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	var logStore raft.LogStore
	var stableStore raft.StableStore

	if s.RaftInmem {
		logStore = raft.NewInmemStore()
		stableStore = raft.NewInmemStore()
	} else {
		boltDB, err := raftboltdb.New(raftboltdb.Options{
			Path: filepath.Join(s.RaftDir, "raft.db"),
		})
		if err != nil {
			return fmt.Errorf("new bbolt store: %s", err)
		}
		logStore = boltDB
		stableStore = boltDB
	}

	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}

	s.raft = ra

	if enableSingle {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	}
	return nil
}

type fsm Store

func (f *fsm) Apply(log *raft.Log) interface{} {
	var command Command
	if err := json.Unmarshal(log.Data, &command); err != nil {
		return err
	}

	switch command.Op {
	case "set":
		return f.applySet(command.Key, command.Value)
	case "delete":
		return f.applyDelete(command.Key)
	default:
		return fmt.Errorf("unknown operation: %s", command.Op)
	}
}

func (f *fsm) applySet(key, value string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mp[key] = value
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.mp, key)
	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the map.
	o := make(map[string]string)
	for k, v := range f.mp {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

func (f *fsm) Restore(r io.ReadCloser) error {
	o := make(map[string]string)
	if err := json.NewDecoder(r).Decode(&o); err != nil {
		return err
	}
	f.mp = o
	return nil
}

type fsmSnapshot struct {
	store map[string]string
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}
		if _, err := sink.Write(b); err != nil {
			return err
		}
		return sink.Close()
	}()
	if err != nil {
		sink.Cancel()
	}
	return err
}

func (f *fsmSnapshot) Release() {}

func (s *Store) Status() (StoreStatus, error) {
	leaderServerAddr, leaderId := s.raft.LeaderWithID()
	leader := Node{
		ID:      string(leaderId),
		Address: string(leaderServerAddr),
	}

	servers := s.raft.GetConfiguration().Configuration().Servers
	followers := []Node{}
	me := Node{
		Address: s.RaftBind,
	}
	for _, server := range servers {
		if server.ID != leaderId {
			followers = append(followers, Node{
				ID:      string(server.ID),
				Address: string(server.Address),
			})
		}

		if string(server.Address) == s.RaftBind {
			me = Node{
				ID:      string(server.ID),
				Address: string(server.Address),
			}
		}
	}

	status := StoreStatus{
		Me:        me,
		Leader:    leader,
		Followers: followers,
	}

	return status, nil
}

func (s *Store) Join(nodeID, raftAddr string) error {
	s.logger.Printf("join request: node %s at %s", nodeID, raftAddr)

	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not the leader")
	}

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("failed to get raft configuration: %v", err)
		return err
	}

	for _, server := range configFuture.Configuration().Servers {
		if server.ID == raft.ServerID(nodeID) {
			s.logger.Printf("node %s already in cluster", nodeID)
			return nil
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(raftAddr), 0, 0)
	if err := f.Error(); err != nil {
		s.logger.Printf("failed to add voter: %v", err)
		return err
	}
	return nil
}
