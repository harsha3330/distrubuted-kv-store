package store

import (
	"fmt"
	"sync"
)

type Store struct {
	mu sync.Mutex
	mp map[string]string
}

func NewStore() *Store {
	return &Store{
		mp: make(map[string]string),
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
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mp[key] = value
	return nil
}

func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.mp, key)
	return nil
}
