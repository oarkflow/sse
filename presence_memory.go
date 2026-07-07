package sse

import "sync"

// MemoryPresenceStore is an in-process user presence tracker.
type MemoryPresenceStore struct {
	mu    sync.RWMutex
	users map[string]map[string]bool
}

func NewMemoryPresenceStore() *MemoryPresenceStore {
	return &MemoryPresenceStore{
		users: make(map[string]map[string]bool),
	}
}

func (s *MemoryPresenceStore) Add(userID, clientID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[userID]; !ok {
		s.users[userID] = make(map[string]bool)
	}
	s.users[userID][clientID] = true
	return nil
}

func (s *MemoryPresenceStore) Remove(userID, clientID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ids, ok := s.users[userID]; ok {
		delete(ids, clientID)
		if len(ids) == 0 {
			delete(s.users, userID)
		}
	}
	return nil
}

func (s *MemoryPresenceStore) IsOnline(userID string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.users[userID]) > 0, nil
}

func (s *MemoryPresenceStore) ConnectionCount(userID string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.users[userID]), nil
}

func (s *MemoryPresenceStore) OnlineUsers() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	users := make([]string, 0, len(s.users))
	for uid := range s.users {
		users = append(users, uid)
	}
	return users, nil
}
