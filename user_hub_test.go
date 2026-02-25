package sse

import "testing"

type mockPresenceStore struct {
	added map[string]map[string]bool
}

func (m *mockPresenceStore) Add(userID, clientID string) error {
	if m.added == nil {
		m.added = make(map[string]map[string]bool)
	}
	if _, ok := m.added[userID]; !ok {
		m.added[userID] = make(map[string]bool)
	}
	m.added[userID][clientID] = true
	return nil
}

func (m *mockPresenceStore) Remove(userID, clientID string) error {
	if users, ok := m.added[userID]; ok {
		delete(users, clientID)
		if len(users) == 0 {
			delete(m.added, userID)
		}
	}
	return nil
}

func (m *mockPresenceStore) IsOnline(userID string) (bool, error) {
	return len(m.added[userID]) > 0, nil
}

func (m *mockPresenceStore) ConnectionCount(userID string) (int, error) {
	return len(m.added[userID]), nil
}

func (m *mockPresenceStore) OnlineUsers() ([]string, error) {
	out := make([]string, 0, len(m.added))
	for userID := range m.added {
		out = append(out, userID)
	}
	return out, nil
}

func TestUserHubPresenceStoreIntegration(t *testing.T) {
	p := &mockPresenceStore{}
	hub := NewUserHubWithPresence(HubOptions{HeartbeatInterval: 0}, p)
	defer hub.Stop()

	c := NewClient(ClientOptions{ID: "c1", UserID: "u1", BufferSize: 8})
	if err := hub.AddClient(c); err != nil {
		t.Fatalf("unexpected add error: %v", err)
	}
	if !hub.IsOnline("u1") {
		t.Fatalf("expected user online")
	}
	if hub.ConnectionCount("u1") != 1 {
		t.Fatalf("expected single connection")
	}

	hub.RemoveClient(c)
	if hub.IsOnline("u1") {
		t.Fatalf("expected user offline after remove")
	}
}
