package sse

// PresenceStore coordinates user online presence across instances.
type PresenceStore interface {
	Add(userID, clientID string) error
	Remove(userID, clientID string) error
	IsOnline(userID string) (bool, error)
	ConnectionCount(userID string) (int, error)
	OnlineUsers() ([]string, error)
}
