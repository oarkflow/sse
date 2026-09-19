package sse

import "sync"

// UserHub wraps Hub and adds a userID -> []clientID index so you can
// broadcast to all browser tabs / devices of a single user.
type UserHub struct {
	*Hub
	mu       sync.RWMutex
	users    map[string]map[string]bool // userID -> set of clientIDs
	presence PresenceStore
}

// NewUserHub creates a UserHub backed by a new Hub.
func NewUserHub(opts HubOptions) *UserHub {
	return NewUserHubWithPresence(opts, nil)
}

// NewUserHubWithPresence creates a UserHub with optional shared presence storage.
func NewUserHubWithPresence(opts HubOptions, presence PresenceStore) *UserHub {
	return &UserHub{
		Hub:      NewHub(opts),
		users:    make(map[string]map[string]bool),
		presence: presence,
	}
}

// AddClient registers a client and indexes it by UserID.
func (u *UserHub) AddClient(c *Client) error {
	if err := u.Hub.AddClient(c); err != nil {
		return err
	}
	if c.UserID != "" {
		u.mu.Lock()
		if _, ok := u.users[c.UserID]; !ok {
			u.users[c.UserID] = make(map[string]bool)
		}
		u.users[c.UserID][c.ID] = true
		u.mu.Unlock()
		if u.presence != nil {
			_ = u.presence.Add(c.UserID, c.ID)
		}
	}
	return nil
}

// RemoveClient unregisters the client and cleans up the user index.
func (u *UserHub) RemoveClient(c *Client) {
	u.Hub.RemoveClient(c)
	if c.UserID != "" {
		u.mu.Lock()
		if ids, ok := u.users[c.UserID]; ok {
			delete(ids, c.ID)
			if len(ids) == 0 {
				delete(u.users, c.UserID)
			}
		}
		u.mu.Unlock()
		if u.presence != nil {
			_ = u.presence.Remove(c.UserID, c.ID)
		}
	}
}

// SendToUser delivers an event to ALL connections belonging to userID on
// this instance and, if a Bus is configured, fans it out so other instances
// deliver to their own local connections for the same user.
func (u *UserHub) SendToUser(userID string, e *Event) {
	if err := u.Hub.validateEvent(e); err != nil {
		u.Hub.rejectedEvents.Add(1)
		return
	}
	ctx := PublishContext{Target: PublishTargetUser, UserID: userID}
	if err := u.Hub.authorizePublish(ctx, e); err != nil {
		u.Hub.rejectedEvents.Add(1)
		u.Hub.logPublishDenied(PublishTargetUser, userID, err)
		return
	}
	u.Hub.persistReplay(e)
	u.sendToUserLocal(userID, e)
	u.Hub.publishToBus(ctx, e)
}

// sendToUserLocal delivers an event to locally connected clients of userID only.
func (u *UserHub) sendToUserLocal(userID string, e *Event) {
	u.mu.RLock()
	ids := make([]string, 0, len(u.users[userID]))
	for id := range u.users[userID] {
		ids = append(ids, id)
	}
	u.mu.RUnlock()

	for _, id := range ids {
		u.Hub.sendLocal(id, e) // ignore not-found; client may have disconnected
	}
}

// DispatchRemote implements Dispatcher. It handles user-targeted events
// locally and delegates every other target to the embedded Hub, so a single
// Bus subscription (go bus.Subscribe(ctx, userHub)) covers both.
func (u *UserHub) DispatchRemote(ctx PublishContext, e *Event) {
	if ctx.Target == PublishTargetUser {
		u.Hub.busDispatchedEvents.Add(1)
		u.sendToUserLocal(ctx.UserID, e)
		return
	}
	u.Hub.DispatchRemote(ctx, e)
}

// OnlineUsers returns a snapshot of currently connected user IDs.
func (u *UserHub) OnlineUsers() []string {
	if u.presence != nil {
		if users, err := u.presence.OnlineUsers(); err == nil {
			return users
		}
	}
	u.mu.RLock()
	defer u.mu.RUnlock()
	users := make([]string, 0, len(u.users))
	for uid := range u.users {
		users = append(users, uid)
	}
	return users
}

// IsOnline returns true if the user has at least one active connection.
func (u *UserHub) IsOnline(userID string) bool {
	if u.presence != nil {
		if ok, err := u.presence.IsOnline(userID); err == nil {
			return ok
		}
	}
	u.mu.RLock()
	defer u.mu.RUnlock()
	return len(u.users[userID]) > 0
}

// ConnectionCount returns how many connections a user has open.
func (u *UserHub) ConnectionCount(userID string) int {
	if u.presence != nil {
		if n, err := u.presence.ConnectionCount(userID); err == nil {
			return n
		}
	}
	u.mu.RLock()
	defer u.mu.RUnlock()
	return len(u.users[userID])
}
