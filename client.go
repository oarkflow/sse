package sse

import (
	"sync"
	"sync/atomic"
	"time"
)

// Client represents a single SSE connection.
type Client struct {
	// ID is the unique identifier for this connection.
	ID string

	// UserID is an optional application-level user identifier.
	// Multiple clients can share the same UserID (multi-tab scenario).
	UserID string

	// Metadata holds arbitrary key-value data attached to this client.
	Metadata map[string]any

	// topics is the set of topics this client subscribes to for sticky replay.
	// Empty means "all topics".
	topics map[string]bool

	// groups this client belongs to.
	groups   map[string]bool
	groupsMu sync.RWMutex

	// send is the outbound event channel.
	send chan *Event

	// ConnectedAt records when the client connected.
	ConnectedAt time.Time

	// LastEventID is the ID of the last event the client acknowledged.
	LastEventID string

	droppedConsecutive atomic.Uint64
}

// ClientOptions configures a new Client.
type ClientOptions struct {
	ID          string
	UserID      string
	Metadata    map[string]any
	Topics      []string
	BufferSize  int
	LastEventID string
}

// NewClient creates a Client. If opts.ID is empty, a UUID is generated.
func NewClient(opts ClientOptions) *Client {
	if opts.ID == "" {
		opts.ID = newID()
	}
	topics := make(map[string]bool, len(opts.Topics))
	for _, t := range opts.Topics {
		topics[t] = true
	}
	bufSize := opts.BufferSize
	if bufSize == 0 {
		bufSize = 64
	}
	return &Client{
		ID:          opts.ID,
		UserID:      opts.UserID,
		Metadata:    opts.Metadata,
		topics:      topics,
		groups:      make(map[string]bool),
		send:        make(chan *Event, bufSize),
		ConnectedAt: time.Now(),
		LastEventID: opts.LastEventID,
	}
}

// Recv returns the client's inbound event channel (read-only).
// Useful in tests and for custom streaming implementations.
func (c *Client) Recv() <-chan *Event {
	return c.send
}

// Groups returns a snapshot of the groups this client belongs to.
func (c *Client) Groups() []string {
	c.groupsMu.RLock()
	defer c.groupsMu.RUnlock()
	gs := make([]string, 0, len(c.groups))
	for g := range c.groups {
		gs = append(gs, g)
	}
	return gs
}
