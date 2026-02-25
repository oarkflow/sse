// Package sse provides a robust Server-Sent Events (SSE) implementation
// for the GoFiber framework with sticky events, client groups, and
// targeted delivery.
package sse

import (
	"sync"
	"sync/atomic"
	"time"
)

// Hub manages all connected SSE clients and provides broadcast,
// targeted, and group-based notification capabilities.
type Hub struct {
	mu      sync.RWMutex
	clients map[string]*Client         // clientID -> Client
	groups  map[string]map[string]bool // groupName -> set of clientIDs

	// Sticky events: last N events per topic are replayed to new subscribers.
	stickyMu     sync.RWMutex
	stickyEvents map[string][]*Event // topic -> events (capped at stickySize)
	stickySize   int

	// Internal channels for serialized mutation.
	register   chan *Client
	unregister chan *Client

	opts HubOptions
	done chan struct{}

	stopOnce sync.Once

	closed   atomic.Bool
	draining atomic.Bool

	deliveredEvents     atomic.Uint64
	droppedEvents       atomic.Uint64
	slowConsumerDrops   atomic.Uint64
	rejectedConnections atomic.Uint64
	replayedEvents      atomic.Uint64
	replayMisses        atomic.Uint64
	rejectedEvents      atomic.Uint64
}

// HubOptions configures the Hub behaviour.
type HubOptions struct {
	// StickySize is the number of events to retain per topic for replay.
	// Default: 10.
	StickySize int

	// ClientBufferSize is the channel buffer size per client.
	// Default: 64.
	ClientBufferSize int

	// HeartbeatInterval controls how often a keep-alive comment is sent.
	// Set to 0 to disable. Default: 30s.
	HeartbeatInterval time.Duration

	// MaxClients is the maximum number of simultaneous connections (0 = unlimited).
	MaxClients int

	// ReplayBufferSize is the fallback in-memory replay store capacity.
	// Default: 1000.
	ReplayBufferSize int

	// ReplayLimitPerConnect caps replayed events for a reconnecting client.
	// Default: 500.
	ReplayLimitPerConnect int

	// ReplayStore is an optional custom store used for Last-Event-ID replay.
	// If nil, an in-memory replay store is used.
	ReplayStore ReplayStore

	// BackpressurePolicy defines behavior when a client buffer is full.
	// Default: BackpressureDropNewest.
	BackpressurePolicy BackpressurePolicy

	// MaxGroupsPerClient limits how many groups one client can join.
	// 0 means unlimited.
	MaxGroupsPerClient int

	// MaxEventBytes limits the UTF-8 size of the event data payload.
	// Default: 64 KiB.
	MaxEventBytes int

	// MaxEventTypeLength limits event type length.
	// Default: 128.
	MaxEventTypeLength int
}

func defaultOptions(o HubOptions) HubOptions {
	if o.StickySize == 0 {
		o.StickySize = 10
	}
	if o.ClientBufferSize == 0 {
		o.ClientBufferSize = 64
	}
	if o.HeartbeatInterval == 0 {
		o.HeartbeatInterval = 30 * time.Second
	}
	if o.ReplayBufferSize == 0 {
		o.ReplayBufferSize = 1000
	}
	if o.ReplayLimitPerConnect == 0 {
		o.ReplayLimitPerConnect = 500
	}
	if o.BackpressurePolicy == "" {
		o.BackpressurePolicy = BackpressureDropNewest
	}
	if o.BackpressurePolicy != BackpressureDropNewest &&
		o.BackpressurePolicy != BackpressureDropOldest &&
		o.BackpressurePolicy != BackpressureDisconnectSlow {
		o.BackpressurePolicy = BackpressureDropNewest
	}
	if o.MaxEventBytes == 0 {
		o.MaxEventBytes = 64 * 1024
	}
	if o.MaxEventTypeLength == 0 {
		o.MaxEventTypeLength = 128
	}
	return o
}

// NewHub creates and starts a new Hub.
func NewHub(opts HubOptions) *Hub {
	opts = defaultOptions(opts)

	if opts.ReplayStore == nil {
		opts.ReplayStore = NewInMemoryReplayStore(opts.ReplayBufferSize)
	}

	h := &Hub{
		clients:      make(map[string]*Client),
		groups:       make(map[string]map[string]bool),
		stickyEvents: make(map[string][]*Event),
		stickySize:   opts.StickySize,
		register:     make(chan *Client, 64),
		unregister:   make(chan *Client, 64),
		opts:         opts,
		done:         make(chan struct{}),
	}
	go h.run()
	return h
}

// run is the Hub's main event loop.
func (h *Hub) run() {
	var heartbeat <-chan time.Time
	if h.opts.HeartbeatInterval > 0 {
		t := time.NewTicker(h.opts.HeartbeatInterval)
		defer t.Stop()
		heartbeat = t.C
	}

	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client.ID] = client
			h.mu.Unlock()

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client.ID]; ok {
				delete(h.clients, client.ID)
				for group, members := range h.groups {
					delete(members, client.ID)
					if len(members) == 0 {
						delete(h.groups, group)
					}
				}
				close(client.send)
			}
			h.mu.Unlock()

		case <-heartbeat:
			h.mu.RLock()
			for _, c := range h.clients {
				select {
				case c.send <- heartbeatEvent():
				default:
				}
			}
			h.mu.RUnlock()

		case <-h.done:
			h.mu.Lock()
			for _, c := range h.clients {
				close(c.send)
			}
			h.clients = make(map[string]*Client)
			h.groups = make(map[string]map[string]bool)
			h.mu.Unlock()
			h.closed.Store(true)
			return
		}
	}
}

// StartDraining rejects new clients while keeping existing streams active.
func (h *Hub) StartDraining() {
	h.draining.Store(true)
}

// IsDraining returns true when the hub is in drain mode.
func (h *Hub) IsDraining() bool {
	return h.draining.Load()
}

// IsClosed returns true after hub shutdown has completed.
func (h *Hub) IsClosed() bool {
	return h.closed.Load()
}

// Drain enters drain mode and waits for disconnects until timeout, then stops.
func (h *Hub) Drain(timeout time.Duration) {
	h.StartDraining()
	if timeout <= 0 {
		h.Stop()
		return
	}

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if h.Stats().ConnectedClients == 0 {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		<-ticker.C
	}
	h.Stop()
}

// Stop shuts down the hub and disconnects all clients.
func (h *Hub) Stop() {
	h.stopOnce.Do(func() {
		close(h.done)
	})
}

// AddClient registers a new client with the hub and replays sticky/recent events.
func (h *Hub) AddClient(c *Client) error {
	select {
	case <-h.done:
		h.rejectedConnections.Add(1)
		return ErrHubClosed
	default:
	}

	if h.IsDraining() {
		h.rejectedConnections.Add(1)
		return ErrHubDraining
	}

	if h.opts.MaxClients > 0 {
		h.mu.RLock()
		count := len(h.clients)
		h.mu.RUnlock()
		if count >= h.opts.MaxClients {
			h.rejectedConnections.Add(1)
			return ErrMaxClientsReached
		}
	}

	select {
	case h.register <- c:
	case <-h.done:
		h.rejectedConnections.Add(1)
		return ErrHubClosed
	}

	if c.LastEventID != "" {
		go h.replayFromStore(c)
	} else {
		go h.replaySticky(c)
	}
	return nil
}

// RemoveClient unregisters a client.
func (h *Hub) RemoveClient(c *Client) {
	select {
	case h.unregister <- c:
	case <-h.done:
	}
}

func (h *Hub) replayFromStore(c *Client) {
	time.Sleep(25 * time.Millisecond)
	if h.opts.ReplayStore == nil {
		h.replaySticky(c)
		return
	}
	events := h.opts.ReplayStore.ReplayAfter(c.LastEventID, c.topics, h.opts.ReplayLimitPerConnect)
	if len(events) == 0 {
		h.replayMisses.Add(1)
		h.replaySticky(c)
		return
	}
	for _, event := range events {
		select {
		case c.send <- event:
			h.replayedEvents.Add(1)
		default:
		}
	}
}

// replaySticky sends retained events to a newly connected client.
func (h *Hub) replaySticky(c *Client) {
	time.Sleep(50 * time.Millisecond)

	h.stickyMu.RLock()
	defer h.stickyMu.RUnlock()

	for topic, events := range h.stickyEvents {
		if len(c.topics) > 0 && !c.topics[topic] {
			continue
		}
		for _, e := range events {
			select {
			case c.send <- e:
				h.replayedEvents.Add(1)
			default:
			}
		}
	}
}

// Broadcast sends an event to every connected client.
func (h *Hub) Broadcast(e *Event) {
	if err := h.validateEvent(e); err != nil {
		h.rejectedEvents.Add(1)
		return
	}
	h.persistSticky(e)
	h.persistReplay(e)
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, c := range h.clients {
		h.deliver(c, e)
	}
}

// Send delivers an event to a single client identified by clientID.
// Returns ErrClientNotFound if the client is not connected.
func (h *Hub) Send(clientID string, e *Event) error {
	if err := h.validateEvent(e); err != nil {
		h.rejectedEvents.Add(1)
		return err
	}
	h.persistReplay(e)
	h.mu.RLock()
	c, ok := h.clients[clientID]
	h.mu.RUnlock()
	if !ok {
		return ErrClientNotFound
	}
	h.deliver(c, e)
	return nil
}

// SendToGroup delivers an event to all members of a named group.
func (h *Hub) SendToGroup(group string, e *Event) {
	if err := h.validateEvent(e); err != nil {
		h.rejectedEvents.Add(1)
		return
	}
	h.persistSticky(e)
	h.persistReplay(e)
	h.mu.RLock()
	members := h.groups[group]
	clients := make([]*Client, 0, len(members))
	for id := range members {
		if c, ok := h.clients[id]; ok {
			clients = append(clients, c)
		}
	}
	h.mu.RUnlock()

	for _, c := range clients {
		h.deliver(c, e)
	}
}

// BroadcastExcept sends to all clients except the ones listed.
func (h *Hub) BroadcastExcept(e *Event, excludeIDs ...string) {
	if err := h.validateEvent(e); err != nil {
		h.rejectedEvents.Add(1)
		return
	}
	excluded := make(map[string]bool, len(excludeIDs))
	for _, id := range excludeIDs {
		excluded[id] = true
	}
	h.persistSticky(e)
	h.persistReplay(e)
	h.mu.RLock()
	defer h.mu.RUnlock()
	for id, c := range h.clients {
		if !excluded[id] {
			h.deliver(c, e)
		}
	}
}

// Disconnect disconnects a client by ID.
func (h *Hub) Disconnect(clientID string) error {
	h.mu.RLock()
	client, ok := h.clients[clientID]
	h.mu.RUnlock()
	if !ok {
		return ErrClientNotFound
	}
	h.RemoveClient(client)
	return nil
}

// deliver non-blockingly pushes an event into a client's send buffer.
func (h *Hub) deliver(c *Client, e *Event) {
	select {
	case c.send <- e:
		h.deliveredEvents.Add(1)
		return
	default:
	}

	switch h.opts.BackpressurePolicy {
	case BackpressureDropOldest:
		select {
		case <-c.send:
		default:
		}
		select {
		case c.send <- e:
			h.deliveredEvents.Add(1)
		default:
			h.droppedEvents.Add(1)
		}
	case BackpressureDisconnectSlow:
		h.slowConsumerDrops.Add(1)
		h.droppedEvents.Add(1)
		h.RemoveClient(c)
	default:
		h.droppedEvents.Add(1)
	}
}

// JoinGroup adds a client to a named group, creating it if needed.
func (h *Hub) JoinGroup(clientID, group string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if c, ok := h.clients[clientID]; ok && h.opts.MaxGroupsPerClient > 0 {
		c.groupsMu.RLock()
		groupCount := len(c.groups)
		c.groupsMu.RUnlock()
		if groupCount >= h.opts.MaxGroupsPerClient && !c.groups[group] {
			return ErrClientGroupLimitExceeded
		}
	}

	if _, ok := h.groups[group]; !ok {
		h.groups[group] = make(map[string]bool)
	}
	h.groups[group][clientID] = true

	if c, ok := h.clients[clientID]; ok {
		c.groupsMu.Lock()
		c.groups[group] = true
		c.groupsMu.Unlock()
	}
	return nil
}

// LeaveGroup removes a client from a named group.
func (h *Hub) LeaveGroup(clientID, group string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if members, ok := h.groups[group]; ok {
		delete(members, clientID)
		if len(members) == 0 {
			delete(h.groups, group)
		}
	}
	if c, ok := h.clients[clientID]; ok {
		c.groupsMu.Lock()
		delete(c.groups, group)
		c.groupsMu.Unlock()
	}
}

// GroupMembers returns a snapshot of client IDs in a group.
func (h *Hub) GroupMembers(group string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	members := h.groups[group]
	ids := make([]string, 0, len(members))
	for id := range members {
		ids = append(ids, id)
	}
	return ids
}

// persistSticky stores the event in the sticky ring-buffer for its topic.
func (h *Hub) persistSticky(e *Event) {
	if !e.Sticky || e.Topic == "" {
		return
	}
	h.stickyMu.Lock()
	defer h.stickyMu.Unlock()
	bucket := h.stickyEvents[e.Topic]
	bucket = append(bucket, e)
	if len(bucket) > h.stickySize {
		bucket = bucket[len(bucket)-h.stickySize:]
	}
	h.stickyEvents[e.Topic] = bucket
}

func (h *Hub) persistReplay(e *Event) {
	if h.opts.ReplayStore != nil {
		h.opts.ReplayStore.Append(e)
	}
}

// ValidateEvent validates an event against configured hub limits.
func (h *Hub) ValidateEvent(e *Event) error {
	return h.validateEvent(e)
}

func (h *Hub) validateEvent(e *Event) error {
	if e == nil {
		return ErrInvalidEvent
	}
	if h.opts.MaxEventTypeLength > 0 && len(e.Type) > h.opts.MaxEventTypeLength {
		return ErrInvalidEvent
	}
	if h.opts.MaxEventBytes > 0 && len(e.Data) > h.opts.MaxEventBytes {
		return ErrEventTooLarge
	}
	return nil
}

// ClearSticky removes all sticky events for a topic.
func (h *Hub) ClearSticky(topic string) {
	h.stickyMu.Lock()
	defer h.stickyMu.Unlock()
	delete(h.stickyEvents, topic)
}

// Stats returns a snapshot of hub metrics.
type Stats struct {
	ConnectedClients int
	Groups           map[string]int
	Draining         bool
	Closed           bool

	DeliveredEvents     uint64
	DroppedEvents       uint64
	SlowConsumerDrops   uint64
	RejectedConnections uint64
	ReplayedEvents      uint64
	ReplayMisses        uint64
	RejectedEvents      uint64
}

func (h *Hub) Stats() Stats {
	h.mu.RLock()
	defer h.mu.RUnlock()
	groups := make(map[string]int, len(h.groups))
	for g, members := range h.groups {
		groups[g] = len(members)
	}
	return Stats{
		ConnectedClients:    len(h.clients),
		Groups:              groups,
		Draining:            h.IsDraining(),
		Closed:              h.IsClosed(),
		DeliveredEvents:     h.deliveredEvents.Load(),
		DroppedEvents:       h.droppedEvents.Load(),
		SlowConsumerDrops:   h.slowConsumerDrops.Load(),
		RejectedConnections: h.rejectedConnections.Load(),
		ReplayedEvents:      h.replayedEvents.Load(),
		ReplayMisses:        h.replayMisses.Load(),
		RejectedEvents:      h.rejectedEvents.Load(),
	}
}
