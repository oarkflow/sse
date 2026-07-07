// Package sse provides a robust Server-Sent Events (SSE) implementation
// for the fh framework with sticky events, client groups, and
// targeted delivery.
//
// SSE is a server push technology enabling a client to receive automatic
// updates from a server via HTTP connection. This package provides a
// production-ready implementation with features like:
//
//   - Sticky events for late-joiner replay
//   - Last-Event-ID based reconnection replay
//   - Client groups (rooms/channels) for targeted broadcasting
//   - User-level targeting for multi-device support
//   - Configurable backpressure policies
//   - Graceful shutdown with drain mode
//   - Comprehensive security controls (auth, CORS, rate limiting)
//   - Built-in metrics and health endpoints
//
// # Quick Start
//
//	hub := sse.NewHub(sse.HubOptions{
//	    StickySize:        20,
//	    HeartbeatInterval: 30 * time.Second,
//	})
//	defer hub.Stop()
//
//	app.Get("/events", sse.Handler(hub, sse.HandlerOptions{}))
//
//	// Broadcast to all clients
//	hub.Broadcast(sse.NewEvent("message", "Hello World"))
//
// # Event Delivery
//
// The Hub provides several delivery methods:
//
//	hub.Broadcast(event)                    // All clients
//	hub.Send(clientID, event)               // Specific client
//	hub.SendToGroup("room", event)          // Group members
//	hub.BroadcastExcept(event, excludeIDs)  // All except some
//
// # Sticky Events
//
// Events marked as sticky are retained and replayed to new subscribers:
//
//	hub.Broadcast(sse.NewEvent("status", data).WithTopic("status"))
//
// # Groups
//
//	hub.JoinGroup(clientID, "room-1")
//	hub.SendToGroup("room-1", event)
//	hub.LeaveGroup(clientID, "room-1")
package sse

import (
	"errors"
	"log/slog"
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
	register   chan registerRequest
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

	slowLogLimiter *logRateLimiter
}

type registerRequest struct {
	client *Client
	result chan error
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

	// MaxTopicLength limits topic identifier length.
	// Default: 128.
	MaxTopicLength int

	// MaxGroupLength limits group identifier length.
	// Default: 128.
	MaxGroupLength int

	// MaxDropsPerClient disconnects a client after this many consecutive drops.
	// 0 disables threshold-based disconnect.
	MaxDropsPerClient int

	// SlowConsumerLogInterval rate-limits slow-consumer log lines.
	// Set to 0 to disable rate limiting.
	SlowConsumerLogInterval time.Duration

	// AuthorizePublish can deny event publishing for broadcast/send/group paths.
	AuthorizePublish func(PublishContext, *Event) error

	// IdentifierValidator optionally overrides topic/group identifier validation.
	IdentifierValidator func(kind, value string) error

	// OnLifecycle receives drain/stop lifecycle events for observability.
	OnLifecycle func(HubLifecycleEvent)

	// Logger is used for slow-consumer and authorization-denial logs.
	Logger *slog.Logger
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
	if o.MaxTopicLength == 0 {
		o.MaxTopicLength = 128
	}
	if o.MaxGroupLength == 0 {
		o.MaxGroupLength = 128
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
		clients:        make(map[string]*Client),
		groups:         make(map[string]map[string]bool),
		stickyEvents:   make(map[string][]*Event),
		stickySize:     opts.StickySize,
		register:       make(chan registerRequest, 64),
		unregister:     make(chan *Client, 64),
		opts:           opts,
		done:           make(chan struct{}),
		slowLogLimiter: newLogRateLimiter(opts.SlowConsumerLogInterval),
	}
	if h.opts.Logger == nil {
		h.opts.Logger = slog.Default()
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
		case req := <-h.register:
			client := req.client
			var regErr error
			h.mu.Lock()
			if _, exists := h.clients[client.ID]; exists {
				regErr = ErrClientAlreadyExists
			} else if h.opts.MaxClients > 0 && len(h.clients) >= h.opts.MaxClients {
				regErr = ErrMaxClientsReached
			} else {
				h.clients[client.ID] = client
			}
			h.mu.Unlock()
			req.result <- regErr

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
			connected := len(h.clients)
			for _, c := range h.clients {
				close(c.send)
			}
			h.clients = make(map[string]*Client)
			h.groups = make(map[string]map[string]bool)
			h.mu.Unlock()
			h.closed.Store(true)
			h.emitLifecycle(HubLifecycleEvent{
				Type:             LifecycleStopCompleted,
				OccurredAt:       time.Now(),
				ConnectedClients: connected,
				Reason:           "hub_closed",
			})
			return
		}
	}
}

// StartDraining rejects new clients while keeping existing streams active.
func (h *Hub) StartDraining() {
	if h.draining.CompareAndSwap(false, true) {
		h.emitLifecycle(HubLifecycleEvent{
			Type:             LifecycleDrainStarted,
			OccurredAt:       time.Now(),
			ConnectedClients: h.Stats().ConnectedClients,
		})
	}
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
	start := time.Now()
	if timeout <= 0 {
		h.Stop()
		h.emitLifecycle(HubLifecycleEvent{
			Type:             LifecycleDrainCompleted,
			OccurredAt:       time.Now(),
			ConnectedClients: h.Stats().ConnectedClients,
			Duration:         time.Since(start),
			Timeout:          timeout,
			Reason:           "stop_immediate",
		})
		return
	}

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	reason := "all_clients_disconnected"
	for {
		if h.Stats().ConnectedClients == 0 {
			break
		}
		if time.Now().After(deadline) {
			reason = "timeout"
			break
		}
		<-ticker.C
	}
	h.Stop()
	h.emitLifecycle(HubLifecycleEvent{
		Type:             LifecycleDrainCompleted,
		OccurredAt:       time.Now(),
		ConnectedClients: h.Stats().ConnectedClients,
		Duration:         time.Since(start),
		Timeout:          timeout,
		Reason:           reason,
	})
}

// Stop shuts down the hub and disconnects all clients.
func (h *Hub) Stop() {
	h.stopOnce.Do(func() {
		h.emitLifecycle(HubLifecycleEvent{
			Type:             LifecycleStopStarted,
			OccurredAt:       time.Now(),
			ConnectedClients: h.Stats().ConnectedClients,
		})
		close(h.done)
	})
}

// AddClient registers a new client with the hub and replays sticky/recent events.
func (h *Hub) AddClient(c *Client) error {
	if c == nil {
		h.rejectedConnections.Add(1)
		return ErrClientNotFound
	}
	for topic := range c.topics {
		if err := h.validateTopic(topic); err != nil {
			h.rejectedConnections.Add(1)
			return err
		}
	}

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

	req := registerRequest{
		client: c,
		result: make(chan error, 1),
	}
	select {
	case h.register <- req:
	case <-h.done:
		h.rejectedConnections.Add(1)
		return ErrHubClosed
	}
	select {
	case err := <-req.result:
		if err != nil {
			h.rejectedConnections.Add(1)
			return err
		}
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
		if sent, closed := trySendEvent(c.send, event); sent {
			h.replayedEvents.Add(1)
		} else if closed {
			return
		}
	}
}

// replaySticky sends retained events to a newly connected client.
func (h *Hub) replaySticky(c *Client) {
	h.stickyMu.RLock()
	defer h.stickyMu.RUnlock()

	for topic, events := range h.stickyEvents {
		if len(c.topics) > 0 && !c.topics[topic] {
			continue
		}
		for _, e := range events {
			if sent, closed := trySendEvent(c.send, e); sent {
				h.replayedEvents.Add(1)
			} else if closed {
				return
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
	if err := h.authorizePublish(PublishContext{Target: PublishTargetBroadcast}, e); err != nil {
		h.rejectedEvents.Add(1)
		h.logPublishDenied(PublishTargetBroadcast, "", err)
		return
	}
	h.persistSticky(e)
	h.persistReplay(e)
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for _, c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()
	for _, c := range clients {
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
	if err := h.authorizePublish(PublishContext{
		Target:   PublishTargetClient,
		ClientID: clientID,
	}, e); err != nil {
		h.rejectedEvents.Add(1)
		h.logPublishDenied(PublishTargetClient, clientID, err)
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
	if err := h.validateGroup(group); err != nil {
		h.rejectedEvents.Add(1)
		return
	}
	if err := h.validateEvent(e); err != nil {
		h.rejectedEvents.Add(1)
		return
	}
	if err := h.authorizePublish(PublishContext{
		Target: PublishTargetGroup,
		Group:  group,
	}, e); err != nil {
		h.rejectedEvents.Add(1)
		h.logPublishDenied(PublishTargetGroup, group, err)
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
	if err := h.authorizePublish(PublishContext{
		Target:     PublishTargetBroadcastExcept,
		ExcludeIDs: append([]string(nil), excludeIDs...),
	}, e); err != nil {
		h.rejectedEvents.Add(1)
		h.logPublishDenied(PublishTargetBroadcastExcept, "", err)
		return
	}
	excluded := make(map[string]bool, len(excludeIDs))
	for _, id := range excludeIDs {
		excluded[id] = true
	}
	h.persistSticky(e)
	h.persistReplay(e)
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for id, c := range h.clients {
		if !excluded[id] {
			clients = append(clients, c)
		}
	}
	h.mu.RUnlock()
	for _, c := range clients {
		h.deliver(c, e)
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
	if sent, closed := trySendEvent(c.send, e); sent {
		c.droppedConsecutive.Store(0)
		h.deliveredEvents.Add(1)
		return
	} else if closed {
		return
	}

	disconnectForDrops := h.recordClientDrop(c)

	switch h.opts.BackpressurePolicy {
	case BackpressureDropOldest:
		select {
		case <-c.send:
		default:
		}
		if sent, closed := trySendEvent(c.send, e); sent {
			c.droppedConsecutive.Store(0)
			h.deliveredEvents.Add(1)
		} else if closed {
			return
		} else {
			if disconnectForDrops {
				h.disconnectSlowConsumer(c, "drop_oldest_threshold")
			}
		}
	case BackpressureDisconnectSlow:
		h.disconnectSlowConsumer(c, "disconnect_policy")
	default:
		if disconnectForDrops {
			h.disconnectSlowConsumer(c, "drop_newest_threshold")
		}
	}
}

func trySendEvent(ch chan *Event, e *Event) (sent bool, closed bool) {
	defer func() {
		if recover() != nil {
			sent = false
			closed = true
		}
	}()
	select {
	case ch <- e:
		return true, false
	default:
		return false, false
	}
}

// JoinGroup adds a client to a named group, creating it if needed.
func (h *Hub) JoinGroup(clientID, group string) error {
	if err := h.validateGroup(group); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	c, ok := h.clients[clientID]
	if !ok {
		return ErrClientNotFound
	}

	if h.opts.MaxGroupsPerClient > 0 {
		c.groupsMu.RLock()
		groupCount := len(c.groups)
		alreadyMember := c.groups[group]
		c.groupsMu.RUnlock()
		if groupCount >= h.opts.MaxGroupsPerClient && !alreadyMember {
			return ErrClientGroupLimitExceeded
		}
	}

	if _, ok := h.groups[group]; !ok {
		h.groups[group] = make(map[string]bool)
	}
	h.groups[group][clientID] = true

	c.groupsMu.Lock()
	c.groups[group] = true
	c.groupsMu.Unlock()
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
	if e.Topic != "" {
		if err := h.validateTopic(e.Topic); err != nil {
			return err
		}
	}
	return nil
}

// ClearSticky removes all sticky events for a topic.
func (h *Hub) ClearSticky(topic string) {
	h.stickyMu.Lock()
	defer h.stickyMu.Unlock()
	delete(h.stickyEvents, topic)
}

// Stats contains a snapshot of hub metrics at a point in time.
// Use Stats for monitoring connection health and event delivery performance.
type Stats struct {
	// ConnectedClients is the current number of active SSE connections.
	ConnectedClients int

	// Groups maps group names to their member counts.
	Groups map[string]int

	// Draining indicates whether the hub is in drain mode (rejecting new connections).
	Draining bool

	// Closed indicates whether the hub has been stopped.
	Closed bool

	// DeliveredEvents is the total count of events successfully delivered to clients.
	DeliveredEvents uint64

	// DroppedEvents is the count of events dropped due to full client buffers.
	DroppedEvents uint64

	// SlowConsumerDrops is the count of clients disconnected for slow consumption.
	// Only incremented when BackpressurePolicy is BackpressureDisconnectSlow.
	SlowConsumerDrops uint64

	// RejectedConnections is the count of connection attempts rejected for any reason.
	RejectedConnections uint64

	// ReplayedEvents is the count of events replayed to reconnecting clients.
	ReplayedEvents uint64

	// ReplayMisses is the count of times replay was requested but no events were found.
	ReplayMisses uint64

	// RejectedEvents is the count of events rejected due to validation failures.
	RejectedEvents uint64
}

// Stats returns a snapshot of current hub metrics including connection counts,
// group memberships, and event delivery statistics. This method is safe to call
// concurrently and provides a consistent point-in-time view of hub state.
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

func (h *Hub) validateTopic(topic string) error {
	if h.opts.IdentifierValidator != nil {
		if err := h.opts.IdentifierValidator("topic", topic); err != nil {
			return errors.Join(ErrInvalidTopic, err)
		}
		return nil
	}
	if !validateIdentifier(topic, h.opts.MaxTopicLength) {
		return ErrInvalidTopic
	}
	return nil
}

func (h *Hub) validateGroup(group string) error {
	if h.opts.IdentifierValidator != nil {
		if err := h.opts.IdentifierValidator("group", group); err != nil {
			return errors.Join(ErrInvalidGroup, err)
		}
		return nil
	}
	if !validateIdentifier(group, h.opts.MaxGroupLength) {
		return ErrInvalidGroup
	}
	return nil
}

func (h *Hub) authorizePublish(ctx PublishContext, event *Event) error {
	if h.opts.AuthorizePublish == nil {
		return nil
	}
	return wrapPublishAuthorizationError(h.opts.AuthorizePublish(ctx, event))
}

func (h *Hub) emitLifecycle(event HubLifecycleEvent) {
	if h.opts.OnLifecycle != nil {
		h.opts.OnLifecycle(event)
	}
}

func (h *Hub) logPublishDenied(target PublishTarget, identifier string, err error) {
	if h.opts.Logger == nil {
		return
	}
	h.opts.Logger.Warn("sse publish denied", "target", target, "identifier", identifier, "error", err.Error())
}

func (h *Hub) recordClientDrop(c *Client) bool {
	drops := c.droppedConsecutive.Add(1)
	h.droppedEvents.Add(1)
	return h.opts.MaxDropsPerClient > 0 && int(drops) >= h.opts.MaxDropsPerClient
}

func (h *Hub) disconnectSlowConsumer(c *Client, reason string) {
	h.slowConsumerDrops.Add(1)
	h.RemoveClient(c)
	if h.opts.Logger != nil && h.slowLogLimiter.Allow() {
		h.opts.Logger.Warn("sse slow consumer disconnected", "client_id", c.ID, "reason", reason)
	}
}
