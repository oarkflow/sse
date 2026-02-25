package sse

import "sync"

// BackpressurePolicy controls behavior when a client's send buffer is full.
type BackpressurePolicy string

const (
	// BackpressureDropNewest drops the incoming event.
	BackpressureDropNewest BackpressurePolicy = "drop_newest"
	// BackpressureDropOldest evicts one queued event to make room for new one.
	BackpressureDropOldest BackpressurePolicy = "drop_oldest"
	// BackpressureDisconnectSlow disconnects slow consumers when buffers fill.
	BackpressureDisconnectSlow BackpressurePolicy = "disconnect_slow_consumer"
)

// ReplayStore persists events for replay by Last-Event-ID.
type ReplayStore interface {
	Append(event *Event)
	ReplayAfter(lastEventID string, topics map[string]bool, limit int) []*Event
}

// InMemoryReplayStore is a bounded in-process replay store.
type InMemoryReplayStore struct {
	mu     sync.RWMutex
	max    int
	events []*Event
}

// NewInMemoryReplayStore creates a replay store retaining up to max events.
func NewInMemoryReplayStore(max int) *InMemoryReplayStore {
	if max <= 0 {
		max = 1000
	}
	return &InMemoryReplayStore{
		max: max,
	}
}

// Append stores an event in a capped ring-like slice.
func (s *InMemoryReplayStore) Append(event *Event) {
	if event == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	if len(s.events) > s.max {
		s.events = s.events[len(s.events)-s.max:]
	}
}

// ReplayAfter returns events after lastEventID, optionally filtered by topics.
func (s *InMemoryReplayStore) ReplayAfter(lastEventID string, topics map[string]bool, limit int) []*Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.events) == 0 {
		return nil
	}

	startIdx := 0
	if lastEventID != "" {
		startIdx = len(s.events)
		for i := len(s.events) - 1; i >= 0; i-- {
			if s.events[i].ID == lastEventID {
				startIdx = i + 1
				break
			}
		}
	}

	if startIdx >= len(s.events) {
		return nil
	}

	if limit <= 0 {
		limit = 500
	}

	out := make([]*Event, 0, limit)
	for _, event := range s.events[startIdx:] {
		if len(topics) > 0 {
			if event.Topic == "" || !topics[event.Topic] {
				continue
			}
		}
		out = append(out, event)
		if len(out) >= limit {
			break
		}
	}
	return out
}
