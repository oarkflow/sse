package sse

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisReplayStore implements ReplayStore using Redis for persistence.
// This enables event replay across multiple server instances and survives
// server restarts. Events are stored in a Redis List with a configurable
// maximum capacity and TTL.
//
// Example usage:
//
//	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	store := sse.NewRedisReplayStore(client, sse.RedisStoreOptions{
//	    Prefix: "sse:events:",
//	    MaxEvents: 10000,
//	    TTL: 24 * time.Hour,
//	})
//	hub := sse.NewHub(sse.HubOptions{
//	    ReplayStore: store,
//	})
type RedisReplayStore struct {
	client *redis.Client
	prefix string
	max    int
	ttl    time.Duration
}

// RedisStoreOptions configures the Redis replay store.
type RedisStoreOptions struct {
	// Prefix is the Redis key prefix for all stored events.
	// Default: "sse:replay:"
	Prefix string

	// MaxEvents is the maximum number of events to retain.
	// Default: 10000
	MaxEvents int

	// TTL is the time-to-live for stored events.
	// Default: 24 hours
	TTL time.Duration
}

// NewRedisReplayStore creates a Redis-backed replay store.
// The store uses a single Redis list to maintain event order and supports
// distributed deployments where multiple server instances need to share
// event history.
func NewRedisReplayStore(client *redis.Client, opts RedisStoreOptions) *RedisReplayStore {
	if opts.Prefix == "" {
		opts.Prefix = "sse:replay:"
	}
	if opts.MaxEvents <= 0 {
		opts.MaxEvents = 10000
	}
	if opts.TTL <= 0 {
		opts.TTL = 24 * time.Hour
	}
	return &RedisReplayStore{
		client: client,
		prefix: opts.Prefix,
		max:    opts.MaxEvents,
		ttl:    opts.TTL,
	}
}

// redisEvent is the JSON structure stored in Redis.
type redisEvent struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Data      string `json:"data"`
	Topic     string `json:"topic,omitempty"`
	CreatedAt int64  `json:"created_at"`
}

// Append stores an event in Redis. Events are added to a list and the list
// is trimmed to maintain the maximum capacity. A TTL is set on each write.
func (s *RedisReplayStore) Append(event *Event) {
	if event == nil || event.ID == "" {
		return
	}

	ctx := context.Background()
	data, err := json.Marshal(redisEvent{
		ID:        event.ID,
		Type:      event.Type,
		Data:      event.Data,
		Topic:     event.Topic,
		CreatedAt: event.CreatedAt.UnixNano(),
	})
	if err != nil {
		return
	}

	key := s.prefix + "events"

	// Use pipeline for atomic operations
	pipe := s.client.Pipeline()
	pipe.RPush(ctx, key, data)
	pipe.LTrim(ctx, key, int64(-s.max), -1)
	pipe.Expire(ctx, key, s.ttl)
	_, _ = pipe.Exec(ctx)
}

// ReplayAfter retrieves events that occurred after the specified lastEventID.
// If lastEventID is empty, returns the most recent events up to the limit.
// Events can be filtered by topic.
func (s *RedisReplayStore) ReplayAfter(lastEventID string, topics map[string]bool, limit int) []*Event {
	ctx := context.Background()
	key := s.prefix + "events"

	if limit <= 0 {
		limit = 500
	}

	// Get all events from the list (up to max capacity)
	results, err := s.client.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		return nil
	}

	var events []*Event
	found := lastEventID == ""

	for _, data := range results {
		var re redisEvent
		if err := json.Unmarshal([]byte(data), &re); err != nil {
			continue
		}

		// Skip until we find the last event ID
		if !found {
			if re.ID == lastEventID {
				found = true
			}
			continue
		}

		// Filter by topic if specified
		if len(topics) > 0 && re.Topic != "" && !topics[re.Topic] {
			continue
		}

		events = append(events, &Event{
			ID:        re.ID,
			Type:      re.Type,
			Data:      re.Data,
			Topic:     re.Topic,
			CreatedAt: time.Unix(0, re.CreatedAt),
		})

		if len(events) >= limit {
			break
		}
	}

	return events
}

// Clear removes all stored events from Redis.
func (s *RedisReplayStore) Clear() error {
	ctx := context.Background()
	return s.client.Del(ctx, s.prefix+"events").Err()
}

// Close closes the Redis client connection.
func (s *RedisReplayStore) Close() error {
	return s.client.Close()
}
