// Package sseredis provides Redis-backed adapters for the sse package's
// pluggable interfaces (Bus, ReplayStore, PresenceStore, ConnectionLimiter),
// enabling multi-instance / horizontally scaled deployments. It is a
// separate Go module so the core sse module stays dependency-free (other
// than the fh HTTP framework); pull it in only if you need Redis.
package sseredis

import (
	"context"
	"encoding/json"
	"time"

	"github.com/oarkflow/sse"
	"github.com/redis/go-redis/v9"
)

// redisEvent is the JSON wire structure used by RedisReplayStore and RedisBus.
type redisEvent struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Data      string `json:"data"`
	Topic     string `json:"topic,omitempty"`
	CreatedAt int64  `json:"created_at"`
}

func encodeRedisEvent(e *sse.Event) redisEvent {
	return redisEvent{
		ID:        e.ID,
		Type:      e.Type,
		Data:      e.Data,
		Topic:     e.Topic,
		CreatedAt: e.CreatedAt.UnixNano(),
	}
}

func (re redisEvent) toEvent() *sse.Event {
	return &sse.Event{
		ID:        re.ID,
		Type:      re.Type,
		Data:      re.Data,
		Topic:     re.Topic,
		CreatedAt: unixNanoToTime(re.CreatedAt),
	}
}

func unixNanoToTime(v int64) time.Time {
	if v <= 0 {
		return time.Now()
	}
	return time.Unix(0, v)
}

// RedisReplayStore implements sse.ReplayStore using Redis for persistence.
// This enables event replay across multiple server instances and survives
// individual server restarts. Events are stored in a Redis List with a
// configurable maximum capacity and TTL.
//
// Example usage:
//
//	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	store := sse.NewRedisReplayStore(client, sse.RedisReplayStoreOptions{
//	    Prefix:    "sse:replay:",
//	    MaxEvents: 10000,
//	    TTL:       24 * time.Hour,
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

// RedisReplayStoreOptions configures the Redis replay store.
type RedisReplayStoreOptions struct {
	// Prefix is the Redis key prefix for all stored events.
	// Default: "sse:replay:"
	Prefix string

	// MaxEvents is the maximum number of events to retain.
	// Default: 10000
	MaxEvents int

	// TTL is the time-to-live refreshed on every write.
	// Default: 24 hours
	TTL time.Duration
}

// NewRedisReplayStore creates a Redis-backed replay store. The store uses a
// single Redis list to maintain event order and supports distributed
// deployments where multiple server instances need to share event history
// for Last-Event-ID reconnection replay.
func NewRedisReplayStore(client *redis.Client, opts RedisReplayStoreOptions) *RedisReplayStore {
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

func (s *RedisReplayStore) key() string {
	return s.prefix + "events"
}

// Append stores an event in Redis, trimming the list to the configured
// capacity and refreshing its TTL.
func (s *RedisReplayStore) Append(event *sse.Event) {
	if event == nil || event.ID == "" {
		return
	}

	ctx := context.Background()
	data, err := json.Marshal(encodeRedisEvent(event))
	if err != nil {
		return
	}

	key := s.key()
	pipe := s.client.Pipeline()
	pipe.RPush(ctx, key, data)
	pipe.LTrim(ctx, key, int64(-s.max), -1)
	pipe.Expire(ctx, key, s.ttl)
	_, _ = pipe.Exec(ctx)
}

// ReplayAfter retrieves events that occurred after the specified lastEventID.
// If lastEventID is empty, all retained events (subject to limit) are
// returned. Events can be filtered by topic.
func (s *RedisReplayStore) ReplayAfter(lastEventID string, topics map[string]bool, limit int) []*sse.Event {
	ctx := context.Background()

	if limit <= 0 {
		limit = 500
	}

	results, err := s.client.LRange(ctx, s.key(), 0, -1).Result()
	if err != nil {
		return nil
	}

	var events []*sse.Event
	found := lastEventID == ""

	for _, data := range results {
		var re redisEvent
		if err := json.Unmarshal([]byte(data), &re); err != nil {
			continue
		}

		if !found {
			if re.ID == lastEventID {
				found = true
			}
			continue
		}

		if len(topics) > 0 && (re.Topic == "" || !topics[re.Topic]) {
			continue
		}

		events = append(events, re.toEvent())
		if len(events) >= limit {
			break
		}
	}

	return events
}

// Clear removes all stored events from Redis.
func (s *RedisReplayStore) Clear() error {
	return s.client.Del(context.Background(), s.key()).Err()
}
