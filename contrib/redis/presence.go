package sseredis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisPresenceStore implements sse.PresenceStore, providing cross-instance
// user presence tracking backed by a Redis set per user plus a set of all
// online users. Wire it into sse.NewUserHubWithPresence so
// IsOnline/ConnectionCount/OnlineUsers reflect connections held by any
// instance, not just the local one.
type RedisPresenceStore struct {
	client *redis.Client
	prefix string
	ttl    time.Duration
}

// RedisPresenceStoreOptions configures RedisPresenceStore.
type RedisPresenceStoreOptions struct {
	// Prefix is the Redis key prefix for presence keys.
	// Default: "sse:presence:"
	Prefix string

	// TTL bounds how long a user's presence set survives without an Add,
	// guarding against orphaned entries from an instance that crashed
	// without calling Remove. Default: 10 minutes.
	TTL time.Duration
}

// NewRedisPresenceStore creates a Redis-backed presence store.
func NewRedisPresenceStore(client *redis.Client, opts RedisPresenceStoreOptions) *RedisPresenceStore {
	if opts.Prefix == "" {
		opts.Prefix = "sse:presence:"
	}
	if opts.TTL <= 0 {
		opts.TTL = 10 * time.Minute
	}
	return &RedisPresenceStore{
		client: client,
		prefix: opts.Prefix,
		ttl:    opts.TTL,
	}
}

func (s *RedisPresenceStore) userKey(userID string) string {
	return s.prefix + "user:" + userID
}

func (s *RedisPresenceStore) usersKey() string {
	return s.prefix + "users"
}

// Add marks userID as online with an active clientID.
func (s *RedisPresenceStore) Add(userID, clientID string) error {
	ctx := context.Background()
	userKey := s.userKey(userID)
	pipe := s.client.Pipeline()
	pipe.SAdd(ctx, userKey, clientID)
	pipe.Expire(ctx, userKey, s.ttl)
	pipe.SAdd(ctx, s.usersKey(), userID)
	_, err := pipe.Exec(ctx)
	return err
}

// Remove removes one active client for userID.
func (s *RedisPresenceStore) Remove(userID, clientID string) error {
	ctx := context.Background()
	userKey := s.userKey(userID)
	n, err := s.client.SRem(ctx, userKey, clientID).Result()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	remaining, err := s.client.SCard(ctx, userKey).Result()
	if err != nil {
		return err
	}
	if remaining == 0 {
		pipe := s.client.Pipeline()
		pipe.Del(ctx, userKey)
		pipe.SRem(ctx, s.usersKey(), userID)
		_, err = pipe.Exec(ctx)
	}
	return err
}

// IsOnline reports whether the user has at least one active connection on
// any instance.
func (s *RedisPresenceStore) IsOnline(userID string) (bool, error) {
	n, err := s.client.SCard(context.Background(), s.userKey(userID)).Result()
	return n > 0, err
}

// ConnectionCount returns the active connection count for userID across all instances.
func (s *RedisPresenceStore) ConnectionCount(userID string) (int, error) {
	n, err := s.client.SCard(context.Background(), s.userKey(userID)).Result()
	return int(n), err
}

// OnlineUsers returns all users currently marked online on any instance.
func (s *RedisPresenceStore) OnlineUsers() ([]string, error) {
	return s.client.SMembers(context.Background(), s.usersKey()).Result()
}
