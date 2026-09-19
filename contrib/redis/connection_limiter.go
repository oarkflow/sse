package sseredis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var redisConnLimiterAcquireScript = redis.NewScript(`
local v = redis.call("INCR", KEYS[1])
if v == 1 then
  redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
if v > tonumber(ARGV[1]) then
  redis.call("DECR", KEYS[1])
  return 0
end
return 1
`)

var redisConnLimiterReleaseScript = redis.NewScript(`
local v = redis.call("DECR", KEYS[1])
if v <= 0 then
  redis.call("DEL", KEYS[1])
end
return v
`)

// RedisConnectionLimiter implements sse.ConnectionLimiter, enforcing a
// concurrent-connection cap per key (e.g. per client IP) across every
// instance sharing the same Redis. Wire it into
// sse.HandlerOptions.ConnectionLimiter for distributed per-IP connection limits.
type RedisConnectionLimiter struct {
	client *redis.Client
	prefix string
	max    int
	ttl    time.Duration
}

// RedisConnectionLimiterOptions configures RedisConnectionLimiter.
type RedisConnectionLimiterOptions struct {
	// Prefix is the Redis key prefix for counter keys.
	// Default: "sse:conn:"
	Prefix string

	// Max is the maximum concurrent connections allowed per key. Max <= 0
	// disables limiting (Acquire always succeeds).
	Max int

	// TTL bounds how long a counter key survives without a matching
	// Release, guarding against leaked slots from a crashed instance.
	// Default: 1 minute.
	TTL time.Duration
}

// NewRedisConnectionLimiter creates a distributed connection limiter.
func NewRedisConnectionLimiter(client *redis.Client, opts RedisConnectionLimiterOptions) *RedisConnectionLimiter {
	if opts.Prefix == "" {
		opts.Prefix = "sse:conn:"
	}
	if opts.TTL <= 0 {
		opts.TTL = time.Minute
	}
	return &RedisConnectionLimiter{
		client: client,
		prefix: opts.Prefix,
		max:    opts.Max,
		ttl:    opts.TTL,
	}
}

// Acquire reserves one connection slot for key (for example a client IP).
func (l *RedisConnectionLimiter) Acquire(key string) bool {
	if l.max <= 0 {
		return true
	}
	ctx := context.Background()
	ok, err := redisConnLimiterAcquireScript.Run(ctx, l.client, []string{l.prefix + key}, l.max, l.ttl.Milliseconds()).Int()
	return err == nil && ok == 1
}

// Release frees one connection slot for key.
func (l *RedisConnectionLimiter) Release(key string) {
	if l.max <= 0 {
		return
	}
	ctx := context.Background()
	_, _ = redisConnLimiterReleaseScript.Run(ctx, l.client, []string{l.prefix + key}).Int()
}

func (l *RedisConnectionLimiter) String() string {
	return fmt.Sprintf("RedisConnectionLimiter(prefix=%s,max=%d,ttl=%s)", l.prefix, l.max, l.ttl.String())
}
