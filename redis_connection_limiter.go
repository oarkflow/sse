package sse

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisConnectionLimiter enforces concurrent-connection limits across instances.
type RedisConnectionLimiter struct {
	client *redis.Client
	prefix string
	max    int
	ttl    time.Duration
}

// RedisConnectionLimiterOptions configures RedisConnectionLimiter.
type RedisConnectionLimiterOptions struct {
	Prefix string
	Max    int
	TTL    time.Duration
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

// Acquire reserves one connection slot for key (for example an IP).
func (l *RedisConnectionLimiter) Acquire(key string) bool {
	if l.max <= 0 {
		return true
	}
	ctx := context.Background()
	redisKey := l.prefix + key
	script := redis.NewScript(`
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
	ok, err := script.Run(ctx, l.client, []string{redisKey}, l.max, l.ttl.Milliseconds()).Int()
	return err == nil && ok == 1
}

// Release frees one connection slot for key.
func (l *RedisConnectionLimiter) Release(key string) {
	if l.max <= 0 {
		return
	}
	ctx := context.Background()
	redisKey := l.prefix + key
	script := redis.NewScript(`
local v = redis.call("DECR", KEYS[1])
if v <= 0 then
  redis.call("DEL", KEYS[1])
end
return v
`)
	_, _ = script.Run(ctx, l.client, []string{redisKey}).Int()
}

func (l *RedisConnectionLimiter) String() string {
	return fmt.Sprintf("RedisConnectionLimiter(prefix=%s,max=%d,ttl=%s)", l.prefix, l.max, l.ttl.String())
}
