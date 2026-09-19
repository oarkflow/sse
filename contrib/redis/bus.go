package sseredis

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/oarkflow/sse"
	"github.com/redis/go-redis/v9"
)

// RedisBus provides cross-instance publish fanout using Redis Pub/Sub. It is
// the reference distributed Bus implementation: wire it into HubOptions.Bus
// to make Broadcast/Send/SendToGroup/BroadcastExcept (and UserHub.SendToUser)
// reach clients connected to other instances.
//
// Example usage:
//
//	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	bus := sseredis.NewRedisBus(client, sseredis.RedisBusOptions{})
//	hub := sse.NewHub(sse.HubOptions{Bus: bus})
//	go bus.Subscribe(context.Background(), hub) // or userHub
type RedisBus struct {
	client  *redis.Client
	channel string
	logger  *slog.Logger
}

// RedisBusOptions configures RedisBus.
type RedisBusOptions struct {
	// Channel is the Redis Pub/Sub channel shared by all instances.
	// Default: "sse:bus"
	Channel string

	Logger *slog.Logger
}

type redisBusMessage struct {
	Target     sse.PublishTarget `json:"target"`
	ClientID   string            `json:"client_id,omitempty"`
	Group      string            `json:"group,omitempty"`
	UserID     string            `json:"user_id,omitempty"`
	ExcludeIDs []string          `json:"exclude_ids,omitempty"`
	Event      redisEvent        `json:"event"`
}

// NewRedisBus creates a Redis Pub/Sub adapter for cross-instance broadcasts.
func NewRedisBus(client *redis.Client, opts RedisBusOptions) *RedisBus {
	if opts.Channel == "" {
		opts.Channel = "sse:bus"
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &RedisBus{
		client:  client,
		channel: opts.Channel,
		logger:  opts.Logger,
	}
}

// Publish sends a publish operation to the shared Redis channel so every
// other subscribed instance can dispatch it to its own local clients.
func (b *RedisBus) Publish(ctx context.Context, publishCtx sse.PublishContext, event *sse.Event) error {
	msg := redisBusMessage{
		Target:     publishCtx.Target,
		ClientID:   publishCtx.ClientID,
		Group:      publishCtx.Group,
		UserID:     publishCtx.UserID,
		ExcludeIDs: append([]string(nil), publishCtx.ExcludeIDs...),
		Event:      encodeRedisEvent(event),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return b.client.Publish(ctx, b.channel, data).Err()
}

// Subscribe consumes bus messages published by other instances and hands
// them to dispatcher.DispatchRemote for local-only delivery. It blocks until
// ctx is cancelled or the subscription errors out.
func (b *RedisBus) Subscribe(ctx context.Context, dispatcher sse.Dispatcher) error {
	sub := b.client.Subscribe(ctx, b.channel)
	defer sub.Close()

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			var m redisBusMessage
			if err := json.Unmarshal([]byte(msg.Payload), &m); err != nil {
				b.logger.Warn("sse redis bus: invalid payload", "error", err.Error())
				continue
			}
			dispatcher.DispatchRemote(sse.PublishContext{
				Target:     m.Target,
				ClientID:   m.ClientID,
				Group:      m.Group,
				UserID:     m.UserID,
				ExcludeIDs: m.ExcludeIDs,
			}, m.Event.toEvent())
		}
	}
}
