package sse

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisBus provides cross-instance publish fanout using Redis Pub/Sub.
type RedisBus struct {
	client  *redis.Client
	channel string
	logger  *slog.Logger
}

// RedisBusOptions configures RedisBus.
type RedisBusOptions struct {
	Channel string
	Logger  *slog.Logger
}

type redisBusMessage struct {
	Target     PublishTarget `json:"target"`
	ClientID   string        `json:"client_id,omitempty"`
	Group      string        `json:"group,omitempty"`
	ExcludeIDs []string      `json:"exclude_ids,omitempty"`
	Event      redisEvent    `json:"event"`
}

// NewRedisBus creates a Redis Pub/Sub adapter for cross-node broadcasts.
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

// Publish sends a publish operation to the shared Redis channel.
func (b *RedisBus) Publish(ctx context.Context, publishCtx PublishContext, event *Event) error {
	msg := redisBusMessage{
		Target:     publishCtx.Target,
		ClientID:   publishCtx.ClientID,
		Group:      publishCtx.Group,
		ExcludeIDs: append([]string(nil), publishCtx.ExcludeIDs...),
		Event: redisEvent{
			ID:        event.ID,
			Type:      event.Type,
			Data:      event.Data,
			Topic:     event.Topic,
			CreatedAt: event.CreatedAt.UnixNano(),
		},
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return b.client.Publish(ctx, b.channel, data).Err()
}

// Subscribe starts consuming bus messages and dispatches into the local hub.
// This method blocks until ctx cancellation or a subscription error.
func (b *RedisBus) Subscribe(ctx context.Context, hub *Hub) error {
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
			event := &Event{
				ID:        m.Event.ID,
				Type:      m.Event.Type,
				Data:      m.Event.Data,
				Topic:     m.Event.Topic,
				CreatedAt: unixNanoToTime(m.Event.CreatedAt),
			}
			switch m.Target {
			case PublishTargetBroadcast:
				hub.Broadcast(event)
			case PublishTargetClient:
				_ = hub.Send(m.ClientID, event)
			case PublishTargetGroup:
				hub.SendToGroup(m.Group, event)
			case PublishTargetBroadcastExcept:
				hub.BroadcastExcept(event, m.ExcludeIDs...)
			default:
				b.logger.Warn("sse redis bus: unknown target", "target", m.Target)
			}
		}
	}
}

func unixNanoToTime(v int64) (t time.Time) {
	if v <= 0 {
		return time.Now()
	}
	return time.Unix(0, v)
}
