// Package ssenats provides a NATS JetStream-backed sse.Bus for durable,
// at-least-once cross-instance fanout. It is a separate Go module so the
// core sse module stays dependency-free (other than the fh HTTP framework);
// pull it in only if you use NATS.
//
// Unlike contrib/redis's RedisBus (plain Redis Pub/Sub, at-most-once and
// lost if a subscriber is briefly disconnected), NatsBus persists published
// events in a JetStream stream and each instance consumes them through its
// own durable consumer with explicit acknowledgement. A subscriber that
// reconnects after a brief network blip picks up any events it missed
// instead of silently dropping them.
package ssenats

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/oarkflow/sse"
)

// NatsBus fans sse events out via a JetStream stream. Every instance gets
// its own durable consumer (keyed by InstanceID), so every instance sees
// every published event exactly like RedisBus's Pub/Sub broadcast — but
// backed by JetStream's persistence and ack tracking instead of a fire-and-
// forget channel.
type NatsBus struct {
	js           jetstream.JetStream
	stream       string
	subject      string
	consumerName string
	ackWait      time.Duration
	logger       *slog.Logger
}

// NatsBusOptions configures NatsBus.
type NatsBusOptions struct {
	// Stream is the JetStream stream name shared by all instances.
	// Default: "SSE_BUS"
	Stream string

	// Subject is the subject events are published to and consumed from.
	// Default: "sse.bus"
	Subject string

	// InstanceID uniquely identifies this instance's durable consumer.
	// Every instance must use a distinct, stable value so JetStream keeps
	// a separate delivery cursor per instance (all instances receive every
	// event, rather than competing for messages like a queue group would).
	// Default: a random ID (a fresh, non-durable-across-restarts consumer;
	// set this explicitly in production so a restarted instance resumes
	// from where it left off instead of only seeing new events).
	InstanceID string

	// MaxAge bounds how long the stream retains events. Default: 24h.
	MaxAge time.Duration

	// AckWait bounds how long JetStream waits for an ack before redelivering
	// a message to this consumer. Default: 30s.
	AckWait time.Duration

	Logger *slog.Logger
}

type natsBusMessage struct {
	Target     sse.PublishTarget `json:"target"`
	ClientID   string            `json:"client_id,omitempty"`
	Group      string            `json:"group,omitempty"`
	UserID     string            `json:"user_id,omitempty"`
	ExcludeIDs []string          `json:"exclude_ids,omitempty"`
	Event      natsEvent         `json:"event"`
}

type natsEvent struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Data      string `json:"data"`
	Topic     string `json:"topic,omitempty"`
	CreatedAt int64  `json:"created_at"`
}

func encodeEvent(e *sse.Event) natsEvent {
	return natsEvent{
		ID:        e.ID,
		Type:      e.Type,
		Data:      e.Data,
		Topic:     e.Topic,
		CreatedAt: e.CreatedAt.UnixNano(),
	}
}

func (ne natsEvent) toEvent() *sse.Event {
	createdAt := time.Now()
	if ne.CreatedAt > 0 {
		createdAt = time.Unix(0, ne.CreatedAt)
	}
	return &sse.Event{
		ID:        ne.ID,
		Type:      ne.Type,
		Data:      ne.Data,
		Topic:     ne.Topic,
		CreatedAt: createdAt,
	}
}

// NewNatsBus creates a JetStream-backed Bus. It creates or updates the
// underlying stream so a fresh deployment works out of the box; call it
// once per nats.Conn (instances typically share one connection).
func NewNatsBus(ctx context.Context, nc *nats.Conn, opts NatsBusOptions) (*NatsBus, error) {
	if opts.Stream == "" {
		opts.Stream = "SSE_BUS"
	}
	if opts.Subject == "" {
		opts.Subject = "sse.bus"
	}
	if opts.InstanceID == "" {
		opts.InstanceID = "sse-" + newRandomID()
	}
	if opts.MaxAge <= 0 {
		opts.MaxAge = 24 * time.Hour
	}
	if opts.AckWait <= 0 {
		opts.AckWait = 30 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("ssenats: create jetstream context: %w", err)
	}

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      opts.Stream,
		Subjects:  []string{opts.Subject},
		Storage:   jetstream.FileStorage,
		MaxAge:    opts.MaxAge,
		Retention: jetstream.LimitsPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("ssenats: create or update stream %q: %w", opts.Stream, err)
	}

	return &NatsBus{
		js:           js,
		stream:       opts.Stream,
		subject:      opts.Subject,
		consumerName: opts.InstanceID,
		ackWait:      opts.AckWait,
		logger:       opts.Logger,
	}, nil
}

// Publish sends a publish operation onto the JetStream stream so every
// other subscribed instance's durable consumer receives and acks it.
func (b *NatsBus) Publish(ctx context.Context, publishCtx sse.PublishContext, event *sse.Event) error {
	msg := natsBusMessage{
		Target:     publishCtx.Target,
		ClientID:   publishCtx.ClientID,
		Group:      publishCtx.Group,
		UserID:     publishCtx.UserID,
		ExcludeIDs: append([]string(nil), publishCtx.ExcludeIDs...),
		Event:      encodeEvent(event),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = b.js.Publish(ctx, b.subject, data)
	return err
}

// Subscribe creates (or resumes) this instance's durable consumer and hands
// every event it receives to dispatcher.DispatchRemote, acking only after
// dispatch succeeds. It blocks until ctx is cancelled or the consumer
// errors out unrecoverably.
func (b *NatsBus) Subscribe(ctx context.Context, dispatcher sse.Dispatcher) error {
	consumer, err := b.js.CreateOrUpdateConsumer(ctx, b.stream, jetstream.ConsumerConfig{
		Durable:       b.consumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		FilterSubject: b.subject,
		AckWait:       b.ackWait,
	})
	if err != nil {
		return fmt.Errorf("ssenats: create or update consumer %q: %w", b.consumerName, err)
	}

	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		var m natsBusMessage
		if err := json.Unmarshal(msg.Data(), &m); err != nil {
			b.logger.Warn("ssenats: invalid payload, dropping", "error", err.Error())
			_ = msg.Term() // malformed payload will never succeed; don't redeliver forever
			return
		}
		dispatcher.DispatchRemote(sse.PublishContext{
			Target:     m.Target,
			ClientID:   m.ClientID,
			Group:      m.Group,
			UserID:     m.UserID,
			ExcludeIDs: m.ExcludeIDs,
		}, m.Event.toEvent())
		_ = msg.Ack()
	})
	if err != nil {
		return fmt.Errorf("ssenats: start consuming: %w", err)
	}
	defer consumeCtx.Stop()

	<-ctx.Done()
	return ctx.Err()
}
