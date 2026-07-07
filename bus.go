package sse

import "context"

// Bus provides cross-instance event broadcasting.
// In-memory hubs handle local delivery directly; a Bus implementation
// fans out publish operations to other instances (e.g. via Redis Pub/Sub).
type Bus interface {
	Publish(ctx context.Context, publishCtx PublishContext, event *Event) error
	Subscribe(ctx context.Context, hub *Hub) error
}

// InMemoryBus is a no-op bus for single-instance deployments.
type InMemoryBus struct{}

func NewInMemoryBus() *InMemoryBus {
	return &InMemoryBus{}
}

func (b *InMemoryBus) Publish(_ context.Context, _ PublishContext, _ *Event) error {
	return nil
}

func (b *InMemoryBus) Subscribe(ctx context.Context, _ *Hub) error {
	<-ctx.Done()
	return ctx.Err()
}
