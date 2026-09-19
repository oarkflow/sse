package sse

import "context"

// Bus provides cross-instance event broadcasting.
// A single instance's Hub handles local delivery directly via Broadcast,
// Send, SendToGroup, and BroadcastExcept; when a Bus is configured on
// HubOptions, those same calls also fan out over the Bus so every other
// instance subscribed to it delivers the event to its own local clients.
//
// Implementations must be safe for concurrent use. Publish should be fast
// and non-blocking relative to the Hub's delivery loop; Hub bounds calls to
// Publish with HubOptions.BusPublishTimeout.
type Bus interface {
	// Publish fans a locally-originated, already-authorized event out to
	// every other subscriber of the Bus.
	Publish(ctx context.Context, publishCtx PublishContext, event *Event) error

	// Subscribe consumes events published by other instances and hands
	// them to dispatcher.DispatchRemote for local-only delivery. It blocks
	// until ctx is cancelled or the underlying transport errors out, so
	// callers typically run it in its own goroutine:
	//
	//	go bus.Subscribe(ctx, hub)
	Subscribe(ctx context.Context, dispatcher Dispatcher) error
}

// InMemoryBus is a no-op bus for single-instance deployments. It is the
// default when HubOptions.Bus is left nil (Hub simply skips publishing).
// It exists so code that explicitly wants an "always local" Bus value can
// use one without a nil check.
type InMemoryBus struct{}

func NewInMemoryBus() *InMemoryBus {
	return &InMemoryBus{}
}

func (b *InMemoryBus) Publish(_ context.Context, _ PublishContext, _ *Event) error {
	return nil
}

func (b *InMemoryBus) Subscribe(ctx context.Context, _ Dispatcher) error {
	<-ctx.Done()
	return ctx.Err()
}
