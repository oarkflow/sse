package sse

import (
	"context"
	"sync"
	"testing"
	"time"
)

// pairBus is a minimal in-process Bus test double connecting exactly two
// dispatchers, used to verify Hub/UserHub Bus wiring without Redis.
type pairBus struct {
	mu    sync.Mutex
	peers []Dispatcher
}

func (b *pairBus) Publish(_ context.Context, ctx PublishContext, e *Event) error {
	b.mu.Lock()
	peers := append([]Dispatcher(nil), b.peers...)
	b.mu.Unlock()
	for _, p := range peers {
		p.DispatchRemote(ctx, e)
	}
	return nil
}

func (b *pairBus) Subscribe(ctx context.Context, dispatcher Dispatcher) error {
	b.mu.Lock()
	b.peers = append(b.peers, dispatcher)
	b.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

func TestHubBusBroadcastFanout(t *testing.T) {
	bus := &pairBus{}
	hubA := NewHub(HubOptions{ClientBufferSize: 16, HeartbeatInterval: 0, Bus: bus})
	hubB := NewHub(HubOptions{ClientBufferSize: 16, HeartbeatInterval: 0, Bus: bus})
	defer hubA.Stop()
	defer hubB.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go bus.Subscribe(ctx, hubA)
	go bus.Subscribe(ctx, hubB)
	time.Sleep(10 * time.Millisecond)

	cb := newTestClient("remote-1")
	if err := hubB.AddClient(cb); err != nil {
		t.Fatalf("AddClient: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	hubA.Broadcast(NewEvent("hello", "from A"))

	e := recv(t, cb, time.Second)
	if e.Data != "from A" {
		t.Fatalf("expected event fanned out via bus, got %q", e.Data)
	}
}

func TestHubBusSendToClient(t *testing.T) {
	bus := &pairBus{}
	hubA := NewHub(HubOptions{ClientBufferSize: 16, HeartbeatInterval: 0, Bus: bus})
	hubB := NewHub(HubOptions{ClientBufferSize: 16, HeartbeatInterval: 0, Bus: bus})
	defer hubA.Stop()
	defer hubB.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go bus.Subscribe(ctx, hubA)
	go bus.Subscribe(ctx, hubB)
	time.Sleep(10 * time.Millisecond)

	cb := newTestClient("remote-2")
	if err := hubB.AddClient(cb); err != nil {
		t.Fatalf("AddClient: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	// Client lives only on hubB; hubA.Send reports ErrClientNotFound locally
	// (the client isn't on this instance) but still fans the event out over
	// the bus so hubB can deliver it.
	if err := hubA.Send("remote-2", NewEvent("direct", "hi")); err != ErrClientNotFound {
		t.Fatalf("expected ErrClientNotFound (local miss), got %v", err)
	}

	e := recv(t, cb, time.Second)
	if e.Data != "hi" {
		t.Fatalf("expected direct event via bus, got %q", e.Data)
	}
}

func TestUserHubBusSendToUser(t *testing.T) {
	bus := &pairBus{}
	uA := NewUserHub(HubOptions{ClientBufferSize: 16, HeartbeatInterval: 0, Bus: bus})
	uB := NewUserHub(HubOptions{ClientBufferSize: 16, HeartbeatInterval: 0, Bus: bus})
	defer uA.Stop()
	defer uB.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go bus.Subscribe(ctx, uA)
	go bus.Subscribe(ctx, uB)
	time.Sleep(10 * time.Millisecond)

	cb := NewClient(ClientOptions{ID: "device-2", UserID: "user-1", BufferSize: 16})
	if err := uB.AddClient(cb); err != nil {
		t.Fatalf("AddClient: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	// user-1's only connection is on instance B; SendToUser on A must reach it via the bus.
	uA.SendToUser("user-1", NewEvent("ping", "cross-instance"))

	e := recv(t, cb, time.Second)
	if e.Data != "cross-instance" {
		t.Fatalf("expected user-targeted event via bus, got %q", e.Data)
	}
}
