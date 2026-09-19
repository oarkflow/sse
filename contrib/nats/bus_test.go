package ssenats

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/oarkflow/sse"
)

func startTestServer(t *testing.T) *server.Server {
	t.Helper()
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // random free port
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("server.NewServer: %v", err)
	}
	srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server did not become ready")
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

func newTestConn(t *testing.T, srv *server.Server) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func newTestClient(id string) *sse.Client {
	return sse.NewClient(sse.ClientOptions{ID: id, BufferSize: 16})
}

func recv(t *testing.T, c *sse.Client, timeout time.Duration) *sse.Event {
	t.Helper()
	select {
	case e := <-c.Recv():
		return e
	case <-time.After(timeout):
		t.Fatalf("client %s: timed out waiting for event", c.ID)
		return nil
	}
}

func TestNatsBusPublishSubscribeBroadcast(t *testing.T) {
	srv := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	busA, err := NewNatsBus(ctx, newTestConn(t, srv), NatsBusOptions{Subject: "test.bus", InstanceID: "instance-a"})
	if err != nil {
		t.Fatalf("NewNatsBus A: %v", err)
	}
	busB, err := NewNatsBus(ctx, newTestConn(t, srv), NatsBusOptions{Subject: "test.bus", InstanceID: "instance-b"})
	if err != nil {
		t.Fatalf("NewNatsBus B: %v", err)
	}

	hubA := sse.NewHub(sse.HubOptions{ClientBufferSize: 16, HeartbeatInterval: 0, Bus: busA})
	hubB := sse.NewHub(sse.HubOptions{ClientBufferSize: 16, HeartbeatInterval: 0, Bus: busB})
	defer hubA.Stop()
	defer hubB.Stop()

	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()
	go busB.Subscribe(subCtx, hubB)
	time.Sleep(200 * time.Millisecond) // let the consumer establish

	cb := newTestClient("remote-1")
	if err := hubB.AddClient(cb); err != nil {
		t.Fatalf("AddClient: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	hubA.Broadcast(sse.NewEvent("hello", "via nats jetstream"))

	e := recv(t, cb, 5*time.Second)
	if e.Data != "via nats jetstream" {
		t.Fatalf("expected event via nats bus, got %q", e.Data)
	}
}

// TestNatsBusRedeliversNakedMessage is the key guarantee contrib/nats adds
// over contrib/redis's RedisBus: a message a consumer received but never
// acknowledged (simulating a crash mid-processing) is redelivered to that
// same durable consumer, instead of being silently lost like Redis Pub/Sub
// would lose it.
func TestNatsBusRedeliversNakedMessage(t *testing.T) {
	srv := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nc := newTestConn(t, srv)
	bus, err := NewNatsBus(ctx, nc, NatsBusOptions{Subject: "test.redeliver", InstanceID: "consumer-1", AckWait: time.Second})
	if err != nil {
		t.Fatalf("NewNatsBus: %v", err)
	}

	// Create the durable consumer (same name/config Subscribe would use)
	// BEFORE publishing: DeliverNewPolicy only sees messages published after
	// the consumer first exists, so the message must be fetched and naked
	// through this same consumer rather than a fresh one.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	consumer, err := js.CreateOrUpdateConsumer(ctx, "SSE_BUS", jetstream.ConsumerConfig{
		Durable:       "consumer-1",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		FilterSubject: "test.redeliver",
		AckWait:       time.Second,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer: %v", err)
	}

	if err := bus.Publish(ctx, sse.PublishContext{Target: sse.PublishTargetBroadcast}, sse.NewEvent("x", "payload")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Manually pull the message and Nak it: this simulates a consumer that
	// received the message but crashed before acking it.
	msgs, err := consumer.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	var gotMessage bool
	for msg := range msgs.Messages() {
		gotMessage = true
		_ = msg.Nak()
	}
	if !gotMessage {
		t.Fatal("expected to fetch the published message before it can be naked")
	}

	// Subscribe normally with the SAME durable consumer name; JetStream must
	// redeliver the naked message since it was never acked.
	hub := sse.NewHub(sse.HubOptions{ClientBufferSize: 16, HeartbeatInterval: 0})
	defer hub.Stop()
	client := newTestClient("c1")
	if err := hub.AddClient(client); err != nil {
		t.Fatalf("AddClient: %v", err)
	}

	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()
	go bus.Subscribe(subCtx, hub)

	e := recv(t, client, 5*time.Second)
	if e.Data != "payload" {
		t.Fatalf("expected redelivered event, got %q", e.Data)
	}
}

// TestNatsBusDeliverNewPolicyExcludesPreConsumerBacklog documents that a
// brand-new durable consumer (one that has never existed before) only sees
// events published after it's created, not the stream's full history. sse's
// ReplayStore already handles late-joiner catch-up; NatsBus only needs to
// guarantee delivery for consumers that have started at least once, and this
// test pins that boundary so a future change to the default DeliverPolicy
// doesn't silently alter it.
func TestNatsBusDeliverNewPolicyExcludesPreConsumerBacklog(t *testing.T) {
	srv := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pubConn := newTestConn(t, srv)
	busPub, err := NewNatsBus(ctx, pubConn, NatsBusOptions{Subject: "test.durable", InstanceID: "publisher"})
	if err != nil {
		t.Fatalf("NewNatsBus publisher: %v", err)
	}

	if err := busPub.Publish(ctx, sse.PublishContext{Target: sse.PublishTargetBroadcast}, sse.NewEvent("early", "missed-while-offline")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	subConn := newTestConn(t, srv)
	busSub, err := NewNatsBus(ctx, subConn, NatsBusOptions{Subject: "test.durable", InstanceID: "durable-subscriber"})
	if err != nil {
		t.Fatalf("NewNatsBus subscriber: %v", err)
	}

	hub := sse.NewHub(sse.HubOptions{ClientBufferSize: 16, HeartbeatInterval: 0})
	defer hub.Stop()
	client := newTestClient("late-joiner")
	if err := hub.AddClient(client); err != nil {
		t.Fatalf("AddClient: %v", err)
	}

	subCtx, subCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer subCancel()
	done := make(chan struct{})
	go func() {
		_ = busSub.Subscribe(subCtx, hub)
		close(done)
	}()
	select {
	case e := <-client.Recv():
		t.Fatalf("did not expect the pre-existing event to be delivered, got %q", e.Data)
	case <-subCtx.Done():
	}
	<-done
}
