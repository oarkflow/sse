package sseredis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/oarkflow/sse"
	"github.com/redis/go-redis/v9"
)

func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(mr.Close)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client
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

func TestRedisReplayStoreAppendAndReplay(t *testing.T) {
	client := newTestRedis(t)
	store := NewRedisReplayStore(client, RedisReplayStoreOptions{MaxEvents: 10})

	e1 := sse.NewEvent("msg", "one")
	e2 := sse.NewEvent("msg", "two").WithTopic("chat")
	e3 := sse.NewEvent("msg", "three").WithTopic("chat")
	store.Append(e1)
	store.Append(e2)
	store.Append(e3)

	all := store.ReplayAfter("", nil, 10)
	if len(all) != 3 {
		t.Fatalf("expected 3 events, got %d", len(all))
	}

	after := store.ReplayAfter(e1.ID, nil, 10)
	if len(after) != 2 || after[0].Data != "two" {
		t.Fatalf("unexpected replay-after result: %+v", after)
	}

	filtered := store.ReplayAfter("", map[string]bool{"chat": true}, 10)
	if len(filtered) != 2 {
		t.Fatalf("expected topic-filtered events, got %d", len(filtered))
	}
}

func TestRedisReplayStoreTrimsToMax(t *testing.T) {
	client := newTestRedis(t)
	store := NewRedisReplayStore(client, RedisReplayStoreOptions{MaxEvents: 2})

	store.Append(sse.NewEvent("msg", "one"))
	store.Append(sse.NewEvent("msg", "two"))
	store.Append(sse.NewEvent("msg", "three"))

	all := store.ReplayAfter("", nil, 10)
	if len(all) != 2 {
		t.Fatalf("expected trimmed to 2 events, got %d", len(all))
	}
	if all[0].Data != "two" || all[1].Data != "three" {
		t.Fatalf("unexpected trimmed contents: %+v", all)
	}
}

func TestRedisPresenceStore(t *testing.T) {
	client := newTestRedis(t)
	store := NewRedisPresenceStore(client, RedisPresenceStoreOptions{})

	if err := store.Add("user-1", "dev-a"); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := store.Add("user-1", "dev-b"); err != nil {
		t.Fatalf("Add: %v", err)
	}

	if online, err := store.IsOnline("user-1"); err != nil || !online {
		t.Fatalf("expected user-1 online, got %v %v", online, err)
	}
	if n, err := store.ConnectionCount("user-1"); err != nil || n != 2 {
		t.Fatalf("expected 2 connections, got %d %v", n, err)
	}

	users, err := store.OnlineUsers()
	if err != nil || len(users) != 1 || users[0] != "user-1" {
		t.Fatalf("unexpected online users: %v %v", users, err)
	}

	if err := store.Remove("user-1", "dev-a"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if n, err := store.ConnectionCount("user-1"); err != nil || n != 1 {
		t.Fatalf("expected 1 connection after remove, got %d %v", n, err)
	}

	if err := store.Remove("user-1", "dev-b"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if online, err := store.IsOnline("user-1"); err != nil || online {
		t.Fatalf("expected user-1 offline, got %v %v", online, err)
	}
	users, err = store.OnlineUsers()
	if err != nil || len(users) != 0 {
		t.Fatalf("expected no online users, got %v %v", users, err)
	}
}

func TestRedisConnectionLimiter(t *testing.T) {
	client := newTestRedis(t)
	limiter := NewRedisConnectionLimiter(client, RedisConnectionLimiterOptions{Max: 2})

	if !limiter.Acquire("1.2.3.4") {
		t.Fatal("expected first acquire to succeed")
	}
	if !limiter.Acquire("1.2.3.4") {
		t.Fatal("expected second acquire to succeed")
	}
	if limiter.Acquire("1.2.3.4") {
		t.Fatal("expected third acquire to fail (limit reached)")
	}

	limiter.Release("1.2.3.4")
	if !limiter.Acquire("1.2.3.4") {
		t.Fatal("expected acquire to succeed after release")
	}
}

func TestRedisConnectionLimiterUnlimited(t *testing.T) {
	client := newTestRedis(t)
	limiter := NewRedisConnectionLimiter(client, RedisConnectionLimiterOptions{})
	for range 5 {
		if !limiter.Acquire("any") {
			t.Fatal("expected unlimited limiter to always acquire")
		}
	}
}

func TestRedisBusPublishSubscribe(t *testing.T) {
	client := newTestRedis(t)
	busA := NewRedisBus(client, RedisBusOptions{Channel: "test:bus"})
	busB := NewRedisBus(client, RedisBusOptions{Channel: "test:bus"})

	hubA := sse.NewHub(sse.HubOptions{ClientBufferSize: 16, HeartbeatInterval: 0, Bus: busA})
	hubB := sse.NewHub(sse.HubOptions{ClientBufferSize: 16, HeartbeatInterval: 0, Bus: busB})
	defer hubA.Stop()
	defer hubB.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go busB.Subscribe(ctx, hubB)
	time.Sleep(50 * time.Millisecond) // let the Redis subscription establish

	cb := newTestClient("remote-1")
	if err := hubB.AddClient(cb); err != nil {
		t.Fatalf("AddClient: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	hubA.Broadcast(sse.NewEvent("hello", "via redis"))

	e := recv(t, cb, 2*time.Second)
	if e.Data != "via redis" {
		t.Fatalf("expected event via redis bus, got %q", e.Data)
	}
}
