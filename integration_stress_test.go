//go:build integration

package sse

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestIntegrationReconnectStorm(t *testing.T) {
	hub := NewHub(HubOptions{
		HeartbeatInterval:     0,
		ClientBufferSize:      64,
		ReplayBufferSize:      10000,
		ReplayLimitPerConnect: 256,
	})
	defer hub.Stop()

	for i := 0; i < 500; i++ {
		hub.Broadcast(NewEvent("state", fmt.Sprintf("snapshot-%d", i)).WithTopic("state"))
	}

	var wg sync.WaitGroup
	for i := 0; i < 300; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := NewClient(ClientOptions{
				ID:          fmt.Sprintf("r-%d", i),
				LastEventID: "",
				Topics:      []string{"state"},
				BufferSize:  512,
			})
			if err := hub.AddClient(c); err != nil {
				t.Errorf("add client failed: %v", err)
				return
			}
			time.Sleep(2 * time.Millisecond)
			hub.RemoveClient(c)
		}(i)
	}
	wg.Wait()
}

func TestIntegrationMixedSlowAndFastConsumers(t *testing.T) {
	hub := NewHub(HubOptions{
		HeartbeatInterval:       0,
		ClientBufferSize:        2,
		BackpressurePolicy:      BackpressureDropNewest,
		MaxDropsPerClient:       5,
		SlowConsumerLogInterval: 200 * time.Millisecond,
	})
	defer hub.Stop()

	for i := 0; i < 50; i++ {
		_ = hub.AddClient(NewClient(ClientOptions{
			ID:         fmt.Sprintf("fast-%d", i),
			BufferSize: 64,
		}))
	}
	for i := 0; i < 10; i++ {
		_ = hub.AddClient(NewClient(ClientOptions{
			ID:         fmt.Sprintf("slow-%d", i),
			BufferSize: 1,
		}))
	}

	for i := 0; i < 200; i++ {
		hub.Broadcast(NewEvent("tick", fmt.Sprintf("%d", i)))
	}
	stats := hub.Stats()
	if stats.SlowConsumerDrops == 0 {
		t.Fatalf("expected slow consumer drops > 0")
	}
}
