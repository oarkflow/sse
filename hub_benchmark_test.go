package sse

import (
	"fmt"
	"testing"
)

func BenchmarkBroadcastThroughput(b *testing.B) {
	hub := NewHub(HubOptions{
		HeartbeatInterval: 0,
		ClientBufferSize:  512,
	})
	defer hub.Stop()

	for i := 0; i < 1000; i++ {
		_ = hub.AddClient(NewClient(ClientOptions{
			ID:         fmt.Sprintf("c-%d", i),
			BufferSize: 512,
		}))
	}

	event := NewEvent("bench", `{"k":"v"}`).WithTopic("bench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hub.Broadcast(event)
	}
}

func BenchmarkReplayAfterReconnect(b *testing.B) {
	hub := NewHub(HubOptions{
		HeartbeatInterval:     0,
		ReplayBufferSize:      20000,
		ReplayLimitPerConnect: 2000,
	})
	defer hub.Stop()

	var first *Event
	for i := 0; i < 5000; i++ {
		e := NewEvent("bench", fmt.Sprintf("payload-%d", i)).WithTopic("bench")
		if i == 1000 {
			first = e
		}
		hub.Broadcast(e)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := NewClient(ClientOptions{
			ID:          fmt.Sprintf("replay-%d", i),
			LastEventID: first.ID,
			Topics:      []string{"bench"},
			BufferSize:  4096,
		})
		_ = hub.AddClient(c)
		hub.RemoveClient(c)
	}
}
