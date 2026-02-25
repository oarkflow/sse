package sse

import (
	"strings"
	"testing"
)

func TestMetricsText(t *testing.T) {
	hub := NewHub(HubOptions{HeartbeatInterval: 0})
	defer hub.Stop()
	_ = hub.AddClient(NewClient(ClientOptions{ID: "c1", BufferSize: 8}))
	hub.Broadcast(NewEvent("msg", "hello"))

	metrics := &HandlerMetrics{}
	metrics.TotalConnections.Add(2)
	metrics.ActiveConnections.Add(1)

	text := MetricsText(hub, metrics)
	for _, metric := range []string{
		"sse_connected_clients",
		"sse_hub_delivered_events",
		"sse_handler_total_connections",
		"sse_handler_active_connections",
	} {
		if !strings.Contains(text, metric) {
			t.Fatalf("expected metric %q in output", metric)
		}
	}
}
