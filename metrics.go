package sse

import (
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v3"
)

// MetricsText returns a Prometheus-compatible text exposition payload.
func MetricsText(hub *Hub, handlerMetrics *HandlerMetrics) string {
	var b strings.Builder

	b.WriteString("# TYPE sse_connected_clients gauge\n")
	b.WriteString("# TYPE sse_hub_delivered_events counter\n")
	b.WriteString("# TYPE sse_hub_dropped_events counter\n")
	b.WriteString("# TYPE sse_hub_slow_consumer_drops counter\n")
	b.WriteString("# TYPE sse_hub_rejected_connections counter\n")
	b.WriteString("# TYPE sse_hub_replayed_events counter\n")
	b.WriteString("# TYPE sse_hub_replay_misses counter\n")
	b.WriteString("# TYPE sse_hub_rejected_events counter\n")
	b.WriteString("# TYPE sse_handler_total_connections counter\n")
	b.WriteString("# TYPE sse_handler_active_connections gauge\n")
	b.WriteString("# TYPE sse_handler_rejected_connections counter\n")
	b.WriteString("# TYPE sse_handler_auth_failures counter\n")
	b.WriteString("# TYPE sse_handler_origin_rejected counter\n")
	b.WriteString("# TYPE sse_handler_ip_limit_rejected counter\n")
	b.WriteString("# TYPE sse_handler_connect_rate_rejected counter\n")
	b.WriteString("# TYPE sse_handler_topic_limit_rejected counter\n")
	b.WriteString("# TYPE sse_handler_draining_rejected counter\n")
	b.WriteString("# TYPE sse_handler_tls_rejected counter\n")
	b.WriteString("# TYPE sse_handler_stream_errors counter\n")

	if hub != nil {
		stats := hub.Stats()
		fmt.Fprintf(&b, "sse_connected_clients %d\n", stats.ConnectedClients)
		fmt.Fprintf(&b, "sse_hub_delivered_events %d\n", stats.DeliveredEvents)
		fmt.Fprintf(&b, "sse_hub_dropped_events %d\n", stats.DroppedEvents)
		fmt.Fprintf(&b, "sse_hub_slow_consumer_drops %d\n", stats.SlowConsumerDrops)
		fmt.Fprintf(&b, "sse_hub_rejected_connections %d\n", stats.RejectedConnections)
		fmt.Fprintf(&b, "sse_hub_replayed_events %d\n", stats.ReplayedEvents)
		fmt.Fprintf(&b, "sse_hub_replay_misses %d\n", stats.ReplayMisses)
		fmt.Fprintf(&b, "sse_hub_rejected_events %d\n", stats.RejectedEvents)
	}

	if handlerMetrics != nil {
		s := handlerMetrics.Snapshot()
		fmt.Fprintf(&b, "sse_handler_total_connections %d\n", s.TotalConnections)
		fmt.Fprintf(&b, "sse_handler_active_connections %d\n", s.ActiveConnections)
		fmt.Fprintf(&b, "sse_handler_rejected_connections %d\n", s.RejectedConnections)
		fmt.Fprintf(&b, "sse_handler_auth_failures %d\n", s.AuthFailures)
		fmt.Fprintf(&b, "sse_handler_origin_rejected %d\n", s.OriginRejected)
		fmt.Fprintf(&b, "sse_handler_ip_limit_rejected %d\n", s.IPLimitRejected)
		fmt.Fprintf(&b, "sse_handler_connect_rate_rejected %d\n", s.ConnectRateRejected)
		fmt.Fprintf(&b, "sse_handler_topic_limit_rejected %d\n", s.TopicLimitRejected)
		fmt.Fprintf(&b, "sse_handler_draining_rejected %d\n", s.DrainingRejected)
		fmt.Fprintf(&b, "sse_handler_tls_rejected %d\n", s.TLSRejected)
		fmt.Fprintf(&b, "sse_handler_stream_errors %d\n", s.StreamErrors)
	}

	return b.String()
}

// MetricsHandler exposes hub and handler metrics as Prometheus text format.
func MetricsHandler(hub *Hub, handlerMetrics *HandlerMetrics) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		ctx.Set(fiber.HeaderContentType, "text/plain; version=0.0.4; charset=utf-8")
		return ctx.SendString(MetricsText(hub, handlerMetrics))
	}
}
