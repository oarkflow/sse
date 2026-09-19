// Package sseotel bridges the sse package's Hub/HandlerMetrics counters into
// OpenTelemetry observable instruments. It is a separate Go module so the
// core sse module stays dependency-free (other than the fh HTTP framework);
// pull it in only if you use OpenTelemetry.
package sseotel

import (
	"context"

	"github.com/oarkflow/sse"
	"go.opentelemetry.io/otel/metric"
)

// RegisterOTelMetrics registers observable OpenTelemetry instruments that
// mirror Hub and HandlerMetrics state. It returns an unregister function
// that should be called on shutdown.
//
// Example usage:
//
//	meter := otel.Meter("github.com/oarkflow/sse")
//	unregister, err := sseotel.RegisterOTelMetrics(meter, hub, handlerMetrics)
//	defer unregister()
func RegisterOTelMetrics(meter metric.Meter, hub *sse.Hub, handler *sse.HandlerMetrics) (func(), error) {
	connectedClients, err := meter.Int64ObservableGauge("sse.connected_clients")
	if err != nil {
		return nil, err
	}
	deliveredEvents, err := meter.Int64ObservableCounter("sse.hub.delivered_events")
	if err != nil {
		return nil, err
	}
	droppedEvents, err := meter.Int64ObservableCounter("sse.hub.dropped_events")
	if err != nil {
		return nil, err
	}
	replayedEvents, err := meter.Int64ObservableCounter("sse.hub.replayed_events")
	if err != nil {
		return nil, err
	}
	busPublishedEvents, err := meter.Int64ObservableCounter("sse.hub.bus_published_events")
	if err != nil {
		return nil, err
	}
	busPublishErrors, err := meter.Int64ObservableCounter("sse.hub.bus_publish_errors")
	if err != nil {
		return nil, err
	}
	busDispatchedEvents, err := meter.Int64ObservableCounter("sse.hub.bus_dispatched_events")
	if err != nil {
		return nil, err
	}
	activeConnections, err := meter.Int64ObservableGauge("sse.handler.active_connections")
	if err != nil {
		return nil, err
	}
	authFailures, err := meter.Int64ObservableCounter("sse.handler.auth_failures")
	if err != nil {
		return nil, err
	}

	reg, err := meter.RegisterCallback(func(ctx context.Context, obs metric.Observer) error {
		if hub != nil {
			s := hub.Stats()
			obs.ObserveInt64(connectedClients, int64(s.ConnectedClients))
			obs.ObserveInt64(deliveredEvents, int64(s.DeliveredEvents))
			obs.ObserveInt64(droppedEvents, int64(s.DroppedEvents))
			obs.ObserveInt64(replayedEvents, int64(s.ReplayedEvents))
			obs.ObserveInt64(busPublishedEvents, int64(s.BusPublishedEvents))
			obs.ObserveInt64(busPublishErrors, int64(s.BusPublishErrors))
			obs.ObserveInt64(busDispatchedEvents, int64(s.BusDispatchedEvents))
		}
		if handler != nil {
			s := handler.Snapshot()
			obs.ObserveInt64(activeConnections, s.ActiveConnections)
			obs.ObserveInt64(authFailures, int64(s.AuthFailures))
		}
		return nil
	},
		connectedClients, deliveredEvents, droppedEvents, replayedEvents,
		busPublishedEvents, busPublishErrors, busDispatchedEvents,
		activeConnections, authFailures,
	)
	if err != nil {
		return nil, err
	}
	return func() { _ = reg.Unregister() }, nil
}
