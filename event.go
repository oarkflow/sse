package sse

import (
	"bytes"
	"fmt"
	"strings"
	"time"
)

// Event represents a single Server-Sent Event. Events are serialized to the
// SSE wire format and sent to connected clients.
//
// The SSE wire format consists of:
//
//	id: <event-id>
//	event: <event-type>
//	data: <first-line>
//	data: <second-line>
//	retry: <milliseconds>
//
// Each event is terminated by a blank line. Multi-line data is handled by
// prefixing each line with "data: ".
//
// Example usage:
//
//	// Simple event
//	event := sse.NewEvent("message", "Hello World")
//
//	// Event with JSON payload
//	event := sse.NewEvent("notification", `{"title": "Alert", "body": "New message"}`)
//
//	// Sticky event for late joiners
//	event := sse.NewEvent("status", `{"online": 150}`).WithTopic("status")
//
//	// Event with retry hint
//	event := sse.NewEvent("ping", "").WithRetry(3000)
type Event struct {
	// ID is the unique event identifier. When set, clients can use the
	// Last-Event-ID header to resume from this event after reconnection.
	// If empty, no ID is sent over the wire.
	ID string

	// Type is the event type name (maps to the "event:" SSE field).
	// Clients can addEventListener for specific types. If empty, defaults to "message".
	Type string

	// Data is the event payload. Can be any string; JSON is commonly used.
	// Multi-line data is automatically handled during encoding.
	Data string

	// Retry suggests the client reconnection delay in milliseconds.
	// If > 0, the "retry:" field is included. Set to 0 to omit.
	Retry int

	// Topic is used internally for sticky-event bucketing.
	// Events with a topic are stored and replayed to clients who subscribe to that topic.
	// This field is NOT sent over the wire.
	Topic string

	// Sticky controls whether this event is retained for late-joining clients.
	// When true and Topic is set, the event is stored in the sticky buffer.
	Sticky bool

	// CreatedAt records when the event was created. Set automatically by NewEvent.
	CreatedAt time.Time
}

// NewEvent creates a new Event with a generated ID and the given type and data.
// The event is assigned a unique ID for replay support and the current timestamp.
//
// Parameters:
//   - eventType: The event type name (e.g., "message", "notification", "update")
//   - data: The event payload (commonly JSON-encoded data)
//
// Example:
//
//	event := sse.NewEvent("notification", `{"message": "Hello"}`)
func NewEvent(eventType, data string) *Event {
	return &Event{
		ID:        newID(),
		Type:      eventType,
		Data:      data,
		CreatedAt: time.Now(),
	}
}

// WithTopic marks the event as sticky and assigns it to a topic for late-joiner replay.
// Clients subscribing to this topic will receive this event upon connection.
//
// Example:
//
//	event := sse.NewEvent("status", `{"users": 150}`).WithTopic("status")
//	hub.Broadcast(event)
func (e *Event) WithTopic(topic string) *Event {
	e.Topic = topic
	e.Sticky = true
	return e
}

// WithRetry sets the client reconnection hint in milliseconds.
// Clients receiving this event will wait the specified duration before
// attempting to reconnect if the connection is lost.
//
// Example:
//
//	event := sse.NewEvent("ping", "").WithRetry(5000) // 5 second retry
func (e *Event) WithRetry(ms int) *Event {
	e.Retry = ms
	return e
}

// Encode serializes the event to the SSE wire format.
// The output follows the SSE specification with proper handling of multi-line data.
// Each line of data is prefixed with "data: " as required by the spec.
//
// The output format:
//
//	id: <ID>           (if ID is set)
//	event: <Type>      (defaults to "message" if empty)
//	data: <line1>
//	data: <line2>      (for multi-line data)
//	retry: <Retry>     (if Retry > 0)
//	<blank line>       (terminates event)
func (e *Event) Encode() []byte {
	var buf bytes.Buffer

	if e.ID != "" {
		fmt.Fprintf(&buf, "id: %s\n", e.ID)
	}

	eventType := e.Type
	if eventType == "" {
		eventType = "message"
	}
	fmt.Fprintf(&buf, "event: %s\n", eventType)

	// Each line of data must be prefixed with "data: "
	for _, line := range strings.Split(e.Data, "\n") {
		fmt.Fprintf(&buf, "data: %s\n", line)
	}

	if e.Retry > 0 {
		fmt.Fprintf(&buf, "retry: %d\n", e.Retry)
	}

	buf.WriteString("\n") // blank line terminates the event
	return buf.Bytes()
}

// heartbeatEvent returns a keep-alive SSE comment.
// SSE comments start with a colon and are ignored by clients but keep
// the connection alive through proxies and load balancers.
func heartbeatEvent() *Event {
	return &Event{Data: ": keep-alive\n\n"} // raw comment, bypass Encode
}
