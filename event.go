package sse

import (
	"bytes"
	"fmt"
	"strings"
	"time"
)

// Event is the data structure sent over the SSE stream.
type Event struct {
	// ID is the event ID. Set to allow clients to resume via Last-Event-ID.
	ID string

	// Type is the event type (maps to the `event:` SSE field).
	// Defaults to "message".
	Type string

	// Data is the payload. Can be any string (JSON is common).
	Data string

	// Retry suggests the reconnection delay in milliseconds.
	// 0 means omit the field.
	Retry int

	// Topic is used internally for sticky-event bucketing.
	// It is NOT sent over the wire.
	Topic string

	// Sticky controls whether this event is retained for late joiners.
	Sticky bool

	// CreatedAt is set automatically when the event is created.
	CreatedAt time.Time
}

// NewEvent is a convenience constructor.
func NewEvent(eventType, data string) *Event {
	return &Event{
		ID:        newID(),
		Type:      eventType,
		Data:      data,
		CreatedAt: time.Now(),
	}
}

// WithTopic marks the event for sticky storage under the given topic.
func (e *Event) WithTopic(topic string) *Event {
	e.Topic = topic
	e.Sticky = true
	return e
}

// WithRetry sets the client reconnect hint (milliseconds).
func (e *Event) WithRetry(ms int) *Event {
	e.Retry = ms
	return e
}

// Encode serialises the event to the SSE wire format.
// Multi-line Data values are handled correctly.
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

// heartbeatEvent returns a keep-alive SSE comment (: keep-alive).
func heartbeatEvent() *Event {
	return &Event{Data: ": keep-alive\n\n"} // raw comment, bypass Encode
}
