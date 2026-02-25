package sse

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrClientNotFound is returned when targeting a client that isn't connected.
	ErrClientNotFound = errors.New("sse: client not found")

	// ErrMaxClientsReached is returned when the hub's MaxClients limit is hit.
	ErrMaxClientsReached = errors.New("sse: maximum number of clients reached")

	// ErrHubClosed is returned when operating on a stopped hub.
	ErrHubClosed = errors.New("sse: hub closed")

	// ErrUnauthorized is returned when handler authentication fails.
	ErrUnauthorized = errors.New("sse: unauthorized")

	// ErrHubDraining is returned when the hub is draining and rejects new clients.
	ErrHubDraining = errors.New("sse: hub draining")

	// ErrClientGroupLimitExceeded is returned when a client exceeds max group membership.
	ErrClientGroupLimitExceeded = errors.New("sse: client group limit exceeded")

	// ErrInvalidEvent is returned when an event payload is malformed.
	ErrInvalidEvent = errors.New("sse: invalid event")

	// ErrEventTooLarge is returned when an event exceeds configured limits.
	ErrEventTooLarge = errors.New("sse: event too large")

	// ErrTLSRequired is returned when secure transport is required but absent.
	ErrTLSRequired = errors.New("sse: tls required")
)

// newID generates a short random ID for events and clients.
func newID() string {
	id, err := uuid.NewRandom()
	if err != nil {
		// Fallback: use timestamp-based ID.
		return fmt.Sprintf("%d", timeNow())
	}
	return id.String()
}

// Helpers to make timeNow mockable in tests.
var timeNow = func() int64 {
	return time.Now().UnixNano()
}
