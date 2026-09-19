package sse

import (
	"errors"
	"time"
)

// PublishTarget describes where an event is being published.
type PublishTarget string

const (
	PublishTargetBroadcast       PublishTarget = "broadcast"
	PublishTargetClient          PublishTarget = "client"
	PublishTargetGroup           PublishTarget = "group"
	PublishTargetBroadcastExcept PublishTarget = "broadcast_except"
	PublishTargetUser            PublishTarget = "user"
)

// PublishContext describes the publish destination for authorization checks.
type PublishContext struct {
	Target     PublishTarget
	ClientID   string
	Group      string
	ExcludeIDs []string
	UserID     string
}

// Dispatcher receives events that originated on another instance (via a Bus)
// and fans them out to locally connected clients only. Implementations must
// not re-publish to the Bus, or messages would loop across instances forever.
type Dispatcher interface {
	DispatchRemote(ctx PublishContext, e *Event)
}

// HubLifecycleEventType describes hub lifecycle transitions.
type HubLifecycleEventType string

const (
	LifecycleDrainStarted   HubLifecycleEventType = "drain_started"
	LifecycleDrainCompleted HubLifecycleEventType = "drain_completed"
	LifecycleStopStarted    HubLifecycleEventType = "stop_started"
	LifecycleStopCompleted  HubLifecycleEventType = "stop_completed"
)

// HubLifecycleEvent captures lifecycle state transitions for observability.
type HubLifecycleEvent struct {
	Type             HubLifecycleEventType
	OccurredAt       time.Time
	ConnectedClients int
	Duration         time.Duration
	Timeout          time.Duration
	Reason           string
}

func wrapPublishAuthorizationError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrPublishUnauthorized) {
		return err
	}
	return errors.Join(ErrPublishUnauthorized, err)
}
