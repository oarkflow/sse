package sse

import "fmt"

// ValidateHubOptions validates production-sensitive HubOptions.
func ValidateHubOptions(opts HubOptions) error {
	if opts.ClientBufferSize < 0 {
		return fmt.Errorf("client buffer size must be >= 0")
	}
	if opts.StickySize < 0 {
		return fmt.Errorf("sticky size must be >= 0")
	}
	if opts.MaxClients < 0 {
		return fmt.Errorf("max clients must be >= 0")
	}
	if opts.ReplayBufferSize < 0 {
		return fmt.Errorf("replay buffer size must be >= 0")
	}
	if opts.ReplayLimitPerConnect < 0 {
		return fmt.Errorf("replay limit per connect must be >= 0")
	}
	if opts.MaxGroupsPerClient < 0 {
		return fmt.Errorf("max groups per client must be >= 0")
	}
	if opts.MaxEventBytes < 0 {
		return fmt.Errorf("max event bytes must be >= 0")
	}
	if opts.MaxEventTypeLength < 0 {
		return fmt.Errorf("max event type length must be >= 0")
	}
	if opts.MaxTopicLength < 0 {
		return fmt.Errorf("max topic length must be >= 0")
	}
	if opts.MaxGroupLength < 0 {
		return fmt.Errorf("max group length must be >= 0")
	}
	if opts.MaxDropsPerClient < 0 {
		return fmt.Errorf("max drops per client must be >= 0")
	}
	if opts.SlowConsumerLogInterval < 0 {
		return fmt.Errorf("slow consumer log interval must be >= 0")
	}
	if opts.BackpressurePolicy != "" &&
		opts.BackpressurePolicy != BackpressureDropNewest &&
		opts.BackpressurePolicy != BackpressureDropOldest &&
		opts.BackpressurePolicy != BackpressureDisconnectSlow {
		return fmt.Errorf("invalid backpressure policy: %s", opts.BackpressurePolicy)
	}
	return nil
}

// ValidateHandlerOptions validates production-sensitive HandlerOptions.
func ValidateHandlerOptions(opts HandlerOptions) error {
	if opts.RequireAuth && opts.Authenticate == nil {
		return fmt.Errorf("require auth enabled but Authenticate hook is nil")
	}
	if opts.MaxConnectionsPerIP < 0 {
		return fmt.Errorf("max connections per ip must be >= 0")
	}
	if opts.MaxConnectsPerIP < 0 {
		return fmt.Errorf("max connects per ip must be >= 0")
	}
	if opts.MaxTopicsPerClient < 0 {
		return fmt.Errorf("max topics per client must be >= 0")
	}
	if opts.ConnectRateWindow < 0 {
		return fmt.Errorf("connect rate window must be >= 0")
	}
	if opts.StreamErrorLogInterval < 0 {
		return fmt.Errorf("stream error log interval must be >= 0")
	}
	return nil
}
