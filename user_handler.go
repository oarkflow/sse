package sse

import "github.com/gofiber/fiber/v3"

// UserHubHandler returns a GoFiber handler that uses a UserHub.
// Identical to Handler but uses UserHub's AddClient/RemoveClient.
func UserHubHandler(hub *UserHub, opts HandlerOptions) fiber.Handler {
	return buildSSEHandler(hub.opts.ClientBufferSize, hub.AddClient, hub.RemoveClient, opts)
}

// fiber:context-methods migrated
