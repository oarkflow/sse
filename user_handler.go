package sse

import "github.com/oarkflow/fh"

// UserHubHandler returns an fh handler that uses a UserHub.
// Identical to Handler but uses UserHub's AddClient/RemoveClient.
func UserHubHandler(hub *UserHub, opts HandlerOptions) fh.Handler {
	return buildSSEHandler(hub.opts.ClientBufferSize, hub.AddClient, hub.RemoveClient, opts)
}
