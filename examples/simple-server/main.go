package main

import (
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/sse"
)

func main() {
	// Create the SSE hub with basic configuration
	hub := sse.NewHub(sse.HubOptions{
		StickySize:         10,               // Retain last 10 events per topic
		ClientBufferSize:   64,               // Events per client buffer
		HeartbeatInterval:  30 * time.Second, // Keep connections alive
		MaxClients:         1000,             // Maximum concurrent connections
		BackpressurePolicy: sse.BackpressureDropNewest,
	})
	defer hub.Stop()

	// Create fh app
	app := fh.New()

	// SSE endpoint - clients connect here to receive events
	app.Get("/events", sse.Handler(hub, sse.HandlerOptions{
		AllowedOrigins: []string{"http://localhost:8080"}, // Allow local client
	}))

	// Health endpoints
	app.Get("/healthz", sse.HealthHandler())
	app.Get("/readyz", sse.ReadinessHandler(hub, nil))

	// Broadcast endpoint - send event to all connected clients
	app.Post("/broadcast", func(ctx fh.Ctx) error {
		event := sse.NewEvent("message", string(ctx.Body()))
		hub.Broadcast(event)
		return ctx.JSON(fh.Map{"ok": true})
	})

	// Send to specific client endpoint
	app.Post("/send/:clientId", func(ctx fh.Ctx) error {
		clientId := ctx.Params("clientId")
		event := sse.NewEvent("message", string(ctx.Body()))
		if err := hub.Send(clientId, event); err != nil {
			return ctx.Status(404).JSON(fh.Map{"error": "client not found"})
		}
		return ctx.JSON(fh.Map{"ok": true})
	})

	// Example: Send a periodic event
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for t := range ticker.C {
			payload := `{"time": "` + t.Format(time.RFC3339) + `", "message": "periodic update"}`
			hub.Broadcast(sse.NewEvent("periodic", payload).WithTopic("updates"))
		}
	}()

	// Start server
	println("Simple SSE server running on http://localhost:3000")
	println("Endpoints:")
	println("  GET  /events        - SSE connection endpoint")
	println("  POST /broadcast     - Send event to all clients")
	println("  POST /send/:clientId - Send event to specific client")
	println("  GET  /healthz       - Health check")
	println("  GET  /readyz        - Readiness check")
	_ = app.Listen(":3000")
}
