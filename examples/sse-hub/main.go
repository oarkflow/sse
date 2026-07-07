package main

import (
	"encoding/json"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/sse"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	hub := sse.NewHub(sse.HubOptions{
		StickySize:        20,
		ClientBufferSize:  128,
		HeartbeatInterval: 25 * time.Second,
		MaxClients:        10_000,
	})

	handlerMetrics := &sse.HandlerMetrics{}
	handlerOpts := sse.HandlerOptions{
		AllowedOrigins:   []string{"*"},
		AllowCredentials: true,
		Logger:           logger,
		Metrics:          handlerMetrics,
	}

	app := fh.New()

	app.Get("/events", sse.Handler(hub, handlerOpts))
	app.Get("/healthz", sse.HealthHandler())
	app.Get("/readyz", sse.ReadinessHandler(hub, nil))
	app.Get("/metrics/sse", sse.MetricsHandler(hub, handlerMetrics))

	app.Post("/broadcast", func(c fh.Ctx) error {
		var body struct {
			Type string `json:"type"`
			Data string `json:"data"`
		}
		if err := c.BodyParser(&body); err != nil {
			return c.Status(400).JSON(fh.Map{"error": err.Error()})
		}
		hub.Broadcast(sse.NewEvent(body.Type, body.Data))
		return c.JSON(fh.Map{"ok": true})
	})

	app.Post("/send/:clientId", func(c fh.Ctx) error {
		var body struct {
			Type string `json:"type"`
			Data string `json:"data"`
		}
		if err := c.BodyParser(&body); err != nil {
			return c.Status(400).JSON(fh.Map{"error": err.Error()})
		}
		if err := hub.Send(c.Param("clientId"), sse.NewEvent(body.Type, body.Data)); err != nil {
			return c.Status(404).JSON(fh.Map{"error": err.Error()})
		}
		return c.JSON(fh.Map{"ok": true})
	})

	app.Post("/group/:name", func(c fh.Ctx) error {
		var body struct {
			Type string `json:"type"`
			Data string `json:"data"`
		}
		if err := c.BodyParser(&body); err != nil {
			return c.Status(400).JSON(fh.Map{"error": err.Error()})
		}
		hub.SendToGroup(c.Param("name"), sse.NewEvent(body.Type, body.Data))
		return c.JSON(fh.Map{"ok": true})
	})

	app.Get("/stats", func(c fh.Ctx) error {
		return c.JSON(hub.Stats())
	})

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			payload, _ := json.Marshal(map[string]any{
				"status": "online",
				"time":   time.Now().Format(time.RFC3339),
			})
			hub.Broadcast(sse.NewEvent("status", string(payload)))
		}
	}()

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		logger.Info("shutting down")
		hub.Drain(10 * time.Second)
		app.Shutdown()
	}()

	logger.Info("sse server starting", "addr", ":3000")
	log.Fatal(app.Listen(":3000"))
}
