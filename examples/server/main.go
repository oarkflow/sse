package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/logger"
	"github.com/oarkflow/sse"
)

func main() {
	appLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	allowedOrigins := parseCSVEnv("SSE_ALLOWED_ORIGINS",
		"http://localhost:5173,http://localhost:3000")
	authTokens := parseTokenUserMapEnv("SSE_AUTH_TOKENS", "demo-token:demo-user")
	adminToken := strings.TrimSpace(os.Getenv("SSE_ADMIN_TOKEN"))
	if adminToken == "" {
		adminToken = randomToken(24)
		appLogger.Warn("SSE_ADMIN_TOKEN not set; generated ephemeral token", "token", adminToken)
	}
	maxConnPerIP := parseIntEnv("SSE_MAX_CONN_PER_IP", 20)
	maxEventBytes := parseIntEnv("SSE_MAX_EVENT_BYTES", 64*1024)
	maxEventTypeLen := parseIntEnv("SSE_MAX_EVENT_TYPE_LEN", 64)
	manageRateLimit := parseIntEnv("SSE_MANAGEMENT_RATE_PER_MIN", 120)
	requireAuth := parseBoolEnv("SSE_REQUIRE_AUTH", true)
	requireTLS := parseBoolEnv("SSE_REQUIRE_TLS", false)
	listenAddr := envOrDefault("SSE_LISTEN_ADDR", ":3000")

	// ── 1. Create the Hub ──────────────────────────────────────────────────
	hubOpts := sse.HubOptions{
		StickySize:         20,               // retain last 20 events per topic
		ClientBufferSize:   128,              // events per client buffer
		HeartbeatInterval:  25 * time.Second, // keep NAT/proxies alive
		MaxClients:         10_000,
		ReplayBufferSize:   10_000,
		BackpressurePolicy: sse.BackpressureDisconnectSlow,
		MaxGroupsPerClient: 128,
		MaxEventBytes:      maxEventBytes,
		MaxEventTypeLength: maxEventTypeLen,
	}
	if err := sse.ValidateHubOptions(hubOpts); err != nil {
		log.Fatal(err)
	}
	hub := sse.NewUserHub(hubOpts)

	// ── 2. Create Fiber app ────────────────────────────────────────────────
	app := fiber.New(fiber.Config{
		// Streaming requires disabling the built-in response buffering.
		StreamRequestBody: true,
	})
	app.Use(logger.New())

	// ── 3. SSE endpoint ────────────────────────────────────────────────────
	//
	// Clients connect to GET /events and stay connected indefinitely.
	// Query params:
	//   ?userId=alice        – identifies the user (for user-targeted sends)
	//   ?topics=orders,chat  – filter sticky replay to these topics
	//
	handlerMetrics := &sse.HandlerMetrics{}
	handlerOpts := sse.HandlerOptions{
		// Extract topic subscriptions from query param.
		TopicsFromCtx: func(ctx fiber.Ctx) []string {
			raw := ctx.Query("topics")
			if raw == "" {
				return nil
			}
			var topics []string
			for _, t := range splitComma(raw) {
				topics = append(topics, t)
			}
			return topics
		},

		RequireAuth:         requireAuth,
		AllowedOrigins:      allowedOrigins,
		AllowCredentials:    true,
		MaxConnectionsPerIP: maxConnPerIP,
		MaxConnectsPerIP:    maxConnPerIP * 3,
		ConnectRateWindow:   time.Minute,
		MaxTopicsPerClient:  64,
		RequireTLS:          requireTLS,
		AllowLocalInsecure:  true,
		Logger:              appLogger,
		Metrics:             handlerMetrics,
		Authenticate: func(ctx fiber.Ctx) (*sse.AuthResult, error) {
			if !requireAuth {
				return nil, nil
			}
			token := bearerToken(ctx.Get("Authorization"))
			if token == "" {
				return nil, errors.New("missing bearer token")
			}
			userID, ok := authTokens[token]
			if !ok {
				return nil, errors.New("invalid token")
			}
			return &sse.AuthResult{
				UserID: userID,
				Metadata: map[string]any{
					"auth_method": "bearer",
				},
			}, nil
		},

		// OnConnect: join rooms, validate auth, set metadata.
		OnConnect: func(ctx fiber.Ctx, client *sse.Client) error {
			room := ctx.Query("room")
			if room != "" {
				if err := hub.JoinGroup(client.ID, room); err != nil {
					return err
				}
			}

			appLogger.Info("sse client connected",
				"client_id", client.ID,
				"user_id", client.UserID,
				"room", room,
			)

			// Send a personalised welcome event immediately.
			welcome, _ := json.Marshal(map[string]any{
				"clientId": client.ID,
				"userId":   client.UserID,
				"message":  "Connected to SSE stream",
			})
			_ = hub.Send(client.ID, sse.NewEvent("connected", string(welcome)))
			return nil
		},

		OnDisconnect: func(client *sse.Client) {
			appLogger.Info("sse client disconnected",
				"client_id", client.ID,
				"user_id", client.UserID,
				"groups", client.Groups(),
			)
		},
	}
	if err := sse.ValidateHandlerOptions(handlerOpts); err != nil {
		log.Fatal(err)
	}
	app.Get("/events", sse.UserHubHandler(hub, handlerOpts))

	app.Get("/healthz", sse.HealthHandler())
	app.Get("/readyz", sse.ReadinessHandler(hub.Hub, func() error {
		if requireAuth && len(authTokens) == 0 {
			return errors.New("no auth tokens configured")
		}
		return nil
	}))

	// ── 4. Management / trigger endpoints (REST) ───────────────────────────
	managementAuth := requireAdminToken(adminToken)
	managementRate := newRequestRateLimiter(manageRateLimit, time.Minute)
	idempotency := newIdempotencyStore(5 * time.Minute)
	mgmt := app.Group("", managementAuth, managementRate.Middleware(), idempotency.Middleware())

	// POST /broadcast  body: {"type":"alert","data":"Hello everyone"}
	mgmt.Post("/broadcast", func(ctx fiber.Ctx) error {
		var body struct {
			Type   string `json:"type"`
			Data   string `json:"data"`
			Topic  string `json:"topic"`
			Sticky bool   `json:"sticky"`
		}
		if err := ctx.Bind().Body(&body); err != nil {
			return ctx.Status(400).JSON(fiber.Map{"error": err.Error()})
		}

		e := sse.NewEvent(body.Type, body.Data)
		if body.Sticky {
			e.WithTopic(body.Topic)
		}
		if err := validatePublishEvent(hub.Hub, e); err != nil {
			return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
		}
		hub.Broadcast(e)
		return ctx.JSON(fiber.Map{"ok": true, "clients": hub.Stats().ConnectedClients})
	})

	// POST /send/:clientId  body: {"type":"ping","data":"hi"}
	mgmt.Post("/send/:clientId", func(ctx fiber.Ctx) error {
		var body struct {
			Type string `json:"type"`
			Data string `json:"data"`
		}
		if err := ctx.Bind().Body(&body); err != nil {
			return ctx.Status(400).JSON(fiber.Map{"error": err.Error()})
		}

		e := sse.NewEvent(body.Type, body.Data)
		if err := validatePublishEvent(hub.Hub, e); err != nil {
			return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
		}
		if err := hub.Send(ctx.Params("clientId"), e); err != nil {
			return ctx.Status(404).JSON(fiber.Map{"error": err.Error()})
		}
		return ctx.JSON(fiber.Map{"ok": true})
	})

	// POST /user/:userId  body: {"type":"notification","data":"..."}
	mgmt.Post("/user/:userId", func(ctx fiber.Ctx) error {
		var body struct {
			Type string `json:"type"`
			Data string `json:"data"`
		}
		if err := ctx.Bind().Body(&body); err != nil {
			return ctx.Status(400).JSON(fiber.Map{"error": err.Error()})
		}

		e := sse.NewEvent(body.Type, body.Data)
		if err := validatePublishEvent(hub.Hub, e); err != nil {
			return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
		}
		hub.SendToUser(ctx.Params("userId"), e)
		return ctx.JSON(fiber.Map{
			"ok":          true,
			"connections": hub.ConnectionCount(ctx.Params("userId")),
		})
	})

	// POST /group/:name  body: {"type":"chat","data":"..."}
	mgmt.Post("/group/:name", func(ctx fiber.Ctx) error {
		var body struct {
			Type   string `json:"type"`
			Data   string `json:"data"`
			Topic  string `json:"topic"`
			Sticky bool   `json:"sticky"`
		}
		if err := ctx.Bind().Body(&body); err != nil {
			return ctx.Status(400).JSON(fiber.Map{"error": err.Error()})
		}

		e := sse.NewEvent(body.Type, body.Data)
		if body.Sticky {
			e.WithTopic(body.Topic)
		}
		if err := validatePublishEvent(hub.Hub, e); err != nil {
			return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
		}
		hub.SendToGroup(ctx.Params("name"), e)
		return ctx.JSON(fiber.Map{
			"ok":      true,
			"members": len(hub.GroupMembers(ctx.Params("name"))),
		})
	})

	// POST /group/:name/join/:clientId
	mgmt.Post("/group/:name/join/:clientId", func(ctx fiber.Ctx) error {
		if err := hub.JoinGroup(ctx.Params("clientId"), ctx.Params("name")); err != nil {
			return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
		}
		return ctx.JSON(fiber.Map{"ok": true})
	})

	// POST /group/:name/leave/:clientId
	mgmt.Post("/group/:name/leave/:clientId", func(ctx fiber.Ctx) error {
		hub.LeaveGroup(ctx.Params("clientId"), ctx.Params("name"))
		return ctx.JSON(fiber.Map{"ok": true})
	})

	// DELETE /sticky/:topic  — clear retained events for a topic
	mgmt.Delete("/sticky/:topic", func(ctx fiber.Ctx) error {
		hub.ClearSticky(ctx.Params("topic"))
		return ctx.JSON(fiber.Map{"ok": true})
	})

	// POST /admin/drain?timeoutSec=10
	mgmt.Post("/admin/drain", func(ctx fiber.Ctx) error {
		timeoutSec := parseIntOrDefault(ctx.Query("timeoutSec"), 10)
		go hub.Drain(time.Duration(timeoutSec) * time.Second)
		return ctx.JSON(fiber.Map{"ok": true, "draining": true, "timeoutSec": timeoutSec})
	})

	// POST /admin/disconnect/:clientId
	mgmt.Post("/admin/disconnect/:clientId", func(ctx fiber.Ctx) error {
		if err := hub.Disconnect(ctx.Params("clientId")); err != nil {
			return ctx.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
		}
		return ctx.JSON(fiber.Map{"ok": true})
	})

	// GET /stats
	app.Get("/stats", func(ctx fiber.Ctx) error {
		s := hub.Stats()
		return ctx.JSON(fiber.Map{
			"connectedClients": s.ConnectedClients,
			"groups":           s.Groups,
			"onlineUsers":      hub.OnlineUsers(),
		})
	})

	// GET /metrics/sse
	app.Get("/metrics/sse", managementAuth, func(ctx fiber.Ctx) error {
		hubStats := hub.Stats()
		handlerStats := handlerMetrics.Snapshot()
		return ctx.JSON(fiber.Map{
			"handler": handlerStats,
			"hub": fiber.Map{
				"connectedClients": hubStats.ConnectedClients,
				"groups":           hubStats.Groups,
				"onlineUsers":      hub.OnlineUsers(),
			},
			"limits": fiber.Map{
				"maxConnectionsPerIP": maxConnPerIP,
				"maxClients":          10_000,
				"maxEventBytes":       maxEventBytes,
			},
		})
	})

	// ── 5. Demo: push a sticky "status" event every 10 s ──────────────────
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for t := range ticker.C {
			payload, _ := json.Marshal(map[string]any{
				"status": "online",
				"time":   t.Format(time.RFC3339),
				"users":  len(hub.OnlineUsers()),
			})
			hub.Broadcast(
				sse.NewEvent("status", string(payload)).WithTopic("status"),
			)
		}
	}()

	// Graceful shutdown: drain existing connections before stopping the hub.
	go func() {
		sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		<-sigCtx.Done()

		appLogger.Info("shutdown signal received; draining hub")
		hub.Drain(10 * time.Second)
		if err := app.Shutdown(); err != nil {
			appLogger.Error("fiber shutdown failed", "error", err.Error())
		}
	}()

	appLogger.Info("sse server starting",
		"listen", listenAddr,
		"allowed_origins", allowedOrigins,
		"require_auth", requireAuth,
		"require_tls", requireTLS,
		"max_connections_per_ip", maxConnPerIP,
		"admin_token_configured", adminToken != "",
	)
	log.Fatal(app.Listen(listenAddr))
}

// splitComma splits a comma-separated string, trimming whitespace.
func splitComma(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			token := trim(s[start:i])
			if token != "" {
				out = append(out, token)
			}
			start = i + 1
		}
	}
	return out
}

func trim(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}

func parseCSVEnv(name, defaultValue string) []string {
	raw := envOrDefault(name, defaultValue)
	items := splitComma(raw)
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}

func parseTokenUserMapEnv(name, defaultValue string) map[string]string {
	raw := envOrDefault(name, defaultValue)
	entries := splitComma(raw)
	out := make(map[string]string, len(entries))
	for _, entry := range entries {
		parts := strings.SplitN(entry, ":", 2)
		if len(parts) != 2 {
			continue
		}
		token := trim(parts[0])
		user := trim(parts[1])
		if token != "" && user != "" {
			out[token] = user
		}
	}
	return out
}

func parseIntEnv(name string, defaultValue int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return defaultValue
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return defaultValue
	}
	return v
}

func parseIntOrDefault(raw string, fallback int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}

func parseBoolEnv(name string, defaultValue bool) bool {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	if raw == "" {
		return defaultValue
	}
	switch raw {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return defaultValue
	}
}

func envOrDefault(name, fallback string) string {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	return v
}

func bearerToken(headerValue string) string {
	if headerValue == "" {
		return ""
	}
	const prefix = "bearer "
	lower := strings.ToLower(headerValue)
	if !strings.HasPrefix(lower, prefix) {
		return ""
	}
	return strings.TrimSpace(headerValue[len(prefix):])
}

func requireAdminToken(token string) fiber.Handler {
	return func(ctx fiber.Ctx) error {
		if bearerToken(ctx.Get("Authorization")) != token {
			return ctx.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "unauthorized",
			})
		}
		return ctx.Next()
	}
}

func randomToken(size int) string {
	if size <= 0 {
		size = 24
	}
	raw := make([]byte, size)
	if _, err := rand.Read(raw); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return hex.EncodeToString(raw)
}

func validatePublishEvent(hub *sse.Hub, event *sse.Event) error {
	if event == nil {
		return sse.ErrInvalidEvent
	}
	if event.Type == "" {
		return errors.New("event type is required")
	}
	if hub != nil {
		return hub.ValidateEvent(event)
	}
	return nil
}

type requestRateLimiter struct {
	max    int
	window time.Duration
	mu     sync.Mutex
	hits   map[string]rateEntry
}

type rateEntry struct {
	count int
	from  time.Time
}

func newRequestRateLimiter(max int, window time.Duration) *requestRateLimiter {
	if window <= 0 {
		window = time.Minute
	}
	return &requestRateLimiter{
		max:    max,
		window: window,
		hits:   make(map[string]rateEntry),
	}
}

func (r *requestRateLimiter) Middleware() fiber.Handler {
	return func(ctx fiber.Ctx) error {
		if r.max <= 0 {
			return ctx.Next()
		}
		key := ctx.IP() + ":" + bearerToken(ctx.Get("Authorization"))
		now := time.Now()

		r.mu.Lock()
		entry := r.hits[key]
		if entry.from.IsZero() || now.Sub(entry.from) >= r.window {
			entry = rateEntry{count: 1, from: now}
			r.hits[key] = entry
			r.mu.Unlock()
			return ctx.Next()
		}
		if entry.count >= r.max {
			r.mu.Unlock()
			return ctx.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{"error": "management rate limit exceeded"})
		}
		entry.count++
		r.hits[key] = entry
		r.mu.Unlock()
		return ctx.Next()
	}
}

type idempotencyStore struct {
	ttl time.Duration
	mu  sync.Mutex
	ids map[string]time.Time
}

func newIdempotencyStore(ttl time.Duration) *idempotencyStore {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &idempotencyStore{
		ttl: ttl,
		ids: make(map[string]time.Time),
	}
}

func (s *idempotencyStore) Middleware() fiber.Handler {
	return func(ctx fiber.Ctx) error {
		key := strings.TrimSpace(ctx.Get("Idempotency-Key"))
		if key == "" {
			return ctx.Next()
		}
		now := time.Now()
		s.mu.Lock()
		for k, until := range s.ids {
			if now.After(until) {
				delete(s.ids, k)
			}
		}
		if until, exists := s.ids[key]; exists && now.Before(until) {
			s.mu.Unlock()
			return ctx.Status(fiber.StatusConflict).JSON(fiber.Map{"error": "duplicate idempotency key"})
		}
		s.ids[key] = now.Add(s.ttl)
		s.mu.Unlock()
		return ctx.Next()
	}
}
