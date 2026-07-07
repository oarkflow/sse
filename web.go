package sse

import (
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

// AuthResult represents the outcome of request authentication.
type AuthResult struct {
	UserID   string
	Metadata map[string]any
}

// HandlerMetrics exposes runtime counters for SSE HTTP handlers.
type HandlerMetrics struct {
	TotalConnections    atomic.Uint64
	ActiveConnections   atomic.Int64
	RejectedConnections atomic.Uint64
	AuthFailures        atomic.Uint64
	OriginRejected      atomic.Uint64
	IPLimitRejected     atomic.Uint64
	ConnectRateRejected atomic.Uint64
	TopicLimitRejected  atomic.Uint64
	DrainingRejected    atomic.Uint64
	TLSRejected         atomic.Uint64
	StreamErrors        atomic.Uint64
}

// HandlerMetricsSnapshot is a copy of current metrics values.
type HandlerMetricsSnapshot struct {
	TotalConnections    uint64
	ActiveConnections   int64
	RejectedConnections uint64
	AuthFailures        uint64
	OriginRejected      uint64
	IPLimitRejected     uint64
	ConnectRateRejected uint64
	TopicLimitRejected  uint64
	DrainingRejected    uint64
	TLSRejected         uint64
	StreamErrors        uint64
}

// Snapshot returns a point-in-time copy of handler metrics.
func (m *HandlerMetrics) Snapshot() HandlerMetricsSnapshot {
	if m == nil {
		return HandlerMetricsSnapshot{}
	}
	return HandlerMetricsSnapshot{
		TotalConnections:    m.TotalConnections.Load(),
		ActiveConnections:   m.ActiveConnections.Load(),
		RejectedConnections: m.RejectedConnections.Load(),
		AuthFailures:        m.AuthFailures.Load(),
		OriginRejected:      m.OriginRejected.Load(),
		IPLimitRejected:     m.IPLimitRejected.Load(),
		ConnectRateRejected: m.ConnectRateRejected.Load(),
		TopicLimitRejected:  m.TopicLimitRejected.Load(),
		DrainingRejected:    m.DrainingRejected.Load(),
		TLSRejected:         m.TLSRejected.Load(),
		StreamErrors:        m.StreamErrors.Load(),
	}
}

// HandlerOptions configures the SSE HTTP handler.
type HandlerOptions struct {
	OnConnect    func(ctx fh.Ctx, client *Client) error
	OnDisconnect func(client *Client)

	ClientIDFromCtx func(ctx fh.Ctx) string
	UserIDFromCtx   func(ctx fh.Ctx) string
	TopicsFromCtx   func(ctx fh.Ctx) []string

	RequireAuth  bool
	Authenticate func(ctx fh.Ctx) (*AuthResult, error)

	AllowedOrigins   []string
	OriginValidator  func(origin string, ctx fh.Ctx) bool
	AllowCredentials bool

	MaxConnectionsPerIP int
	MaxConnectsPerIP    int
	ConnectRateWindow   time.Duration
	MaxTopicsPerClient  int
	RequireTLS          bool
	AllowLocalInsecure  bool

	// ConnectionLimiter optionally overrides in-process per-IP connection limiting.
	// If nil, the built-in local limiter is used.
	ConnectionLimiter ConnectionLimiter

	IPFromCtx func(ctx fh.Ctx) string

	Logger  *slog.Logger
	Metrics *HandlerMetrics

	// StreamErrorLogInterval rate-limits stream I/O error log lines.
	// Set to 0 to disable rate limiting.
	StreamErrorLogInterval time.Duration
}

// Handler returns an fh handler that upgrades the connection to SSE.
func Handler(hub *Hub, opts HandlerOptions) fh.Handler {
	return buildSSEHandler(hub.opts.ClientBufferSize, hub.AddClient, hub.RemoveClient, opts)
}

// HealthHandler returns a simple liveness endpoint.
func HealthHandler() fh.Handler {
	return func(ctx fh.Ctx) error {
		return ctx.JSON(fh.Map{"status": "ok"})
	}
}

// ReadinessHandler reports readiness based on hub state and optional checks.
func ReadinessHandler(hub *Hub, extraCheck func() error) fh.Handler {
	return func(ctx fh.Ctx) error {
		if hub == nil || hub.IsClosed() || hub.IsDraining() {
			return ctx.Status(fh.StatusServiceUnavailable).JSON(fh.Map{
				"ready": false,
				"hub":   "not-ready",
			})
		}
		if extraCheck != nil {
			if err := extraCheck(); err != nil {
				return ctx.Status(fh.StatusServiceUnavailable).JSON(fh.Map{
					"ready": false,
					"error": err.Error(),
				})
			}
		}
		return ctx.JSON(fh.Map{"ready": true})
	}
}

func buildSSEHandler(
	bufferSize int,
	addClient func(*Client) error,
	removeClient func(*Client),
	opts HandlerOptions,
) fh.Handler {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	allowedOrigins, wildcardOrigin := compileAllowedOrigins(opts.AllowedOrigins)
	var ipLimiter ConnectionLimiter
	if opts.ConnectionLimiter != nil {
		ipLimiter = opts.ConnectionLimiter
	} else {
		ipLimiter = newIPConnLimiter(opts.MaxConnectionsPerIP)
	}
	rateLimiter := newConnectRateLimiter(opts.MaxConnectsPerIP, opts.ConnectRateWindow)
	streamErrLimiter := newLogRateLimiter(opts.StreamErrorLogInterval)

	return func(ctx fh.Ctx) error {
		origin := strings.TrimSpace(ctx.Get("Origin"))
		clientIP := extractClientIP(ctx, opts)
		if opts.RequireTLS && !isTLSRequest(ctx, opts.AllowLocalInsecure) {
			incrementRejected(opts.Metrics)
			if opts.Metrics != nil {
				opts.Metrics.TLSRejected.Add(1)
			}
			return ctx.Status(fh.StatusUpgradeRequired).SendString(ErrTLSRequired.Error())
		}
		if ok := isOriginAllowed(origin, ctx, opts, allowedOrigins, wildcardOrigin); !ok {
			incrementRejected(opts.Metrics)
			if opts.Metrics != nil {
				opts.Metrics.OriginRejected.Add(1)
			}
			logger.Warn("sse connection rejected: origin not allowed", "origin", origin, "ip", clientIP)
			return ctx.Status(fh.StatusForbidden).SendString("origin not allowed")
		}

		if !rateLimiter.Allow(clientIP) {
			incrementRejected(opts.Metrics)
			if opts.Metrics != nil {
				opts.Metrics.ConnectRateRejected.Add(1)
			}
			logger.Warn("sse connection rejected: connect-rate limit reached", "ip", clientIP)
			return ctx.Status(fh.StatusTooManyRequests).SendString("connect rate limit exceeded")
		}

		if opts.RequireAuth && opts.Authenticate == nil {
			incrementRejected(opts.Metrics)
			logger.Error("sse handler misconfigured: RequireAuth=true without Authenticate hook")
			return ctx.Status(fh.StatusInternalServerError).SendString("sse auth misconfiguration")
		}

		clientOpts := ClientOptions{
			LastEventID: ctx.Get("Last-Event-ID"),
			BufferSize:  bufferSize,
		}
		if opts.ClientIDFromCtx != nil {
			clientOpts.ID = opts.ClientIDFromCtx(ctx)
		}
		if opts.UserIDFromCtx != nil {
			clientOpts.UserID = opts.UserIDFromCtx(ctx)
		}
		if opts.TopicsFromCtx != nil {
			clientOpts.Topics = opts.TopicsFromCtx(ctx)
		}
		if opts.MaxTopicsPerClient > 0 && len(clientOpts.Topics) > opts.MaxTopicsPerClient {
			incrementRejected(opts.Metrics)
			if opts.Metrics != nil {
				opts.Metrics.TopicLimitRejected.Add(1)
			}
			return ctx.Status(fh.StatusBadRequest).SendString("topic limit exceeded")
		}

		authResult, authErr := runAuth(ctx, opts)
		if authErr != nil {
			incrementRejected(opts.Metrics)
			if opts.Metrics != nil {
				opts.Metrics.AuthFailures.Add(1)
			}
			logger.Warn("sse connection rejected: authentication failed", "ip", clientIP, "error", authErr.Error())
			return ctx.Status(fh.StatusUnauthorized).SendString("unauthorized")
		}
		if authResult != nil {
			if authResult.UserID != "" {
				clientOpts.UserID = authResult.UserID
			}
			if len(authResult.Metadata) > 0 {
				clientOpts.Metadata = authResult.Metadata
			}
		}

		if !ipLimiter.Acquire(clientIP) {
			incrementRejected(opts.Metrics)
			if opts.Metrics != nil {
				opts.Metrics.IPLimitRejected.Add(1)
			}
			logger.Warn("sse connection rejected: per-ip limit reached", "ip", clientIP, "limit", opts.MaxConnectionsPerIP)
			return ctx.Status(fh.StatusTooManyRequests).SendString("too many active connections for this IP")
		}

		client := NewClient(clientOpts)
		if err := addClient(client); err != nil {
			ipLimiter.Release(clientIP)
			incrementRejected(opts.Metrics)
			if opts.Metrics != nil && errors.Is(err, ErrHubDraining) {
				opts.Metrics.DrainingRejected.Add(1)
			}
			status := fh.StatusServiceUnavailable
			if errors.Is(err, ErrMaxClientsReached) || errors.Is(err, ErrHubDraining) {
				status = fh.StatusTooManyRequests
			}
			if errors.Is(err, ErrInvalidTopic) {
				status = fh.StatusBadRequest
			}
			logger.Warn("sse connection rejected: unable to register client",
				"client_id", client.ID, "user_id", client.UserID, "ip", clientIP, "error", err.Error())
			return ctx.Status(status).SendString(err.Error())
		}

		if opts.OnConnect != nil {
			if err := opts.OnConnect(ctx, client); err != nil {
				removeClient(client)
				ipLimiter.Release(clientIP)
				incrementRejected(opts.Metrics)
				logger.Warn("sse connection rejected by OnConnect hook",
					"client_id", client.ID, "user_id", client.UserID, "ip", clientIP, "error", err.Error())
				return ctx.Status(fh.StatusForbidden).SendString(err.Error())
			}
		}

		if opts.Metrics != nil {
			opts.Metrics.TotalConnections.Add(1)
			opts.Metrics.ActiveConnections.Add(1)
		}

		logger.Info("sse client connected",
			"client_id", client.ID, "user_id", client.UserID, "ip", clientIP, "topics", len(client.topics))

		if origin != "" && (wildcardOrigin || len(allowedOrigins) > 0 || opts.OriginValidator != nil) {
			ctx.Set("Vary", "Origin")
			if wildcardOrigin && !opts.AllowCredentials {
				ctx.Set("Access-Control-Allow-Origin", "*")
			} else {
				ctx.Set("Access-Control-Allow-Origin", origin)
			}
			if opts.AllowCredentials {
				ctx.Set("Access-Control-Allow-Credentials", "true")
			}
		}

		ctx.Set("Content-Type", "text/event-stream; charset=utf-8")
		ctx.Set("Cache-Control", "no-cache, no-transform")
		ctx.Set("Connection", "keep-alive")
		ctx.Set("Transfer-Encoding", "chunked")
		ctx.Set("X-Accel-Buffering", "no")

		return ctx.Stream(func(w *fh.StreamWriter) error {
			defer func() {
				removeClient(client)
				ipLimiter.Release(clientIP)
				if opts.Metrics != nil {
					opts.Metrics.ActiveConnections.Add(-1)
				}
				if opts.OnDisconnect != nil {
					opts.OnDisconnect(client)
				}
				logger.Info("sse client disconnected", "client_id", client.ID, "user_id", client.UserID, "ip", clientIP)
			}()

			for event := range client.send {
				var err error
				if event.Type == "" && event.Data != "" {
					_, err = w.Write([]byte(event.Data))
				} else {
					_, err = w.Write(event.Encode())
				}
				if err != nil {
					recordStreamError(opts.Metrics, logger, streamErrLimiter, client.ID, err)
					return err
				}
				if err = w.Flush(); err != nil {
					recordStreamError(opts.Metrics, logger, streamErrLimiter, client.ID, err)
					return err
				}
			}
			return nil
		})
	}
}

func recordStreamError(metrics *HandlerMetrics, logger *slog.Logger, limiter *logRateLimiter, clientID string, err error) {
	if metrics != nil {
		metrics.StreamErrors.Add(1)
	}
	if limiter.Allow() {
		logger.Warn("sse stream io failed", "client_id", clientID, "error", err.Error())
	}
}

func runAuth(ctx fh.Ctx, opts HandlerOptions) (*AuthResult, error) {
	if opts.Authenticate == nil {
		if opts.RequireAuth {
			return nil, ErrUnauthorized
		}
		return nil, nil
	}
	result, err := opts.Authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if opts.RequireAuth && result == nil {
		return nil, ErrUnauthorized
	}
	return result, nil
}

func incrementRejected(metrics *HandlerMetrics) {
	if metrics != nil {
		metrics.RejectedConnections.Add(1)
	}
}

func extractClientIP(ctx fh.Ctx, opts HandlerOptions) string {
	if opts.IPFromCtx != nil {
		if ip := strings.TrimSpace(opts.IPFromCtx(ctx)); ip != "" {
			return ip
		}
	}
	if ip := strings.TrimSpace(ctx.IP()); ip != "" {
		return ip
	}
	return "unknown"
}

func compileAllowedOrigins(origins []string) (map[string]struct{}, bool) {
	allowed := make(map[string]struct{}, len(origins))
	wildcard := false
	for _, origin := range origins {
		normalized := normalizeOrigin(origin)
		if normalized == "" {
			continue
		}
		if normalized == "*" {
			wildcard = true
			continue
		}
		allowed[normalized] = struct{}{}
	}
	return allowed, wildcard
}

func isOriginAllowed(origin string, ctx fh.Ctx, opts HandlerOptions, allowedOrigins map[string]struct{}, wildcard bool) bool {
	policyEnabled := wildcard || len(allowedOrigins) > 0 || opts.OriginValidator != nil
	if !policyEnabled {
		return true
	}
	if origin == "" {
		return false
	}
	normalizedOrigin := normalizeOrigin(origin)
	if normalizedOrigin == "" {
		return false
	}
	if !wildcard {
		if _, ok := allowedOrigins[normalizedOrigin]; !ok && len(allowedOrigins) > 0 {
			return false
		}
	}
	if opts.OriginValidator != nil && !opts.OriginValidator(origin, ctx) {
		return false
	}
	return true
}

func normalizeOrigin(origin string) string {
	return strings.ToLower(strings.TrimSpace(origin))
}

func isTLSRequest(ctx fh.Ctx, allowLocalInsecure bool) bool {
	return isTLSRequestValues(ctx.Get("X-Forwarded-Proto"), ctx.Get("X-Forwarded-Ssl"), ctx.IP(), allowLocalInsecure)
}

func isTLSRequestValues(forwardedProto, forwardedSsl, ip string, allowLocalInsecure bool) bool {
	if strings.EqualFold(forwardedProto, "https") {
		return true
	}
	if strings.EqualFold(forwardedSsl, "on") {
		return true
	}
	if !allowLocalInsecure {
		return false
	}
	normalizedIP := strings.TrimSpace(ip)
	return normalizedIP == "127.0.0.1" || normalizedIP == "::1" || normalizedIP == "localhost"
}

type ipConnLimiter struct {
	max int

	mu     sync.Mutex
	counts map[string]int
}

func newIPConnLimiter(max int) *ipConnLimiter {
	return &ipConnLimiter{
		max:    max,
		counts: make(map[string]int),
	}
}

func (l *ipConnLimiter) Acquire(ip string) bool {
	if l.max <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	count := l.counts[ip]
	if count >= l.max {
		return false
	}
	l.counts[ip] = count + 1
	return true
}

func (l *ipConnLimiter) Release(ip string) {
	if l.max <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	count := l.counts[ip]
	if count <= 1 {
		delete(l.counts, ip)
		return
	}
	l.counts[ip] = count - 1
}

type connectRateLimiter struct {
	maxPerWindow int
	window       time.Duration

	mu      sync.Mutex
	entries map[string]connectWindow
	lastGC  time.Time
}

type connectWindow struct {
	start time.Time
	count int
}

func newConnectRateLimiter(maxPerWindow int, window time.Duration) *connectRateLimiter {
	if window <= 0 {
		window = time.Minute
	}
	return &connectRateLimiter{
		maxPerWindow: maxPerWindow,
		window:       window,
		entries:      make(map[string]connectWindow),
	}
}

// cleanup removes expired entries from the map.
func (l *connectRateLimiter) cleanup() {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	for ip, entry := range l.entries {
		if now.Sub(entry.start) >= l.window {
			delete(l.entries, ip)
		}
	}
}

// Stop is retained for backward compatibility.
func (l *connectRateLimiter) Stop() {
}

func (l *connectRateLimiter) Allow(ip string) bool {
	if l.maxPerWindow <= 0 {
		return true
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lastGC.IsZero() || now.Sub(l.lastGC) >= l.window {
		l.cleanupLocked(now)
		l.lastGC = now
	}

	entry := l.entries[ip]
	if entry.start.IsZero() || now.Sub(entry.start) >= l.window {
		l.entries[ip] = connectWindow{start: now, count: 1}
		return true
	}
	if entry.count >= l.maxPerWindow {
		return false
	}
	entry.count++
	l.entries[ip] = entry
	return true
}

func (l *connectRateLimiter) cleanupLocked(now time.Time) {
	for ip, entry := range l.entries {
		if now.Sub(entry.start) >= l.window {
			delete(l.entries, ip)
		}
	}
}
