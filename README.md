# SSE - Server-Sent Events for GoFiber

[![Go Reference](https://pkg.go.dev/badge/github.com/oarkflow/sse.svg)](https://pkg.go.dev/github.com/oarkflow/sse)
[![Go Report Card](https://goreportcard.com/badge/github.com/oarkflow/sse)](https://goreportcard.com/report/github.com/oarkflow/sse)

A robust, production-ready Server-Sent Events (SSE) implementation for the [GoFiber](https://gofiber.io/) framework. Features include sticky events, client groups, user-level targeting, backpressure handling, and comprehensive security controls.

## Features

- **Full SSE Protocol Support** - Proper wire format encoding with multi-line data, event types, and retry hints
- **Sticky Events** - Late-joining clients automatically receive recent events per topic
- **Last-Event-ID Replay** - Reconnecting clients resume from where they left off
- **Client Groups** - Broadcast to rooms/channels with group membership
- **User-Level Targeting** - Send events to all devices/tabs of a specific user
- **Backpressure Policies** - Handle slow consumers with configurable strategies
- **Security Controls** - Authentication hooks, CORS, rate limiting, TLS enforcement
- **Graceful Shutdown** - Drain mode for zero-downtime deployments
- **Observability** - Built-in metrics, health endpoints, structured logging

## Installation

```bash
go get github.com/oarkflow/sse
```

The core module depends only on [`fh`](https://github.com/oarkflow/fh); it
pulls in no Redis, NATS, or OpenTelemetry client. Distributed-deployment
adapters live in separate `contrib` modules with their own `go.mod` (see
[`contrib/redis`](contrib/redis), [`contrib/nats`](contrib/nats), and
[`contrib/otel`](contrib/otel)), so you only pay for the dependencies you
actually use:

```bash
go get github.com/oarkflow/sse/contrib/redis   # RedisBus, RedisReplayStore, RedisPresenceStore, RedisConnectionLimiter
go get github.com/oarkflow/sse/contrib/nats    # NatsBus (durable, at-least-once fanout via JetStream)
go get github.com/oarkflow/sse/contrib/otel    # RegisterOTelMetrics
```

## Quick Start

### Server Setup

```go
package main

import (
    "time"
    "github.com/gofiber/fiber/v3"
    "github.com/oarkflow/sse"
)

func main() {
    // Create the SSE hub
    hub := sse.NewHub(sse.HubOptions{
        StickySize:         20,                // Retain last 20 events per topic
        ClientBufferSize:   64,                // Events per client buffer
        HeartbeatInterval:  30 * time.Second,  // Keep connections alive
        MaxClients:         10_000,            // Maximum concurrent connections
        BackpressurePolicy: sse.BackpressureDropNewest,
    })
    defer hub.Stop()

    // Create Fiber app
    app := fiber.New()

    // SSE endpoint
    app.Get("/events", sse.Handler(hub, sse.HandlerOptions{
        AllowedOrigins: []string{"http://localhost:3000"},
    }))

    // Health endpoints
    app.Get("/healthz", sse.HealthHandler())
    app.Get("/readyz", sse.ReadinessHandler(hub, nil))

    // Broadcast endpoint
    app.Post("/broadcast", func(ctx fiber.Ctx) error {
        hub.Broadcast(sse.NewEvent("message", string(ctx.Body())))
        return ctx.JSON(fiber.Map{"ok": true})
    })

    app.Listen(":3000")
}
```

### Client Usage (JavaScript)

```javascript
// Basic EventSource
const es = new EventSource('http://localhost:3000/events');

es.addEventListener('message', (event) => {
    console.log('Received:', event.data);
});

es.addEventListener('message', (event) => {
    console.log('Last Event ID:', event.lastEventId);
});

// Using the resilient client (from examples)
import { ResilientSSEClient } from './resilient-sse-client.js';

const client = new ResilientSSEClient('http://localhost:3000/events', {
    tokenProvider: () => localStorage.getItem('auth_token'),
    onMessage: (event) => console.log('Message:', event.data),
    onState: (state) => console.log('State:', state),
});
client.start();
```

## Core Concepts

### Hub

The `Hub` manages all connected SSE clients and provides event delivery methods:

```go
hub := sse.NewHub(sse.HubOptions{
    StickySize:           20,   // Events retained per topic for replay
    ClientBufferSize:     64,   // Per-client channel buffer size
    HeartbeatInterval:    30 * time.Second,
    MaxClients:           0,    // 0 = unlimited
    ReplayBufferSize:     1000, // In-memory replay store capacity
    ReplayLimitPerConnect: 500, // Max events replayed on reconnect
    BackpressurePolicy:   sse.BackpressureDropNewest,
    MaxGroupsPerClient:   100,  // Max group memberships per client
    MaxEventBytes:        64 * 1024, // 64 KiB max event size
    MaxEventTypeLength:   128,  // Max event type string length
    MaxTopicLength:       128,  // Topic identifier length cap
    MaxGroupLength:       128,  // Group identifier length cap
    MaxDropsPerClient:    200,  // Auto-disconnect after consecutive drops
    SlowConsumerLogInterval: 5 * time.Second, // Rate-limit slow consumer logs

    // Optional per-publish authorization checks
    AuthorizePublish: func(ctx sse.PublishContext, event *sse.Event) error {
        return nil
    },

    // Optional lifecycle hook for drain/stop observability
    OnLifecycle: func(evt sse.HubLifecycleEvent) {
        slog.Info("hub lifecycle", "type", evt.Type, "reason", evt.Reason)
    },
})
```

### Event Delivery Methods

```go
// Broadcast to all connected clients
hub.Broadcast(sse.NewEvent("alert", "System maintenance in 5 minutes"))

// Send to a specific client
hub.Send("client-123", sse.NewEvent("notification", "You have a new message"))

// Send to a group (room)
hub.SendToGroup("chat-room-1", sse.NewEvent("chat", "Hello room!"))

// Broadcast except specific clients
hub.BroadcastExcept(sse.NewEvent("update", "..."), "client-123", "client-456")
```

### Sticky Events

Sticky events are retained and replayed to clients who connect later:

```go
// This event will be replayed to new subscribers of "status" topic
hub.Broadcast(
    sse.NewEvent("status", `{"online": 150}`).WithTopic("status"),
)
```

Clients subscribe to topics via query parameter:

```
GET /events?topics=status,notifications
```

### Groups (Rooms)

Organize clients into groups for targeted broadcasting:

```go
// Add client to a group
hub.JoinGroup("client-123", "room-abc")

// Remove from group
hub.LeaveGroup("client-123", "room-abc")

// Send to group
hub.SendToGroup("room-abc", sse.NewEvent("chat", "Hello!"))

// Get group members
members := hub.GroupMembers("room-abc")
```

### UserHub - Multi-Device Support

`UserHub` extends `Hub` with user-level targeting for multi-device scenarios:

```go
userHub := sse.NewUserHub(sse.HubOptions{...})

// Send to all devices/tabs of a user
userHub.SendToUser("user-alice", sse.NewEvent("notification", "New email!"))

// Check if user is online
if userHub.IsOnline("user-alice") {
    // User has at least one active connection
}

// Get connection count
count := userHub.ConnectionCount("user-alice")

// Get all online users
onlineUsers := userHub.OnlineUsers()
```

## HTTP Handler Configuration

```go
handlerOpts := sse.HandlerOptions{
    // Lifecycle hooks
    OnConnect: func(ctx fiber.Ctx, client *sse.Client) error {
        // Called after client is registered
        // Return error to reject connection
        return nil
    },
    OnDisconnect: func(client *sse.Client) {
        // Called when client disconnects
    },

    // Client identification
    ClientIDFromCtx: func(ctx fiber.Ctx) string {
        return ctx.Query("clientId")
    },
    UserIDFromCtx: func(ctx fiber.Ctx) string {
        return ctx.Locals("userId").(string)
    },
    TopicsFromCtx: func(ctx fiber.Ctx) []string {
        // Parse topics from query
        return []string{"notifications", "updates"}
    },

    // Authentication
    RequireAuth: true,
    Authenticate: func(ctx fiber.Ctx) (*sse.AuthResult, error) {
        token := ctx.Get("Authorization")
        // Validate token...
        return &sse.AuthResult{
            UserID: "user-123",
            Metadata: map[string]any{"role": "admin"},
        }, nil
    },

    // CORS
    AllowedOrigins: []string{"https://app.example.com"},
    AllowCredentials: true,

    // Rate limiting
    MaxConnectionsPerIP: 10,  // Max concurrent connections per IP
    MaxConnectsPerIP:    30,  // Max new connections per minute per IP
    ConnectRateWindow:   time.Minute,
    MaxTopicsPerClient:  50,

    // Security
    RequireTLS: true,
    AllowLocalInsecure: true, // Allow HTTP for localhost

    // Observability
    Logger: slog.Default(),
    Metrics: &sse.HandlerMetrics{},
    StreamErrorLogInterval: 5 * time.Second,
}

app.Get("/events", sse.Handler(hub, handlerOpts))
```

## Backpressure Policies

When a client's buffer is full, the hub can respond in different ways:

```go
// Drop the incoming event (default)
BackpressurePolicy: sse.BackpressureDropNewest

// Drop the oldest queued event to make room
BackpressurePolicy: sse.BackpressureDropOldest

// Disconnect the slow consumer
BackpressurePolicy: sse.BackpressureDisconnectSlow
```

## Graceful Shutdown

```go
// Start drain mode (rejects new connections, keeps existing)
hub.StartDraining()

// Drain with timeout, then stop
hub.Drain(10 * time.Second)

// Immediate stop
hub.Stop()
```

Use with signal handling:

```go
go func() {
    sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()
    <-sigCtx.Done()

    slog.Info("Shutdown signal received, draining connections")
    hub.Drain(10 * time.Second)
    app.Shutdown()
}()
```

## Metrics and Monitoring

### Hub Stats

```go
stats := hub.Stats()
// stats.ConnectedClients - Current connections
// stats.Groups - Map of group name to member count
// stats.Draining - Is hub in drain mode
// stats.Closed - Is hub stopped
// stats.DeliveredEvents - Total events delivered
// stats.DroppedEvents - Events dropped due to backpressure
// stats.SlowConsumerDrops - Clients disconnected for slow consumption
// stats.ReplayedEvents - Events replayed on reconnect
```

### Handler Metrics

```go
metrics := &sse.HandlerMetrics{}
// Use in HandlerOptions.Metrics

snapshot := metrics.Snapshot()
// snapshot.TotalConnections - All-time connections
// snapshot.ActiveConnections - Current active
// snapshot.RejectedConnections - Rejected for various reasons
// snapshot.AuthFailures - Authentication failures
// snapshot.StreamErrors - Write errors
```

### Prometheus-Compatible Metrics Endpoint

```go
handlerMetrics := &sse.HandlerMetrics{}
app.Get("/metrics", sse.MetricsHandler(hub, handlerMetrics))
```

### OpenTelemetry Metrics

Requires the `contrib/otel` module (`go get github.com/oarkflow/sse/contrib/otel`):

```go
import sseotel "github.com/oarkflow/sse/contrib/otel"

meter := otel.Meter("your-service")
unregister, err := sseotel.RegisterOTelMetrics(meter, hub, handlerMetrics)
if err != nil {
    panic(err)
}
defer unregister()
```

### Health Endpoints

```go
// Liveness - Is the server running?
app.Get("/healthz", sse.HealthHandler())

// Readiness - Can the server handle requests?
app.Get("/readyz", sse.ReadinessHandler(hub, func() error {
    // Optional custom check
    return checkDatabaseConnection()
}))
```

## Custom Replay Store

`ReplayStore` is a two-method interface — implement it yourself for any
persistent or distributed backend, or use the reference Redis
implementation shipped in `contrib/redis` (`go get github.com/oarkflow/sse/contrib/redis`):

```go
import sseredis "github.com/oarkflow/sse/contrib/redis"

redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
store := sseredis.NewRedisReplayStore(redisClient, sseredis.RedisReplayStoreOptions{
    Prefix:    "sse:replay:",
    MaxEvents: 10000,
    TTL:       24 * time.Hour,
})

hub := sse.NewHub(sse.HubOptions{
    ReplayStore: store,
})
```

## Distributed Publish Fanout (Bus)

Wire a `Bus` into `HubOptions.Bus` and every `Broadcast` / `Send` /
`SendToGroup` / `BroadcastExcept` (and `UserHub.SendToUser`) call
automatically fans out to every other instance subscribed to the same
channel — no need to call `bus.Publish` yourself. Two reference `Bus`
implementations ship as separate `contrib` modules, with the same trade-off
you'd expect from their transports:

| | `contrib/redis`'s `RedisBus` | `contrib/nats`'s `NatsBus` |
| --- | --- | --- |
| Transport | Redis Pub/Sub | NATS JetStream |
| Delivery guarantee | At-most-once — a message published while a subscriber is disconnected, or lost mid-flight, is gone | At-least-once — messages persist in a stream; each instance's durable consumer explicitly acks, and unacked messages are redelivered |
| Setup cost | Lower — no stream/consumer config | Slightly higher — needs a JetStream-enabled NATS server |
| When to reach for it | You're fine with occasional dropped fanout messages (typical for SSE, since clients reconnect and replay via `ReplayStore` anyway) and want the simplest option | You need every instance to reliably see every published event even across brief network blips |

### Redis Pub/Sub

```go
import sseredis "github.com/oarkflow/sse/contrib/redis"

redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
bus := sseredis.NewRedisBus(redisClient, sseredis.RedisBusOptions{
    Channel: "sse:bus",
    Logger:  slog.Default(),
})

hub := sse.NewHub(sse.HubOptions{
    Bus: bus, // Broadcast/Send/SendToGroup/BroadcastExcept now fan out via Redis
})

// Subscribe in background on each node so remote publishes reach this
// instance's local clients too.
go func() {
    _ = bus.Subscribe(context.Background(), hub) // pass a *UserHub here to also cover SendToUser
}()

// A plain call now reaches clients on every instance, not just this one.
hub.Broadcast(sse.NewEvent("notice", "multi-node fanout"))
```

### NATS JetStream (durable, at-least-once)

```go
import ssenats "github.com/oarkflow/sse/contrib/nats"

nc, _ := nats.Connect("nats://localhost:4222")
bus, err := ssenats.NewNatsBus(context.Background(), nc, ssenats.NatsBusOptions{
    Subject:    "sse.bus",
    InstanceID: "instance-1", // stable per instance so restarts resume, not reset
})
if err != nil {
    panic(err)
}

hub := sse.NewHub(sse.HubOptions{Bus: bus})

go func() {
    _ = bus.Subscribe(context.Background(), hub)
}()

hub.Broadcast(sse.NewEvent("notice", "multi-node fanout, redelivered on crash"))
```

## Distributed Connection Limits and Presence (Redis)

```go
import sseredis "github.com/oarkflow/sse/contrib/redis"

redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

connLimiter := sseredis.NewRedisConnectionLimiter(redisClient, sseredis.RedisConnectionLimiterOptions{
    Prefix: "sse:conn:",
    Max:    50,              // max active connections per IP
    TTL:    time.Minute,
})

presence := sseredis.NewRedisPresenceStore(redisClient, sseredis.RedisPresenceStoreOptions{
    Prefix: "sse:presence:",
    TTL:    10 * time.Minute,
})

hub := sse.NewUserHubWithPresence(sse.HubOptions{}, presence)
app.Get("/events", sse.UserHubHandler(hub, sse.HandlerOptions{
    ConnectionLimiter: connLimiter,
}))
```

## Event Structure

```go
type Event struct {
    ID        string    // Unique event ID for replay
    Type      string    // Event type (default: "message")
    Data      string    // Event payload
    Retry     int       // Reconnection hint (milliseconds)
    Topic     string    // Topic for sticky events (not sent over wire)
    Sticky    bool      // Retain for late joiners
    CreatedAt time.Time // Timestamp
}

// Create event
e := sse.NewEvent("notification", `{"message": "Hello"}`)

// Make sticky with topic
e.WithTopic("notifications")

// Set retry hint
e.WithRetry(3000) // 3 seconds

// Encode to SSE format
data := e.Encode()
// Output:
// id: uuid
// event: notification
// data: {"message": "Hello"}
// retry: 3000
//
```

## Error Handling

```go
var (
    ErrClientNotFound          // Client doesn't exist
    ErrMaxClientsReached       // Hub at capacity
    ErrHubClosed               // Hub has been stopped
    ErrHubDraining             // Hub in drain mode
    ErrUnauthorized            // Authentication failed
    ErrClientGroupLimitExceeded // Too many group memberships
    ErrInvalidEvent            // Malformed event
    ErrEventTooLarge           // Event exceeds size limit
    ErrTLSRequired             // TLS required but not used
)
```

## Examples

See the [`examples/`](examples/) directory for complete, ready-to-run implementations:

### Simple Example
- [`examples/simple-server/`](examples/simple-server/main.go) - Minimal server implementation
- [`examples/simple-client/`](examples/simple-client/index.html) - Basic HTML client with UI

### Advanced Example
- [`examples/server/`](examples/server/main.go) - Full-featured server with authentication, rate limiting, and management endpoints
- [`examples/client/`](examples/client/main.go) - Static file server for the demo client
- [`examples/client/views/`](examples/client/views/index.html) - Interactive demo UI with all features
- [`examples/client/views/resilient-sse-client.js`](examples/client/views/resilient-sse-client.js) - Reconnection logic with exponential backoff

### Running Examples

#### Simple Example:
```bash
# Start server (terminal 1)
cd examples/simple-server
go run main.go

# Open client (terminal 2 or browser)
open examples/simple-client/index.html
```

#### Advanced Example:
```bash
# Start server (terminal 1)
cd examples/server
go run main.go

# Start client server (terminal 2)
cd examples/client
go run main.go

# Open browser to http://localhost:8090
```

## Testing

```bash
# Run tests
go test ./...

# Run with coverage
go test -cover ./...

# Run benchmarks
go test -bench=. ./...
```

## Production Considerations

### Horizontal Scaling

The default in-memory implementations don't support multi-instance deployments. For horizontal scaling:

1. Use sticky sessions (layer 4 load balancing)
2. Use `sseredis.RedisReplayStore` (`contrib/redis`) for replay persistence
3. Wire a `Bus` into `HubOptions.Bus` for cross-instance publish fanout: `sseredis.RedisBus` (`contrib/redis`) for the simplest setup, or `ssenats.NatsBus` (`contrib/nats`) if you need at-least-once delivery across brief instance/network blips

### Memory Management

- Set appropriate `MaxClients` to limit connections
- Use `MaxEventBytes` to prevent oversized events
- Configure `ClientBufferSize` based on expected throughput
- Monitor `DroppedEvents` and `SlowConsumerDrops` metrics

### Security

- Enable `RequireTLS` in production
- Implement proper authentication in `Authenticate`
- Configure `AllowedOrigins` to prevent CSRF
- Set `MaxConnectionsPerIP` to prevent abuse

### Production Checklist

- [ ] `RequireTLS=true` in production and trusted reverse-proxy TLS headers configured.
- [ ] Explicit `AllowedOrigins` allowlist; avoid wildcard with credentials.
- [ ] `RequireAuth=true` with token/session validation in `Authenticate`.
- [ ] `MaxConnectionsPerIP`, `MaxConnectsPerIP`, and `ConnectRateWindow` tuned.
- [ ] `MaxEventBytes`, `MaxTopicLength`, `MaxGroupLength` set to bounded values.
- [ ] `BackpressurePolicy` selected and `MaxDropsPerClient` set for abusive/slow clients.
- [ ] `OnLifecycle` hook wired to logs/metrics for drain and stop visibility.
- [ ] `/healthz`, `/readyz`, and `/metrics` endpoints integrated with your platform.
- [ ] Replay and fanout strategy selected for multi-instance deployments (`contrib/redis`'s `RedisReplayStore` + `RedisBus`, or `contrib/nats`'s `NatsBus` for at-least-once fanout).

## API Reference

Full API documentation is available at [pkg.go.dev](https://pkg.go.dev/github.com/oarkflow/sse).

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please read the contributing guidelines before submitting PRs.
