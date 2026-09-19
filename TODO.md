# SSE Production Readiness TODO

## Verdict
- Current state: **production-ready candidate** for both single-instance and multi-instance (horizontally scaled) deployments.
- Reason: core correctness gaps were fixed and validated with unit, race, vet, benchmarks, and integration stress tests; `Bus`/`ReplayStore`/`PresenceStore`/`ConnectionLimiter` all ship real Redis-backed implementations in addition to the pluggable interfaces, and `HubOptions.Bus` wires distributed pub/sub directly into `Broadcast`/`Send`/`SendToGroup`/`BroadcastExcept`/`SendToUser`.

## P0 - Must Fix Before Production
- [x] Prevent duplicate client ID overwrite in `AddClient`.
  - File: `hub.go`
  - Problem: registering an existing `client.ID` silently overwrites map entry and can orphan the previous connection/channel.
  - Expected: reject duplicate IDs or explicitly disconnect old client before replacement.
- [x] Fix client group membership race/read without lock.
  - File: `hub.go`
  - Problem: `JoinGroup` reads `c.groups[group]` outside `groupsMu` lock.
  - Expected: keep both `len(c.groups)` and membership check inside the same lock scope.
- [x] Validate client existence in `JoinGroup`.
  - File: `hub.go`
  - Problem: non-existent client IDs can be inserted into `h.groups`, creating ghost members.
  - Expected: return `ErrClientNotFound` when `clientID` is not connected.
- [x] Remove time-based replay synchronization (`time.Sleep`) in replay path.
  - File: `hub.go`
  - Problem: `replayFromStore`/`replaySticky` rely on fixed sleeps (25ms/50ms), which is nondeterministic under load and adds avoidable latency.
  - Expected: deterministic registration/replay sequencing (e.g., replay inside hub event loop or via explicit registration ack).
- [x] Add explicit tests for the above failure modes.
  - Files: `hub_test.go`, `web_test.go`
  - Expected: regression tests for duplicate IDs, ghost group members, lock-safe group logic, deterministic replay ordering.

## P1 - Reliability and Operability Enhancements
- [x] Add first-class metrics exporter integration (Prometheus/OpenTelemetry).
  - Files: new docs/examples + optional helper package.
  - Include connection gauges, delivery/drop counters, replay stats, auth/origin/IP rejections, stream errors.
- [x] Add structured lifecycle hooks for shutdown/drain observability.
  - File: `hub.go`
  - Emit timing and reason codes for drain completion vs timeout.
- [x] Improve backpressure controls.
  - File: `hub.go`
  - Add optional per-client drop thresholds and auto-disconnect policy after sustained drops.
- [x] Add configurable slow-consumer logging rate limit.
  - Files: `web.go`, `hub.go`
  - Prevent log storms during incident spikes.
- [x] Support distributed replay/publish patterns.
  - Current default replay store is in-memory only.
  - Add reference adapter(s) (Redis/Kafka/NATS) for multi-instance deployments.

## P2 - Security and API Hardening
- [x] Add stricter input validation for topic/group identifiers.
  - Files: `hub.go`, handler usage in examples.
  - Define length/charset constraints to prevent abuse and cardinality explosions.
- [x] Document and test CORS/auth/TLS production profiles.
  - Files: `web_test.go`, docs.
  - Cover reverse-proxy TLS headers and strict origin handling.
- [x] Add optional publish authorization hooks.
  - New hook in hub or wrapper layer for per-topic/group publish ACL checks.

## P3 - Developer Experience and Maintenance
- [x] Add benchmark suite.
  - Files: `*_test.go` benchmarks.
  - Include broadcast throughput, per-client latency, replay cost, memory growth under sticky+replay.
- [x] Add stress/integration test harness.
  - Simulate reconnect storms, burst traffic, and mixed slow/fast consumers.
- [x] Add README production checklist.
  - Include recommended `HubOptions` and `HandlerOptions` defaults, LB/proxy settings, drain strategy, and monitoring alerts.

## Suggested Implementation Order
1. Fix P0 correctness issues (`AddClient`, `JoinGroup`, replay sequencing).
2. Add regression tests that fail before fixes and pass after.
3. Add observability + benchmark baseline.
4. Add distributed replay/publish adapter guidance for multi-instance rollout.

## Additional Hardening (In Progress)
- [x] Avoid replay goroutine panics when clients disconnect during replay (`hub.go` replay paths now use safe send).
- [x] Remove per-handler background goroutine from connect-rate limiter and switch to lazy cleanup in `Allow` (`web.go`).
- [x] Add OpenTelemetry metrics bridge for hub/handler observability (`contrib/otel/metrics.go`).
- [x] Add distributed connection-limiting adapter via Redis (`contrib/redis/connection_limiter.go`, `HandlerOptions.ConnectionLimiter`).
- [x] Add optional cross-instance user presence store integration (`presence.go`, `contrib/redis/presence.go`, `NewUserHubWithPresence`).
- [x] Add fuzz/property tests for event encoding and identifier validation (`event_fuzz_test.go`, `validation_fuzz_test.go`).

## Distributed Pub/Sub and Storage (Completed)

Previously this section listed items as done that were not actually present
in the repository (an earlier commit deleted `redis_bus.go`,
`redis_connection_limiter.go`, `redis_presence_store.go`, `redis_store.go`,
and `otel_metrics.go` during a migration to the `fh` HTTP framework, but the
checklist above was never corrected). They have now been reimplemented and
wired in for real:

- [x] `Bus` interface (`bus.go`) is now consulted by `Hub.Broadcast` / `Send`
  / `SendToGroup` / `BroadcastExcept` and `UserHub.SendToUser` via
  `HubOptions.Bus` — calling these methods fans out across instances
  automatically instead of requiring manual `bus.Publish` calls.
- [x] `Dispatcher` interface (`hub_features.go`) + `Hub.DispatchRemote` /
  `UserHub.DispatchRemote` decouple "deliver locally" from "publish
  remotely" so a Bus subscriber never re-publishes what it just received
  (no fanout loops).
- [x] `PublishTargetUser` + `PublishContext.UserID` let `UserHub.SendToUser`
  reach a user's connections on *any* instance, not just the local one.
- [x] `RedisBus` (`contrib/redis/bus.go`) — reference distributed pub/sub
  adapter over Redis Pub/Sub, in its own `contrib/redis` Go module so the
  core module doesn't depend on `go-redis`.
- [x] `RedisReplayStore` (`contrib/redis/replay_store.go`) — shared, TTL'd
  Last-Event-ID replay storage across instances and restarts.
- [x] `RedisPresenceStore` (`contrib/redis/presence.go`) — cross-instance
  online-user tracking.
- [x] `RedisConnectionLimiter` (`contrib/redis/connection_limiter.go`) —
  distributed per-key (e.g. per-IP) concurrent connection cap via a Lua
  script.
- [x] `RegisterOTelMetrics` (`contrib/otel/metrics.go`) — OpenTelemetry
  observable instruments mirroring `Hub.Stats()` / `HandlerMetrics.Snapshot()`,
  including new `BusPublishedEvents` / `BusPublishErrors` /
  `BusDispatchedEvents` counters (also exposed via `MetricsText`), in its own
  `contrib/otel` Go module so the core module doesn't depend on the
  OpenTelemetry SDK.
- [x] Tests: `bus_test.go` (in-process fanout across two Hubs/UserHubs via a
  fake Bus, in the core module) and `contrib/redis/redis_test.go` (all four
  Redis adapters against `miniredis`, no real Redis server required).
- [x] `go.work` at the repo root wires the core module and both `contrib`
  modules together for local development; each `contrib` module also
  `replace`s `github.com/oarkflow/sse => ../..` so it builds standalone.

### Known remaining gaps for multi-instance deployments
- Redis Pub/Sub (`RedisBus`) is at-most-once and unordered across a
  reconnect of the Redis client itself. For at-least-once delivery, use
  `contrib/nats`'s `NatsBus` instead (JetStream-backed, durable consumer per
  instance, explicit ack/redelivery — see below).
- `RedisReplayStore` and `RedisPresenceStore` scale to one Redis
  keyspace; for very high event volume, shard by topic/prefix.

## Durable Bus, License, CI (Completed)

- [x] `NatsBus` (`contrib/nats/bus.go`) — a second reference `Bus`
  implementation over NATS JetStream, for deployments that need at-least-once
  cross-instance fanout instead of RedisBus's at-most-once Pub/Sub. Each
  instance gets its own durable JetStream consumer (keyed by
  `NatsBusOptions.InstanceID`); explicit ack after successful
  `Dispatcher.DispatchRemote`, so a message a consumer received but crashed
  before processing is redelivered rather than lost. Its own Go module
  (`contrib/nats`), same pattern as `contrib/redis`/`contrib/otel`.
- [x] Tests (`contrib/nats/bus_test.go`, embedded `nats-server` +
  JetStream, no external server required): broadcast fanout across two
  instances, redelivery of a naked (crashed-before-ack) message through the
  same durable consumer, and an explicit pin on `DeliverNewPolicy`'s
  boundary (a brand-new durable consumer doesn't replay the stream's
  pre-existing backlog — `ReplayStore` already owns late-joiner catch-up).
- [x] `LICENSE` (MIT) added — README already referenced it but the file was
  missing.
- [x] `.github/workflows/ci.yml` — builds/vets/race-tests all four modules
  (root, `contrib/redis`, `contrib/otel`, `contrib/nats`) via a matrix, plus
  a `gofmt -l` check, on push/PR to `main`.
- [x] `go.work` updated to include `contrib/nats`.
