# SSE Production Readiness TODO

## Verdict
- Current state: **production-ready candidate** for most deployments, with P0-P3 checklist items implemented in this repository.
- Reason: core correctness gaps were fixed and validated with unit, race, vet, benchmarks, and integration stress tests.

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
- [x] Add OpenTelemetry metrics bridge for hub/handler observability (`otel_metrics.go`).
- [x] Add distributed connection-limiting adapter via Redis (`redis_connection_limiter.go`, `HandlerOptions.ConnectionLimiter`).
- [x] Add optional cross-instance user presence store integration (`presence.go`, `redis_presence_store.go`, `NewUserHubWithPresence`).
- [x] Add fuzz/property tests for event encoding and identifier validation (`event_fuzz_test.go`, `validation_fuzz_test.go`).
