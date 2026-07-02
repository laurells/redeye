# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- README: documented failover/split-brain tradeoffs in distributed mode — a new "Split-brain: what's prevented, and what isn't" section, plus three new numbered items in the reliability tradeoffs list covering partitioned-instance fail-open behavior, the rare `claimTrial`-error double-trial window, and inherited Redis replication/failover consistency.
- README: documented total coordination-layer outage behavior — a new "Total coordination-layer outage (the store itself is down)" section distinguishing fleet-wide store unavailability from the single-partitioned-instance case, covering fail-open/fail-closed at fleet scale, the lack of buffering/replay or store back-off, and that store HA (Sentinel/Cluster) is the consumer's responsibility.

## [0.1.1] - 2026-07-02

### Added

- CI: lint, unit tests, and build across Node 18/20/22, plus a separate job running the integration suite against a real Redis service container, on every push and pull request.
- `CHANGELOG.md` and `CONTRIBUTING.md`.
- CI / npm version / license badges in the README.

## [0.1.0] - 2026-07-02

Initial release.

### Added

- `CircuitBreaker` with two tripping strategies:
  - `consecutive` (default): trips after `failureThreshold` consecutive failures.
  - `errorRate`: trips on an EWMA-smoothed failure rate once `minimumCalls` samples are seen, catching flapping/degrading dependencies that `consecutive` structurally can't.
- Local mode (default, zero dependencies): per-process in-memory breaker state.
- Distributed mode via a pluggable `Store` interface: breaker state shared across instances through Redis (or any store you implement).
- `RedisStore` (`redeye-breaker/redis-store`): atomic `recordFailureAtomic` / `recordOutcomeAtomic` via Lua scripts, and `claimTrial` via `SET ... NX`, so distributed mode gets exact failure counting and real single-trial half-open recovery instead of best-effort approximations.
- Exponential backoff with jitter on repeated failed half-open trials (`backoffMultiplier`, `maxResetTimeout`, `jitter`).
- `failOpenOnStoreError` option to control fail-open vs. fail-closed behavior when the store itself is unreachable, plus `StoreUnavailableError` to distinguish that condition from a downstream failure.
- Per-operation call/success/failure/rejection/store-error metrics via `getMetrics` / `getAllMetrics`.
- `onStateChange` and `onStoreError` hooks for observability.
- Optional per-call `timeout` and `trialTimeout`, plus a local-mode monitor that force-releases a stale half-open trial claim if a call never settles.
- Unit test suite (in-memory fakes) and an integration test suite that runs against real Redis via `docker compose up -d && npm run test:integration`.

[Unreleased]: https://github.com/laurells/redeye/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/laurells/redeye/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/laurells/redeye/releases/tag/v0.1.0
