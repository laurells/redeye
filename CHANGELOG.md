# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1] - 2026-07-08

### Changed

- **Distributed mode, `strategy: 'consecutive'`: a successful call against an already-clean breaker no longer writes to the store.** Previously every non-trial success called `closeDistributed` unconditionally — a full `SET CLOSED_STATE`, even when the breaker was already closed with zero accumulated failures, i.e. on every single healthy call. The gate's own read (already paid for, since it happens before every call anyway) now threads the observed state (`Gate.observedState`) through to the success path: the write is skipped when that read positively observed clean state (`null`, or `{ isOpen: false, failures: 0 }`), and still happens whenever there's real accumulated state to clear (an open breaker recovering via a non-trial path, or `failures > 0` mid-count) or when the read itself errored and we can't know what's there. Net effect: the steady-state healthy path drops from 1 read + 1 write to 1 read + 0 writes per call. Trial closes (a real open→closed transition) and `errorRate`'s `recordOutcomeAtomic` (which must see every outcome to keep the rate meaningful) are both unchanged — see the revised limitation item 1 in the README. Race analysis: if a concurrent failure lands between this call's gate read and its (now-skipped) write, not writing preserves that concurrent increment — strictly safer than the old unconditional write, which could silently erase it by overwriting with `CLOSED_STATE`.

## [0.3.0] - 2026-07-08

### Added

- `execute()`/`recordFailure()`/`recordSuccess()` now log a one-time warning the first time they see an `operation` name over 100 characters or containing `/` — both common signs of an interpolated URL, tenant ID, or user ID rather than the small fixed dependency name `operation` is meant to be (see the existing README cardinality note). Purely a warning — it never rejects a call.
- New optional `maxOperations` option: logs a one-time warning once the breaker has seen more distinct `operation` names than this, as an early signal that names are dynamic and leaking memory indefinitely rather than waiting to notice via memory growth. Unset by default (no limit, no warning).

## [0.2.0] - 2026-07-03

**Breaking changes in this 0.x minor** (both allowed under semver's pre-1.0 rules, neither is a patch-level change):
1. `CircuitState` widened to include `'half-open'` — a compile-time break for exhaustive `switch`/`if` over the type, even though additive at runtime. See the `Added` entry below.
2. `Store.claimTrial`'s return type changed from `Promise<boolean>` to `Promise<string | null>`, and `Store` gained an optional `releaseTrial(key, token)`. Only relevant if you have a custom `Store` implementation (`RedisStore` is already updated). See the `Fixed` entry below.

### Added

- `CircuitState` (and `onStateChange`) now includes `'half-open'`, fired exactly when a caller claims the single half-open trial slot, in both local and distributed mode. Previously only `'open'`/`'closed'` were observable, so entering half-open recovery was invisible to consumers of the hook. **Note for TypeScript consumers:** widening this union is a compile-time break for any exhaustive `switch`/`if` over `CircuitState` (e.g. a `default: assertNever(state)` pattern) even though it's purely additive at runtime — shipping under a 0.x minor per semver's pre-1.0 allowance, not a patch.
- `canExecute()` now logs a one-time warning the first time it's called in distributed mode, pointing callers at `canExecuteAsync()` — it always returned `true` there (documented), but silently.

### Fixed

- **`claimTrial`/release now uses a per-claim ownership token instead of an unconditional delete, closing a correctness hole in the single-trial guarantee.** Previously, releasing a trial claim was a plain `del()` on the trial key. Sequence that broke it: instance A claims a trial (TTL 10s) but its call runs 12s; at t=10s the claim expires; at t=11s instance B claims a *new* trial on the same key (expected — see item 11); at t=12s A's call finally settles and its release deletes the key — which is now *B's* claim, not A's, since a plain delete can't tell them apart. A third instance could then claim a third concurrent trial while B's was still in flight, one more concurrent trial than the TTL-expiry scenario is supposed to allow. Fixed with the standard Redlock-style pattern: `Store.claimTrial` now returns a unique per-call token (or `null` if the claim failed) instead of a boolean, and a new optional `Store.releaseTrial(key, token)` performs a compare-and-delete — a release only succeeds if the token still matches what's stored, so a stale release can never touch a different, newer claim. `RedisStore` implements this via `randomUUID()` tokens and a Lua compare-and-delete script. **Breaking change to the `Store` interface** for anyone with a custom implementation: `claimTrial`'s return type changed from `Promise<boolean>` to `Promise<string | null>` (see the breaking-changes note at the top of this section).
- **The no-token release fallback initially recreated the exact bug the token scheme fixes, in the path most likely to hit it — now fixed to do nothing instead.** The first cut of the fix above fell back to an unconditional `del()` whenever there was no token to compare (the gate failed open on the trial — e.g. `claimTrial` itself errored — or the store lacks `releaseTrial`). But the `claimTrial`-error case is exactly "instance A doesn't know whether the key exists, and instance B may hold a perfectly valid claim right now" — so releasing via unconditional delete there reintroduces the identical cascade risk item 11 already documents, just relocated to a different trigger. `safeReleaseTrial` now does nothing at all when there's no token to release, on the principle that a claim occupying its slot up to `trialTimeout` longer than necessary is a bounded, safe cost, while deleting a claim you never confirmed is yours is not. A store that implements `claimTrial` but not `releaseTrial` now also logs a one-time warning (matching the existing pattern for the other three optional capabilities) instead of silently falling back to the same unsafe delete.
- Two doc comments (on `Gate.trialToken` in `circuit-breaker.ts` and `Store.releaseTrial` in `store.ts`) still described the pre-fix unconditional-delete fallback as current behavior after the fix above landed — leftover prose from the revision before it changed. Both now describe the actual current behavior: no release is attempted at all when there's no token or no `releaseTrial`; the claim simply expires via its TTL.
- `RedisStore`'s three Lua-scripted operations (`recordFailureAtomic`, `recordOutcomeAtomic`, and the new `releaseTrial`) now use ioredis's `defineCommand` (`EVALSHA` with automatic script-cache management) instead of `eval()` on every call, which shipped the full ~1KB script body over the wire and reparsed it on every single invocation. Meaningful specifically because item 1 in the reliability tradeoffs list already tells users to measure per-call Redis cost — this was avoidable overhead on that exact path.
- Distributed mode now fires `onStateChange('closed', operation)` when a successful half-open trial closes the breaker. This was a pre-existing gap: `reset()` already fired `'closed'` correctly on a real open→closed transition, but the `execute()`-driven successful-trial-close path never did, so distributed-mode consumers had no way to observe recovery via the hook.
- A failed half-open trial now fires `onStateChange('open', operation)` again (both local and distributed mode). Previously `reopenAfterFailedTrialLocal`/`reopenAfterFailedTrialDistributed` updated internal state and logged a warning but fired no event at all — an observer tracking state purely via the hook would see `'half-open'` and then nothing, and could reasonably conclude the breaker was stuck in half-open forever.
- `recordSuccess()` now fires `onStateChange('closed', operation)` in distributed mode when it closes a breaker that was actually open, matching local mode's existing behavior (`resetLocal` already did this correctly). Previously distributed mode's `closeDistributed` never fired the event at all, so closing an open breaker via the manual API was silent in distributed mode but not local mode.
- **Jitter is now rolled once per open episode instead of on every gate check.** Previously `effectiveResetTimeout` called `Math.random()` on every `execute()`/`canExecute()`/`canExecuteAsync()` invocation, which had two real effects: (1) near the reset boundary, the same instance could flip between "blocked" and "eligible" on consecutive calls milliseconds apart, and (2) because callers effectively kept sampling until a roll happened to pass, the *practical* earliest trial time converged toward `capped * (1 - jitter)` rather than being spread around the nominal timeout — a systematic early bias, not random noise. The jittered timeout is now computed once per (operation, `openCount`, `lastFailure`) combination — i.e. once when the breaker opens and once each time a trial fails — and cached until the next real transition.
- `recordFailureDistributedCounting`'s non-atomic fallback no longer resets `openCount` to `0` on every write — it now preserves the prior value (`current.openCount ?? 0`), matching what the `recordFailureAtomic` Lua script and the `errorRate` fallback already did correctly. Closed-phase failures can't normally occur while a breaker is open, but under `failOpenOnStoreError: true` a store blip on the gate read can let a call proceed while the shared state is still open elsewhere; if that call then failed and a store lacking `recordFailureAtomic` fell back to this path, accumulated backoff was silently wiped back to the base `resetTimeout`.
- `reset()` in distributed mode now also clears the internal per-episode jitter cache (`cachedResetTimeouts`), matching `closeDistributed` and `resetLocal`. Harmless either way — a stale cache entry simply misses on the next episode's different `(openCount, lastFailure)` key and recomputes — but the inconsistency was there to be found.

### Changed

- README: documented failover/split-brain tradeoffs in distributed mode — a new "Split-brain: what's prevented, and what isn't" section, plus three new numbered items in the reliability tradeoffs list covering partitioned-instance fail-open behavior, the rare `claimTrial`-error double-trial window, and inherited Redis replication/failover consistency.
- README: documented total coordination-layer outage behavior — a new "Total coordination-layer outage (the store itself is down)" section distinguishing fleet-wide store unavailability from the single-partitioned-instance case, covering fail-open/fail-closed at fleet scale, the lack of buffering/replay or store back-off, and that store HA (Sentinel/Cluster) is the consumer's responsibility.
- README: added a new numbered tradeoff item documenting that a trial outliving its `trialTimeout` claim is a *more likely* double-trial window than the existing `claimTrial`-error case — routine for a slowly-recovering dependency, not rare — and cross-referenced it from the Split-brain section.
- README: clarified that `onStateChange` in distributed mode is a per-process callback fired only on the instance whose own call triggers a given transition, not a fleet-wide broadcast — expanded in both the Options table and the Observability bullet.
- README: added a "Local mode vs. distributed mode: which do you need?" decision guide, documented `RedisStore`'s default key format/prefix, clarified that a single successful trial fully closes the breaker (no N-consecutive-successes support), added the previously-missing per-call latency/Redis-load tradeoff as item 1 in the reliability tradeoffs list (renumbering the rest), and trimmed the "Total coordination-layer outage" section for length.
- README: updated the `Store` interface documentation (`claimTrial`/`releaseTrial` signatures and semantics) in "Bring your own store" — including correcting the `releaseTrial` fallback description to "warn once and let the TTL expire it" rather than the initial (and, per the fix above, incorrect) "falls back to an unconditional delete" — and updated item 11 to state that the token-based release now strictly bounds the double-trial window to two concurrent trials rather than allowing it to cascade further (the double-trial window itself is unchanged and still by design).
- README: added a note that `operation` names should be a small, fixed set (one per dependency) rather than having per-request values (a tenant ID, a URL) interpolated into them — every distinct `operation` string gets its own entry in several per-instance maps that are never evicted, the same high-cardinality-label footgun any metrics library has.

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

[Unreleased]: https://github.com/laurells/redeye/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/laurells/redeye/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/laurells/redeye/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/laurells/redeye/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/laurells/redeye/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/laurells/redeye/releases/tag/v0.1.0
