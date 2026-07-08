# redeye

[![CI](https://github.com/laurells/redeye/actions/workflows/ci.yml/badge.svg)](https://github.com/laurells/redeye/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/redeye-breaker.svg)](https://www.npmjs.com/package/redeye-breaker)
[![license](https://img.shields.io/npm/l/redeye-breaker.svg)](LICENSE)

A circuit breaker for Node.js with an optional **distributed mode**: instead of tracking failures only in the memory of one process, breaker state lives in Redis (or any store you plug in), so a failure burst against a downstream dependency trips the breaker for *every* instance sharing that store — not just the one that saw the failures.

When paired with `RedisStore`, redeye is a *reliable* distributed circuit breaker, not just a best-effort one: failure counting is atomic (a Redis Lua script, not a racy get-then-set), and recovery goes through a real half-open state where exactly one instance gets to try the dependency again while everyone else stays blocked — the two properties most local-only or naively-distributed circuit breakers skip.

**Read [Reliability model & limitations](#reliability-model--limitations) before using this for anything load-bearing.** A handful of tradeoffs are fundamental to any distributed system (trusting your store, tolerating clock skew) and no library can engineer those away — that section is honest about exactly which ones those are, and which ones redeye actually solves.

## Install

```bash
npm install redeye-breaker
```

Redis support is an optional peer dependency — only needed if you use `RedisStore`:

```bash
npm install ioredis
```

## Usage

### Local mode (default, no dependencies)

```ts
import { CircuitBreaker } from 'redeye-breaker';

const breaker = new CircuitBreaker({
  failureThreshold: 5,
  resetTimeout: 60_000,
});

const result = await breaker.execute('payment-gateway', () => callPaymentGateway());
```

Local mode gets real half-open (single in-flight trial) and backoff+jitter too — it's single-process, so there's no atomicity concern to begin with.

### Local mode vs. distributed mode: which do you need?

Local mode is not the "lesser" option — for a lot of services, N independent per-instance breakers is the right answer, not a compromise:

- **Use local mode** when each instance tripping independently is fine, or even preferable: instances see meaningfully different traffic, the dependency is instance-local anyway (a sidecar, a per-instance cache), or you'd rather each instance protect itself than take on a new piece of shared infrastructure (the store) that can itself fail. Zero dependencies, zero added latency per call, nothing extra to operate.
- **Use distributed mode** when a failure burst against a *shared* downstream dependency should trip the breaker for the whole fleet at once — e.g. a payment gateway or third-party API, where 5 of 6 instances continuing to hammer a dependency the 6th just found dead is actively harmful (wasted capacity, a worse incident, a slower recovery signal). This costs something real in return: a store round trip on every gated decision (see [item 1](#whats-still-a-fundamental-tradeoff-not-a-bug) below), and a new dependency whose own outages now shape the breaker's behavior (see [Total coordination-layer outage](#total-coordination-layer-outage-the-store-itself-is-down)).

If you're not sure, start local — it's strictly cheaper, and nothing about switching to distributed mode later changes your call sites.

### Two tripping strategies

```ts
// Default: trips after N failures in a row. Good for hard drops (a
// dependency going fully down). Resets to zero on any success, so it
// will not catch a dependency that's merely degraded.
new CircuitBreaker({ strategy: 'consecutive', failureThreshold: 5 });

// Trips when an EWMA-smoothed failure rate crosses a threshold, once
// enough samples have been seen. Catches flapping/degrading dependencies
// a consecutive-failure breaker structurally cannot: e.g. an API that
// fails 4 out of every 5 requests (80% failure rate) never fails twice
// in a row in that exact pattern, so 'consecutive' never trips — but
// 'errorRate' does, because it looks at the rate, not the streak.
new CircuitBreaker({
  strategy: 'errorRate',
  errorRateThreshold: 0.5, // open at >= 50% failure rate
  minimumCalls: 10,        // ...but only once we've seen at least 10 calls
  errorRateDecay: 0.9,     // how much weight recent calls get vs. history
});
```

Pick `consecutive` when you mainly care about clean outages, `errorRate` when the dependency is more likely to degrade than to go fully dark. You can run two breaker *instances* with different strategies over the same logical operation for both kinds of protection at once — in distributed mode, give each its own `RedisStore` `keyPrefix` (or the two breakers will read/write the same Redis key with incompatible state shapes and corrupt each other).

### Distributed mode (Redis-backed, fully atomic)

```ts
import { CircuitBreaker } from 'redeye-breaker';
import { RedisStore } from 'redeye-breaker/redis-store';
import Redis from 'ioredis';

const redis = new Redis(process.env.REDIS_URL);
const store = new RedisStore(redis, { keyPrefix: 'myapp:' });

const breaker = new CircuitBreaker({
  failureThreshold: 5,
  resetTimeout: 60_000,
  store, // <- presence of a store is what enables distributed mode
  onStateChange: (state, operation) => {
    console.warn(`circuit breaker for ${operation} is now ${state}`);
  },
  onStoreError: (error, operation) => {
    // Redis unreachable, timed out, etc. — see "Store unavailability" below.
    console.error(`circuit breaker store error for ${operation}`, error);
  },
});

await breaker.execute('payment-gateway', () => callPaymentGateway());
```

Every process pointed at the same Redis instance (and using the same operation name / key prefix) shares breaker state, with atomic counting and single-trial recovery.

`RedisStore` stores state under `{keyPrefix}circuit_breaker:{operation}`, plus `{keyPrefix}circuit_breaker:{operation}:trial` while a half-open trial is in flight. `keyPrefix` defaults to `''` (no prefix) — set one (as above) if you share a Redis instance/DB across services and want your keys namespaced.

### Bring your own store

`RedisStore` implements a `Store` interface with two required methods and four *optional* ones:

```ts
export interface Store {
  get<T>(key: string): Promise<T | null>;
  set<T>(key: string, value: T, ttlSeconds: number): Promise<void>;
  del(key: string): Promise<void>;

  // Optional — implement these for the reliability guarantees below.
  // Without them, redeye falls back to best-effort semantics and logs a
  // one-time warning telling you exactly what's degraded.
  recordFailureAtomic?(key: string, opts: { ttlSeconds: number; failureThreshold: number; now: number }): Promise<CircuitBreakerState>;
  recordOutcomeAtomic?(key: string, opts: { success: boolean; decay: number; minimumCalls: number; errorRateThreshold: number; ttlSeconds: number; now: number }): Promise<CircuitBreakerState & { openedNow: boolean }>;
  claimTrial?(key: string, ttlSeconds: number): Promise<string | null>;
  releaseTrial?(key: string, token: string): Promise<void>;
}
```

- `recordFailureAtomic` (`strategy: 'consecutive'`) should increment the failure count and decide `isOpen` in one atomic round trip (a Lua script in Redis, a conditional update in DynamoDB, etc.).
- `recordOutcomeAtomic` (`strategy: 'errorRate'`) should fold one call's outcome into the EWMA rate and decide `isOpen` in one atomic round trip — called on every closed-phase call, not just failures.
- `claimTrial` should be a conditional "create if absent" write (`SET key token NX EX ttl` in Redis, with `token` a fresh unique value per call, e.g. a UUID) — it's what makes half-open recovery exclusive to one caller instead of a free-for-all. Return the token you wrote if this call won the claim, or `null` if the key was already taken.
- `releaseTrial` should be a compare-and-delete (`if GET key == token then DEL key` in Redis, a conditional delete elsewhere), not an unconditional delete. This is what stops a trial that outran its TTL (see [item 11](#whats-still-a-fundamental-tradeoff-not-a-bug)) from deleting a *different* instance's newer claim on the same key when it finally gets around to releasing its own now-stale one. If omitted (or if a call has no confirmed ownership token to release at all — e.g. `claimTrial` itself errored), redeye logs a one-time warning and leaves the claim for its TTL to expire naturally, rather than risk an unconditional delete that could destroy a claim it never confirmed was its own.

Implement the two required methods against Memcached, DynamoDB, your own cache wrapper, etc., and you have a working (best-effort) distributed breaker. Add the optional methods relevant to the strategy you use when that store supports a real atomic increment and a real conditional write, and you get the full reliability guarantees.

## API

- `execute(operation, fn)` — runs `fn` if the breaker allows it (closed, or this call won the half-open trial); throws immediately without calling `fn` otherwise. Records the outcome automatically.
- `canExecute(operation)` — synchronous, read-only, best-effort check. In distributed mode it can't await the store, so it can only consult the local open-state cache (see `openCacheRefreshMs` below): a `false` is a real, cached-open result, but a `true` is still only advisory — it means either nothing is cached open, or nothing has been cached yet, not a confirmed closed state — use `canExecuteAsync` for that. Never claims a trial slot. Logs a one-time warning the first time it's called in distributed mode, explaining that asymmetry.
- `canExecuteAsync(operation)` — async, store-aware, read-only check. Honors `failOpenOnStoreError`. Never claims a trial slot — see the TOCTOU note below.
- `recordFailure(operation)` / `recordSuccess(operation)` — manually record an outcome without routing the call through `execute`. Note: these bypass the half-open trial-claim mechanism entirely (see [limitations](#reliability-model--limitations)) — prefer `execute`.
- `getState(operation)` — returns `{ failures, lastFailure, isOpen, openCount, errorRate, sampleCount }`. `openCount` is how many consecutive half-open trials have failed since the breaker last fully closed (drives backoff). `errorRate`/`sampleCount` are only meaningful under `strategy: 'errorRate'`.
- `getMetrics(operation)` / `getAllMetrics()` — per-instance counters: `{ totalCalls, totalSuccesses, totalFailures, totalRejections, totalStoreErrors }`.
- `reset(operation)` — manually closes the breaker for an operation.
- `destroy()` — stops the internal monitor interval. Call this when a breaker instance is no longer needed (e.g. in tests, or on service shutdown) to avoid leaking a timer.

`operation` should be a small, fixed set of names (`'payment-gateway'`, `'fraud-api'`, one per dependency) — not something you interpolate per-request values into (a tenant ID, a URL, a user ID). Every distinct `operation` string gets its own entry in several per-instance maps (metrics, local-mode failure/backoff state, an internal jitter cache) that are never evicted, so a dynamic operation name is an unbounded memory leak, the same footgun as labeling a Prometheus metric with a high-cardinality value.

redeye can't stop you from doing this, but it can flag it: `execute`/`recordFailure`/`recordSuccess` log a one-time warning the first time they see an `operation` name over 100 characters or containing `/` (both common signs of an interpolated URL or ID), and — if you set `maxOperations` — a second one-time warning once the breaker has seen more distinct operation names than that. Neither check ever rejects a call; they're a smoke alarm, not enforcement.

## Options

| Option | Default | Description |
|---|---|---|
| `strategy` | `'consecutive'` | `'consecutive'` or `'errorRate'` — see [Two tripping strategies](#two-tripping-strategies) |
| `failureThreshold` | `5` | `'consecutive'` only: failures in a row before the breaker opens |
| `errorRateThreshold` | `0.5` | `'errorRate'` only: failure rate (0-1) at or above which the breaker opens |
| `minimumCalls` | `10` | `'errorRate'` only: minimum samples before the rate can trip the breaker |
| `errorRateDecay` | `0.9` | `'errorRate'` only: EWMA decay factor — closer to 1 weighs history more heavily |
| `resetTimeout` | `60000` | ms to stay open before allowing a half-open trial |
| `backoffMultiplier` | `2` | Multiplies `resetTimeout` on each failed trial (`1` disables backoff) |
| `maxResetTimeout` | `resetTimeout * 8` | Ceiling for the backed-off reset timeout |
| `jitter` | `0.1` | Randomizes the effective reset timeout by ±this fraction, so instances don't all retry in lockstep |
| `trialTimeout` | `min(timeout ?? 10000, resetTimeout)` | Max ms a claimed half-open trial may run before its claim is released/expires |
| `monitorInterval` | `5000` | ms between local-mode sweeps for a trial that never settled (safety net) |
| `timeout` | none | optional per-call timeout in ms |
| `store` | none | enables distributed mode when provided |
| `failOpenOnStoreError` | `true` | see [Store unavailability](#store-unavailability-fail-open-vs-fail-closed) |
| `onStateChange` | none | `(state: 'open' \| 'half-open' \| 'closed', operation: string) => void` — `'half-open'` fires exactly when a caller claims the single half-open trial slot. In distributed mode this is a per-process callback, not a fleet-wide broadcast — see [Observability](#what-redeye-actually-solves-with-redisstore-or-any-store-implementing-the-matching-optional-methods) below for which instance actually sees it. |
| `onStoreError` | none | `(error: unknown, operation: string) => void` — fired whenever a store read/write fails, in addition to being logged |
| `logger` | no-op | `{ warn(msg), log(msg) }` — plug in your own logger |
| `maxOperations` | none | Logs a one-time warning once the breaker has seen more than this many distinct `operation` names — see the cardinality note above. Unset: no limit, no warning. |
| `openCacheRefreshMs` | `2000` | Distributed mode only: while `operation` is known-open, reject locally without a store read, re-verifying against the store at most once per this many ms (to catch an early close by another instance's trial, or a manual `reset()`). Never delays trial eligibility — see [limitation item 1](#whats-still-a-fundamental-tradeoff-not-a-bug). `0` disables the cache (a store read on every call, the pre-cache behavior). |

### Store unavailability: fail-open vs fail-closed

If the store itself throws (Redis is down, times out, network partition, etc.), redeye does **not** treat that the same as the protected operation failing — it's a distinct condition, controlled by `failOpenOnStoreError`:

- **`true` (default)** — treat the breaker as closed and let the call through. A store outage degrades you to "no circuit-breaker protection," not "every call blocked."
- **`false`** — throw `StoreUnavailableError` (exported) without calling the wrapped function at all. Appropriate when the protected operation is expensive, dangerous to retry blindly, or would itself add load to whatever's already causing the outage.

Writes back to the store are **always** best-effort — a write failure is logged and reported via `onStoreError`, but never thrown, and never overrides the real result of a call that already happened.

### Total coordination-layer outage (the store itself is down)

Same `failOpenOnStoreError` branch as above, just hit by every instance at once instead of one partitioned instance — redeye doesn't promote a backup coordinator or fall back to per-instance local tracking as a substitute:

- **`true` (default):** every instance lets every call through — no breaking happens anywhere, fleet-wide, for as long as the outage lasts.
- **`false`:** every instance blocks every call with `StoreUnavailableError`, fleet-wide, including requests that would have succeeded.

Nothing is buffered or replayed: outcomes during the outage are never recorded, and the breaker has no memory of the blackout once the store recovers. There's also no back-off from hitting a store known to be down — every guarded call keeps retrying it, so pair `failOpenOnStoreError: false` with fail-fast client options (`ioredis`'s `maxRetriesPerRequest`, `enableOfflineQueue: false`) if you don't want calls queuing on the client's own reconnect logic. Recovery is instant and automatic on the next successful read — no warm-up, no manual reset.

High availability of the store itself (Sentinel, Cluster) is your responsibility, configured on the client you hand to `RedisStore` — whatever failover consistency that setup provides is inherited as-is (see [Split-brain](#split-brain-whats-prevented-and-what-isnt)).

## Reliability model & limitations

### What redeye actually solves (with `RedisStore`, or any store implementing the matching optional methods)

- **Exact counting under concurrent failure, for both strategies.** `recordFailureAtomic` (consecutive) and `recordOutcomeAtomic` (errorRate) each run as a single Lua script inside Redis's single-threaded execution — two instances updating at the same instant cannot race and lose an update the way a plain get-then-set would.
- **Catches flapping, not just hard drops.** `strategy: 'errorRate'` trips on a smoothed failure *rate* once enough samples are seen, so a dependency that succeeds 1 in 5 requests (an 80% failure rate) still trips the breaker even though it never fails N times consecutively — a case `'consecutive'` structurally cannot catch, by design (see [Two tripping strategies](#two-tripping-strategies)).
- **Real half-open, single-trial recovery.** When the reset window elapses, callers don't all rush in — one caller atomically claims the trial slot (`claimTrial`, a conditional write) and the rest stay blocked (`Circuit breaker is open`) until that trial resolves. This is the classic Hystrix-style half-open state, not "everyone tries at once." Applies to both strategies. A single successful trial fully closes the breaker — redeye does not support requiring N consecutive successful trials before closing; that's intentionally out of scope for now, not an oversight.
- **Exponential backoff with jitter.** A dependency that keeps failing its trial is retried less often each time (`resetTimeout * backoffMultiplier ^ openCount`, capped at `maxResetTimeout`), and the exact retry moment is jittered per-instance so a cluster doesn't hammer a recovering dependency in lockstep.
- **No hard dependency on precise TTL timing for correctness.** The gating decision is based on elapsed time since the last recorded failure, not "did the key vanish yet" — the store's TTL is a generous safety-net for cleanup, not the primary mechanism. (Early eviction is still possible — see below — but it degrades gracefully instead of being load-bearing.)
- **Store outages are a distinct, handled condition**, not silently misattributed as the protected operation failing (see fail-open/fail-closed above).
- **Observability**: per-instance call/success/failure/rejection/store-error counters via `getMetrics`, plus `onStateChange` and `onStoreError` hooks. In distributed mode, `onStateChange` is invoked locally by whichever instance's own call happens to trigger a given transition — not broadcast to the rest of the fleet. Concretely: `'open'` fires only on the instance whose write is the one that crosses the threshold (or whose failed trial reopens it); `'half-open'` fires only on the instance that wins the trial claim; `'closed'` fires only on the instance that closes it (via a successful trial, or a manual `recordSuccess()`/`reset()` call). Every other instance sharing that operation still sees the new state reflected in their own gating decisions immediately — they just never get their own `onStateChange` call for a transition they didn't personally cause. If you need a fleet-wide notification (e.g. to page on-call once, not once per instance that happens to notice), de-duplicate downstream of the hook, or drive alerting off the store directly instead.

If your store *doesn't* implement the atomic method your chosen strategy needs (`recordFailureAtomic` or `recordOutcomeAtomic`) or `claimTrial`, redeye logs a one-time warning per missing capability and falls back to a non-atomic get-then-set — it still works, just without the atomicity/exclusivity guarantee for that piece.

### Split-brain: what's prevented, and what isn't

There's no split-brain among your app instances in the classic sense, because instances never hold independent authoritative state to disagree over — every instance reads the store fresh before every single decision (`gateDistributed`) instead of trusting a locally cached opinion. The store is the only "brain"; instances are just readers/writers of it. Two mechanisms enforce agreement on the common path:

- **Atomic writes.** `recordFailureAtomic`/`recordOutcomeAtomic` run as one Lua script — GET, decode, increment, decide, SET — inside Redis's single-threaded execution, so two instances failing at the same instant can't both read the same count and both write the same increment.
- **Exclusive trial claim.** `claimTrial` is a `SET ... NX`, which Redis resolves atomically, so exactly one instance ever runs the half-open recovery probe — never two instances simultaneously deciding they're the one testing recovery.

So the design doesn't eliminate split-brain so much as route around it: it pushes the single-arbiter requirement onto the store and never lets an instance act on stale local state instead of asking the store. That said, there are real, narrow windows where instances *can* still disagree — each one is a deliberate availability-over-consistency choice, not a hidden gap:

- **An instance partitioned from the store fails open independently** (item 9 below) — during the partition, that one instance's view of the breaker can genuinely diverge from the rest of the fleet's.
- **A `claimTrial` error can rarely produce two "winners"** (item 10 below) — a narrow window traded for never permanently wedging the breaker open.
- **A slow trial outliving its claim's TTL is the *more likely* way to get two concurrent trials** (item 11 below) — routine for a slowly-recovering dependency, not rare like item 10.
- **The store's own replication/failover consistency is inherited, not solved** (item 12 below) — this library adds no consensus layer on top of whatever your Redis deployment already guarantees.
- **Clock skew causes timing disagreement, not state disagreement** (item 3 below) — it can shift exactly when a trial becomes eligible, but it cannot cause a double-trial, since that's arbitrated by the store, not by comparing clocks.

### What's still a fundamental tradeoff, not a bug

These are true of any distributed circuit breaker, rate limiter, or lock built on a shared store — not gaps specific to redeye:

1. **Distributed mode costs Redis round trips per guarded call, not zero — except while the breaker is actually open, which is exactly when that matters most.** On the closed, healthy path, every `execute()` call does a gate read (`store.get`) before deciding whether to proceed; what happens after differs by strategy — `errorRate` writes (`recordOutcomeAtomic`) on every closed-phase call, success or failure, since the rate needs both to be meaningful, while `consecutive` only writes when the gate's own read observed accumulated state to clear (an open breaker, or `failures > 0`), making a run of healthy successes 1 read and 0 writes per call, not 1-and-1 (a measured claim about the steady state, not a guarantee — a dependency flapping near the failure threshold still writes on most calls). **Once the breaker is open, rejections stop costing a read at all**: the gate caches the open state locally (`lastFailure`, `openCount`, and the same jittered `expiresAt` the eligibility check itself uses) and rejects straight from that cache, re-verifying against the store only once per `openCacheRefreshMs` (default 2000ms) — to catch an early close by another instance's trial, or a manual `reset()` — and always falling through to a real read right at `expiresAt`, so the cache can delay a rejection's freshness but can never delay a trial's eligibility. Net effect: the request-volume-scaling cost this item used to describe applies to the closed, healthy path — the load *during an incident*, when the store or its host infrastructure may itself be degraded, drops instead of scaling with traffic. A resolving half-open trial still adds a round trip to claim it and another to release its claim — that path is unaffected, and unconditional, regardless of strategy. On a latency-sensitive path the closed-path round trip is still added tail latency — the opposite tradeoff from something like Envoy's outlier detection, which caches state locally with a short TTL and accepts bounded staleness instead of paying a store round trip per call. redeye chose per-call freshness over that on the closed path; it's a reasonable choice for many services, but measure it before putting this in front of your highest-QPS call. Local mode has none of this cost — see [Local mode vs. distributed mode](#local-mode-vs-distributed-mode-which-do-you-need) if you're not sure you need shared state at all.
2. **Correctness depends on trusting your store.** If Redis evicts a breaker key early under memory pressure (e.g. `maxmemory-policy allkeys-lru` with `circuit_breaker:*` competing for space with everything else), the breaker can reset earlier than intended. Mitigate by giving circuit-breaker keys their own keyspace/DB with a policy that doesn't evict them, or by monitoring `onStoreError` and Redis memory pressure directly. This is not fixable in-library — it's an operational configuration matter.
3. **Distributed timing is sensitive to clock skew.** Backoff and jitter windows are computed by comparing each instance's local `Date.now()` against a `lastFailure` timestamp written by whichever instance recorded it. With reasonably synchronized clocks (standard NTP) this is a non-issue; on a fleet with significant clock drift, instances can disagree by that drift amount about exactly when a trial becomes eligible. This affects *timing* only — the half-open claim mechanism (`claimTrial`) still guarantees exactly one trial runs regardless of skew, so skew cannot cause a thundering herd, only a slightly early or late trial attempt.
4. **`recordFailure`/`recordSuccess`/`canExecuteAsync` don't participate in trial claiming.** Only `execute()` claims and releases the half-open trial slot. If you build your own call flow around the manual API instead of `execute()`, you lose the single-trial guarantee and get the old "everyone retries once elapsed" behavior for that flow. Prefer `execute()`.
5. **One strategy/threshold/backoff policy per breaker instance**, applied uniformly to every `operation` string passed to it. Use separate `CircuitBreaker` instances for dependencies that need different policies — and separate `RedisStore` key prefixes if two breakers share an operation name with different strategies (their state shapes are incompatible and will corrupt each other under the same key).
6. **Local mode has no cross-restart persistence**, by design — it's explicitly the zero-dependency option. Use distributed mode if breaker state needs to survive a process restart.
7. **Metrics are per-instance**, not automatically aggregated across your fleet — the same as any Prometheus counter you'd scrape per-pod. Aggregate them in your own metrics backend if you need a fleet-wide view.
8. **`errorRate` is EWMA-smoothed, not a precise sliding window.** It approximates "failure rate over roughly the last ~`1/(1-decay)` calls," not an exact count over an exact window — good enough to catch flapping dependencies, but the exact trip point for a given call sequence is a function of the decay math, not literally "N of the last M calls."
9. **A partitioned instance fails open on its own, independently of the rest of the fleet.** If an instance's own call to the store errors (that instance can't reach Redis, but others can), it doesn't consult anyone else — it applies `failOpenOnStoreError` locally (default `true`). For the duration of that partition, the isolated instance treats the breaker as closed while the rest of the fleet, still able to reach the store, correctly sees it open. This is scoped to exactly the partitioned instance for exactly the duration of the partition, and it's a deliberate choice: favoring that one instance staying available over it blocking calls based on a store it can't even confirm the state of.
10. **A `claimTrial` error can rarely let two instances both believe they won the trial.** If the `claimTrial` call itself throws (not "no key", an actual error), the gate does not block the caller — it fails open on the trial too (`allowed: true, isTrial: true`), because refusing here would mean a single store blip at exactly the wrong moment could permanently wedge the breaker open (no one could ever claim the trial again). If two instances hit that specific error in the same narrow window, both can proceed with a trial call. This trades a rare double-trial for never risking permanent lockout.
11. **The trial-claim TTL expiring mid-flight is a more likely double-trial window than item 10, and it's by design.** `claimTrial`'s key has a TTL of `trialTimeout` (default `min(timeout ?? 10000, resetTimeout)`). If the trial call itself runs *longer* than that TTL, the claim expires while the call is still in flight, and another instance can then claim a second trial against the same recovering dependency — this is the intended anti-wedging mechanism (nothing should be able to hold the exclusive slot forever), not a bug. It's also the routine case, not the rare one: a slowly-recovering dependency whose first trial takes 12 seconds against a 10-second default `trialTimeout` will hit this on every recovery attempt, not occasionally. Set `trialTimeout` comfortably above your dependency's real p99 recovery-check latency if avoiding this matters to you. **What this does *not* do, strictly bounded by the release mechanism:** cascade into a third concurrent trial. `claimTrial` returns a per-call ownership token, and releasing a claim is always either a compare-and-delete against that exact token or nothing at all — never an unconditional delete — so the original caller's own expired claim, once it finally gets released, can never destroy whichever *other* instance's claim now legitimately occupies the key. That makes this limitation "double trial, strictly bounded to two" rather than "double trial, possibly cascading further."
12. **`RedisStore` inherits Redis's own replication/failover consistency — it doesn't add a consensus layer on top.** A single Redis instance has nothing to split-brain over. But if you run Sentinel or Cluster behind it, a failover with asynchronous replication can lose the last few writes, and reading from a lagging replica can return stale state. `RedisStore` doesn't issue `WAIT` or otherwise wait for replica acknowledgment — it trusts whatever consistency guarantees your Redis deployment itself provides.

### What redeye deliberately does not try to be

redeye is scoped to "circuit breaking, done correctly, optionally shared via a store." It does not do retries, bulkheading, or request hedging. If you need those, compose redeye with a separate retry library, and think carefully about ordering: retries should generally happen *inside* what the breaker counts as a single call, not wrapped around it — otherwise a retry storm can trip the breaker faster than intended, or mask real failures from it entirely.

## Testing

`npm test` runs the unit suite (`test/*.spec.ts`) against in-memory fakes — no external services required.

`npm run test:integration` runs `test/*.integration.spec.ts` against a real Redis, exercising `RedisStore`'s actual Lua scripts and `SET ... NX` trial-claim logic instead of a reimplementation of them. Start Redis first:

```sh
docker compose up -d
npm run test:integration
docker compose down
```

It connects to `REDIS_URL` (default `redis://localhost:6379`).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Changes are tracked in [CHANGELOG.md](CHANGELOG.md).

## License

MIT
