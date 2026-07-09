# Benchmark

Results from `bench/index.js` (`npm run bench`), run against a real Redis. This is the data behind the `localCache`/`openCacheRefreshMs` claims in [README.md](README.md#local-caching-hybrid-mode) and the `Reliability model & limitations` section's limitation item 1 — re-run it yourself before trusting these numbers on different hardware, since latency in particular is environment-sensitive.

## Environment

- Redis 7.4.9 (`redis:7-alpine`, via `docker-compose.yml`)
- Node v22.19.0
- ioredis 5.11.1
- Redis reached over `localhost:6379` from the host (Docker Desktop for Windows, WSL2 backend) — not a bare-metal Linux/Redis network path, so treat absolute latency numbers as indicative, not a floor. The *ops-per-call* counts are exact regardless of network path.
- `redeye-breaker` 0.5.0

## Method

Three configurations, each against a freshly flushed DB, `strategy: 'consecutive'`:

1. **no-cache** — neither local cache: `closeAtomic`/`reopenTrialFailureAtomic`/`subscribeTransitions` stripped from the store (so `closeDistributed`/`reopenAfterFailedTrialDistributed` use their non-atomic get-then-set fallback), `openCacheRefreshMs: 0` (no open-state cache), `localCache` unset (no closed-state cache). **Not** a pre-Release-1 baseline — the healthy-path write-skip lives unconditionally in the breaker core, not behind any capability or option, so this config has it too (see its own `set` count below). A true pre-Release-1 run isn't reproducible through configuration at all; it would show one `set` per healthy call instead of effectively zero.
2. **release1+2 (today's default)** — full `RedisStore`, `localCache` unset. Gets the open-state rejection cache (Release 2, `openCacheRefreshMs` default 2000ms) on top of the write-skip both configs already have.
3. **+localCache (Release 3)** — same as above, plus `localCache: { staleToleranceMs: 100 }`.

Workload per config:

- 10,000 sequential `execute(op, succeed)` calls against a single operation (the healthy path).
- A scripted incident: trip the breaker (`failureThreshold: 5` consecutive failures), hold it open for 5 seconds while continuously issuing calls (the rejection path — this is where volume is highest during a real incident), let a trial recover it, then 200 more healthy calls (post-recovery).

Reported per config: total `Store`-level calls (`CountingStore` wraps the real `RedisStore` and tallies one count per method call — each is exactly one Redis round trip, whether or not its own Lua script also does an internal `XADD`/`XTRIM`), and p50/p99 latency added by `execute()` on each of the three phases.

## Results

| Config | Total store ops | Healthy p50 / p99 | Rejection p50 / p99 (n) | Post-recovery p50 / p99 |
|---|---|---|---|---|
| no-cache | 20,267 | 0.371ms / 1.323ms | 0.405ms / 1.345ms (10,053) | 0.506ms / 1.463ms |
| release1+2 (default) | 10,216 | 0.357ms / 1.117ms | 0.010ms / 0.033ms (411,103) | 0.410ms / 1.060ms |
| +localCache | **14** | **0.001ms** / 0.004ms | 0.009ms / 0.029ms (464,042) | **0.001ms** / 0.002ms |

Ops by method, for reference:

```
no-cache:     { get: 20259, set: 1, recordFailureAtomic: 5, claimTrial: 1, releaseTrial: 1 }
release1+2:   { get: 10208, recordFailureAtomic: 5, claimTrial: 1, releaseTrial: 1, closeAtomic: 1 }
+localCache:  { get: 5, recordFailureAtomic: 5, claimTrial: 1, releaseTrial: 1, closeAtomic: 1, subscribeTransitions: 1 }
```

(`recordFailureAtomic`, `claimTrial`, `releaseTrial`, `closeAtomic`, `subscribeTransitions` counts are identical across the two non-`no-cache` configs — they come from the fixed 5-failure trip + one trial recovery + one subscription setup that every config's incident script does once, not from the 10,000-call healthy loop. The whole story is in `get` and, for `no-cache`, the once-per-run `set`.)

## Reading the numbers

- **`no-cache`'s own `set` count (1, not ~10,000) already shows the write-skip is in effect** — it isn't behind any option this benchmark toggles, so all three configs have it. Don't read this table as "write-skip vs. no write-skip"; all three rows already reflect it.
- **Healthy path, `no-cache` vs. release1+2**: `get` count is roughly halved (20,259 → 10,208) — that gap (≈10,051) is almost exactly `no-cache`'s rejection-path call count (10,053). This is the Release 2 open-state cache eliminating a store read on every *rejected* call during the incident hold, not anything about the healthy path itself: `release1+2` still does a real `store.get` on every single healthy call, identically to `no-cache`. Healthy-path latency confirms this — both are ~0.36-0.37ms p50, statistically indistinguishable, because both still pay one network round trip per healthy call. The difference between these two configs is invisible on the healthy-path axis and only shows up in the rejection-path numbers below.
- **Healthy path, release1+2 vs. +localCache**: `get` drops from 10,208 to 5 (a handful of the cache's own warm-up read plus the fallback-poll's occasional reconciliation reads, not one per call) — a **~2,000x** reduction in store ops for the healthy path specifically, and this *is* `localCache`'s doing (it's the only config of the three that touches the healthy-path read at all). Latency follows: p50 drops from 0.357ms to 0.001ms, over **300x**, because the fast path is now a local map lookup with zero network involvement.
- **Rejection path (the incident hold)**: this is where release1+2's open-state cache does its job, independent of `localCache` — both non-`no-cache` configs show p50 ≈ 0.01ms during the 5-second hold, a **~40x** drop from `no-cache`'s 0.405ms, and the incident absorbed 411k-464k calls in the same 5-second window once rejections stopped costing a round trip (vs. 10,053 for `no-cache`, which is network-bound the whole time). This is the "load during an incident should be lowest, not highest" claim, and it doesn't require `localCache` — `openCacheRefreshMs` alone gets you this.
- **14 total ops for the entire +localCache run** (10,000 healthy calls + a full incident + recovery + 200 more calls) is the headline number: `consecutive` genuinely reaches a 0-store-op steady state once the cache is warm, exactly as designed. Read this as *decoupled from request volume* (at most one read per `staleToleranceMs` window under sustained load, plus one poll per idle operation every ~5s), not as a literal universal constant — this run-to-run count (13, 14, ...) is partly an artifact of the run fitting inside a handful of staleness/poll windows; a longer or higher-concurrency run would show a few more, still bounded, still nothing like one-per-call.

## Reproducing

```sh
docker compose up -d
npm run bench
docker compose down
```

Connects to `REDIS_URL` (default `redis://localhost:6379`), same as the integration suite.
