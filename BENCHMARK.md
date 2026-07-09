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

1. **baseline** — approximates pre-Release-1/2/3: `closeAtomic`/`reopenTrialFailureAtomic`/`subscribeTransitions` stripped from the store (so `closeDistributed`/`reopenAfterFailedTrialDistributed` use their non-atomic get-then-set fallback), `openCacheRefreshMs: 0` (no open-state cache).
2. **release1+2 (today's default)** — full `RedisStore`, `localCache` unset. Gets the healthy-path write-skip (Release 1, which is unconditional, not opt-in) and the open-state rejection cache (Release 2, `openCacheRefreshMs` default 2000ms).
3. **+localCache (Release 3)** — same as above, plus `localCache: { staleToleranceMs: 100 }`.

Workload per config:

- 10,000 sequential `execute(op, succeed)` calls against a single operation (the healthy path).
- A scripted incident: trip the breaker (`failureThreshold: 5` consecutive failures), hold it open for 5 seconds while continuously issuing calls (the rejection path — this is where volume is highest during a real incident), let a trial recover it, then 200 more healthy calls (post-recovery).

Reported per config: total `Store`-level calls (`CountingStore` wraps the real `RedisStore` and tallies one count per method call — each is exactly one Redis round trip, whether or not its own Lua script also does an internal `XADD`/`XTRIM`), and p50/p99 latency added by `execute()` on each of the three phases.

## Results

| Config | Total store ops | Healthy p50 / p99 | Rejection p50 / p99 (n) | Post-recovery p50 / p99 |
|---|---|---|---|---|
| baseline | 20,037 | 0.356ms / 0.967ms | 0.415ms / 1.358ms (9,823) | 0.449ms / 1.015ms |
| release1+2 (default) | 10,217 | 0.370ms / 1.039ms | 0.010ms / 0.032ms (418,154) | 0.362ms / 1.345ms |
| +localCache | **13** | **0.002ms** / 0.023ms | 0.009ms / 0.026ms (483,436) | **0.001ms** / 0.001ms |

Ops by method, for reference:

```
baseline:     { get: 20029, set: 1, recordFailureAtomic: 5, claimTrial: 1, releaseTrial: 1 }
release1+2:   { get: 10209, recordFailureAtomic: 5, claimTrial: 1, releaseTrial: 1, closeAtomic: 1 }
+localCache:  { get: 4, recordFailureAtomic: 5, claimTrial: 1, releaseTrial: 1, closeAtomic: 1, subscribeTransitions: 1 }
```

(`recordFailureAtomic`, `claimTrial`, `releaseTrial`, `closeAtomic`, `subscribeTransitions` counts are identical across the two non-baseline configs — they come from the fixed 5-failure trip + one trial recovery + one subscription setup that every config's incident script does once, not from the 10,000-call healthy loop. The whole story is in `get` and, for baseline, the once-per-run `set`.)

## Reading the numbers

- **Healthy path, baseline vs. release1+2**: `get` count is roughly halved (20,029 → 10,209) purely from Release 1's write-skip — a `set` almost never happens against an already-clean key (baseline: 1 total `set` across the whole run), so the *reads* were already the dominant cost even before `localCache`; `release1+2` doesn't reduce them at all (a real `store.get` still runs on every healthy call). Healthy-path latency is close to identical between the two (~0.35-0.37ms p50) because both still pay one network round trip per call — the difference between them is invisible on this axis and only shows up in the op count.
- **Healthy path, release1+2 vs. +localCache**: `get` drops from 10,209 to 4 (the 4 are the cache's own warm-up read plus the fallback-poll's occasional reconciliation reads, not one per call) — a **~2,500x** reduction in store ops for the healthy path specifically. Latency follows: p50 drops from 0.370ms to 0.002ms, roughly **185x**, because the fast path is now a local map lookup with zero network involvement.
- **Rejection path (the incident hold)**: this is where release1+2's open-state cache does its job, independent of `localCache` — both non-baseline configs show p50 ≈ 0.01ms during the 5-second hold, a **~40x** drop from baseline's 0.415ms, and the incident absorbed 418k-483k calls in the same 5-second window once rejections stopped costing a round trip (vs. 9,823 for baseline, which is network-bound the whole time). This is the "load during an incident should be lowest, not highest" claim, and it doesn't require `localCache` — `openCacheRefreshMs` alone gets you this.
- **13 total ops for the entire +localCache run** (10,000 healthy calls + a full incident + recovery + 200 more calls) is the headline number: `consecutive` genuinely reaches a 0-store-op steady state once the cache is warm, exactly as designed.

## Reproducing

```sh
docker compose up -d
npm run bench
docker compose down
```

Connects to `REDIS_URL` (default `redis://localhost:6379`), same as the integration suite.
