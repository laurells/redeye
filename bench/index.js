#!/usr/bin/env node
'use strict';

/**
 * Benchmarks three distributed-mode configurations against a real Redis:
 *
 *   1. "no-cache"    -- neither local cache: no closeAtomic/reopenTrialFailureAtomic/
 *                       subscribeTransitions, openCacheRefreshMs disabled, localCache
 *                       unset. Every gate check reads the store. NOT a pre-Release-1
 *                       baseline: the healthy-path write skip lives unconditionally in
 *                       the breaker core, not behind any option here, so this config
 *                       has it too (see its own near-zero `set` count in the report).
 *   2. "release1+2"  -- today's default: the open-state cache (Release 2) on top of
 *                       the write skip both configs already have, localCache off.
 *   3. "localCache"  -- release1+2 plus the closed-state local cache (Release 3),
 *                       opt-in via `localCache: { staleToleranceMs: 100 }`.
 *
 * Workload per config, against a freshly flushed DB:
 *   - 10,000 sequential healthy calls (`execute(op, succeed)`).
 *   - A scripted incident: trip the breaker, hold it open for 5s (issuing
 *     rejected calls throughout, to also measure rejection cost), then let a
 *     trial recover it and run a short batch of post-recovery healthy calls.
 *
 * Reports, per config: total Store-level ops (one entry per Redis round
 * trip: get/set/del/recordFailureAtomic/recordOutcomeAtomic/claimTrial/
 * releaseTrial/closeAtomic/reopenTrialFailureAtomic -- each is exactly one
 * round trip, whether or not its own Lua script also does an XADD/XTRIM
 * internally), and p50/p99 latency added by the breaker on the healthy path
 * and during the incident's rejection window.
 *
 * Requires a real Redis and the package built first:
 *   docker compose up -d
 *   npm run build
 *   node bench/index.js        (or: npm run bench)
 */

const Redis = require('ioredis');
const { CircuitBreaker } = require('../dist/index.js');
const { RedisStore } = require('../dist/stores/redis-store.js');

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const HEALTHY_CALLS = 10000;
const INCIDENT_HOLD_MS = 5000;
const FAILURE_THRESHOLD = 5;
const RESET_TIMEOUT_MS = 5000; // matches the "hold open 5s" incident script

const succeed = () => Promise.resolve('ok');
const fail = () => Promise.reject(new Error('boom'));

function percentile(sortedMs, p) {
  if (sortedMs.length === 0) return 0;
  const idx = Math.min(sortedMs.length - 1, Math.ceil((p / 100) * sortedMs.length) - 1);
  return sortedMs[idx];
}

function summarize(latenciesMs) {
  const sorted = [...latenciesMs].sort((a, b) => a - b);
  return { p50: percentile(sorted, 50), p99: percentile(sorted, 99), count: sorted.length };
}

/** Wraps a real Store, tallying one count per method call -- each call is exactly one Redis round trip. */
class CountingStore {
  constructor(inner) {
    this.inner = inner;
    this.counts = {};
    for (const method of [
      'get',
      'set',
      'del',
      'recordFailureAtomic',
      'recordOutcomeAtomic',
      'claimTrial',
      'releaseTrial',
      'closeAtomic',
      'reopenTrialFailureAtomic',
      'subscribeTransitions',
    ]) {
      if (typeof inner[method] !== 'function') continue;
      this.counts[method] = 0;
      this[method] = (...args) => {
        this.counts[method]++;
        return inner[method](...args);
      };
    }
  }

  total() {
    return Object.values(this.counts).reduce((a, b) => a + b, 0);
  }
}

/**
 * A plain object forwarding only the pre-Release-3 capabilities of a real
 * RedisStore, for the "no-cache" config. Deliberately a fresh object (not a
 * prototype trick) -- closeAtomic/reopenTrialFailureAtomic/
 * subscribeTransitions are simply absent, which is what `if (store.closeAtomic)`
 * checks throughout circuit-breaker.ts key off of. Non-atomic get-then-set
 * fallbacks are what actually run for closes/reopens here, same as real
 * old-fleet coexistence. Note this does NOT disable the healthy-path write
 * skip (Release 1) -- that lives unconditionally in the breaker core, not
 * behind any store capability, so this config has it too.
 */
function stripToNoCache(store) {
  return {
    get: store.get.bind(store),
    set: store.set.bind(store),
    del: store.del.bind(store),
    recordFailureAtomic: store.recordFailureAtomic.bind(store),
    recordOutcomeAtomic: store.recordOutcomeAtomic.bind(store),
    claimTrial: store.claimTrial.bind(store),
    releaseTrial: store.releaseTrial.bind(store),
  };
}

async function runHealthyPath(breaker, operation, n) {
  const latencies = [];
  for (let i = 0; i < n; i++) {
    const start = process.hrtime.bigint();
    await breaker.execute(operation, succeed);
    const end = process.hrtime.bigint();
    latencies.push(Number(end - start) / 1e6);
  }
  return latencies;
}

async function runIncident(breaker, operation) {
  // Trip it.
  for (let i = 0; i < FAILURE_THRESHOLD; i++) {
    await breaker.execute(operation, fail).catch(() => {});
  }

  // Hold it open, issuing rejected calls throughout to measure rejection cost.
  const rejectionLatencies = [];
  const holdUntil = Date.now() + INCIDENT_HOLD_MS;
  while (Date.now() < holdUntil) {
    const start = process.hrtime.bigint();
    await breaker.execute(operation, succeed).catch(() => {});
    const end = process.hrtime.bigint();
    rejectionLatencies.push(Number(end - start) / 1e6);
  }

  // Let the trial recover it, then a short post-recovery healthy batch.
  await breaker.execute(operation, succeed).catch(() => {});
  const recoveryLatencies = await runHealthyPath(breaker, operation, 200);

  return { rejectionLatencies, recoveryLatencies };
}

async function benchConfig(name, redis, buildStore, breakerExtraOpts) {
  await redis.flushdb();
  const rawStore = buildStore();
  const countingStore = new CountingStore(rawStore);
  const breaker = new CircuitBreaker({
    strategy: 'consecutive',
    failureThreshold: FAILURE_THRESHOLD,
    resetTimeout: RESET_TIMEOUT_MS,
    jitter: 0,
    store: countingStore,
    ...breakerExtraOpts,
  });

  // Let an async subscribeTransitions (if any) establish before measuring.
  await new Promise((resolve) => setTimeout(resolve, 150));

  const healthy = await runHealthyPath(breaker, 'bench-op', HEALTHY_CALLS);
  const { rejectionLatencies, recoveryLatencies } = await runIncident(breaker, 'bench-op');

  breaker.destroy();

  return {
    name,
    totalOps: countingStore.total(),
    opsByMethod: { ...countingStore.counts },
    healthy: summarize(healthy),
    rejection: summarize(rejectionLatencies),
    recovery: summarize(recoveryLatencies),
  };
}

function printReport(results) {
  console.log('\n=== redeye distributed-mode benchmark ===\n');
  console.log(`Healthy calls per config: ${HEALTHY_CALLS}, incident hold: ${INCIDENT_HOLD_MS}ms, failureThreshold: ${FAILURE_THRESHOLD}\n`);

  for (const r of results) {
    console.log(`--- ${r.name} ---`);
    console.log(`  Total store ops: ${r.totalOps}`);
    console.log(`  Ops by method:   ${JSON.stringify(r.opsByMethod)}`);
    console.log(`  Healthy path:    p50=${r.healthy.p50.toFixed(3)}ms  p99=${r.healthy.p99.toFixed(3)}ms  (n=${r.healthy.count})`);
    console.log(`  Rejection path:  p50=${r.rejection.p50.toFixed(3)}ms  p99=${r.rejection.p99.toFixed(3)}ms  (n=${r.rejection.count})`);
    console.log(`  Post-recovery:   p50=${r.recovery.p50.toFixed(3)}ms  p99=${r.recovery.p99.toFixed(3)}ms  (n=${r.recovery.count})`);
    console.log('');
  }

  console.log('Rejection-path latency and healthy-path ops-per-call are the two numbers to compare across configs --');
  console.log('release1+2 should show ~0 rejection latency vs. no-cache paying a full round trip per rejected call');
  console.log('(that is what halves its total op count too); localCache should additionally approach 0 ops/call');
  console.log('on the healthy path once its cache is warm.\n');
}

async function main() {
  // retryStrategy: null -- this is a one-shot benchmark run, not a
  // long-lived service; don't let ioredis's background reconnect loop keep
  // the process alive after a failed initial connection.
  const redis = new Redis(REDIS_URL, { maxRetriesPerRequest: 1, lazyConnect: true, retryStrategy: () => null });
  redis.on('error', () => {}); // surfaced instead via the explicit connect()/ping() try-catch below
  try {
    await redis.connect();
    await redis.ping();
  } catch (error) {
    console.error(`Could not reach Redis at ${REDIS_URL}. Start it with "docker compose up -d" first.\n${error}`);
    redis.disconnect();
    process.exitCode = 1;
    return;
  }

  const results = [];

  results.push(
    await benchConfig(
      'no-cache',
      redis,
      () => stripToNoCache(new RedisStore(redis, { keyPrefix: 'redeye-bench:' })),
      { openCacheRefreshMs: 0 },
    ),
  );

  results.push(
    await benchConfig('release1+2 (today\'s default)', redis, () => new RedisStore(redis, { keyPrefix: 'redeye-bench:' }), {}),
  );

  results.push(
    await benchConfig('+localCache (Release 3)', redis, () => new RedisStore(redis, { keyPrefix: 'redeye-bench:' }), {
      localCache: { staleToleranceMs: 100 },
    }),
  );

  printReport(results);

  await redis.quit();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
