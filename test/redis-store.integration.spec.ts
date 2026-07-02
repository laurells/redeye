import { Redis } from 'ioredis';
import { RedisStore } from '../src/stores/redis-store';
import { CircuitBreaker } from '../src/circuit-breaker';

/**
 * Runs against a real Redis instance (see docker-compose.yml: `docker compose
 * up -d` then `npm run test:integration`). Unlike the unit suite's in-memory
 * fakes, this exercises the actual Lua scripts and `SET ... NX` semantics —
 * the things that are easy to get subtly wrong (cjson field types, TTL
 * arguments, atomicity under real network concurrency) and that a fake store
 * can't catch because it doesn't reimplement them independently.
 */

const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';
const fail = () => Promise.reject(new Error('boom'));
const succeed = () => Promise.resolve('ok');

describe('RedisStore (integration)', () => {
  let redis: Redis;
  let store: RedisStore;

  beforeAll(async () => {
    redis = new Redis(REDIS_URL, { maxRetriesPerRequest: 1, lazyConnect: true });
    try {
      await redis.connect();
      await redis.ping();
    } catch (error) {
      throw new Error(
        `Could not reach Redis at ${REDIS_URL}. Start it with "docker compose up -d" before running the integration suite.\n${error}`,
      );
    }
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(async () => {
    await redis.flushdb();
    store = new RedisStore(redis, { keyPrefix: 'redeye-test:' });
  });

  describe('get/set/del', () => {
    it('returns null for a missing key', async () => {
      await expect(store.get('missing')).resolves.toBeNull();
    });

    it('round-trips a JSON value and applies the TTL', async () => {
      await store.set('k', { a: 1, b: 'two' }, 30);
      await expect(store.get('k')).resolves.toEqual({ a: 1, b: 'two' });

      const ttl = await redis.ttl('redeye-test:k');
      expect(ttl).toBeGreaterThan(0);
      expect(ttl).toBeLessThanOrEqual(30);
    });

    it('deletes a key', async () => {
      await store.set('k', { a: 1 }, 30);
      await store.del('k');
      await expect(store.get('k')).resolves.toBeNull();
    });
  });

  describe('recordFailureAtomic', () => {
    it('increments failures and opens once the threshold is reached', async () => {
      const first = await store.recordFailureAtomic('op', { ttlSeconds: 30, failureThreshold: 3, now: Date.now() });
      expect(first.failures).toBe(1);
      expect(first.isOpen).toBe(false);

      const second = await store.recordFailureAtomic('op', { ttlSeconds: 30, failureThreshold: 3, now: Date.now() });
      expect(second.failures).toBe(2);
      expect(second.isOpen).toBe(false);

      const third = await store.recordFailureAtomic('op', { ttlSeconds: 30, failureThreshold: 3, now: Date.now() });
      expect(third.failures).toBe(3);
      expect(third.isOpen).toBe(true);
    });

    it('does not lose increments under concurrent callers (the reason this needs to be atomic)', async () => {
      await Promise.all(
        Array.from({ length: 25 }, () =>
          store.recordFailureAtomic('op', { ttlSeconds: 30, failureThreshold: 1000, now: Date.now() }),
        ),
      );

      const state = await store.get<{ failures: number }>('op');
      expect(state?.failures).toBe(25);
    });
  });

  describe('recordOutcomeAtomic', () => {
    it('folds outcomes into an EWMA error rate and reports openedNow exactly once', async () => {
      const opts = { decay: 0.5, minimumCalls: 2, errorRateThreshold: 0.5, ttlSeconds: 30, now: Date.now() };

      const r1 = await store.recordOutcomeAtomic('op', { ...opts, success: false });
      expect(r1.isOpen).toBe(false);
      expect(r1.openedNow).toBe(false);

      const r2 = await store.recordOutcomeAtomic('op', { ...opts, success: false });
      expect(r2.isOpen).toBe(true);
      expect(r2.openedNow).toBe(true);

      const r3 = await store.recordOutcomeAtomic('op', { ...opts, success: false });
      expect(r3.isOpen).toBe(true);
      expect(r3.openedNow).toBe(false);
    });
  });

  describe('claimTrial', () => {
    it('only the first caller claims the slot; it is released once the TTL expires', async () => {
      await expect(store.claimTrial('op:trial', 1)).resolves.toBe(true);
      await expect(store.claimTrial('op:trial', 1)).resolves.toBe(false);

      await new Promise((resolve) => setTimeout(resolve, 1100));

      await expect(store.claimTrial('op:trial', 1)).resolves.toBe(true);
    });
  });
});

describe('CircuitBreaker + RedisStore (integration)', () => {
  let redis: Redis;
  let store: RedisStore;

  beforeAll(async () => {
    redis = new Redis(REDIS_URL, { maxRetriesPerRequest: 1, lazyConnect: true });
    await redis.connect();
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(async () => {
    await redis.flushdb();
    store = new RedisStore(redis, { keyPrefix: 'redeye-test:' });
  });

  it('shares open state across breaker instances via real Redis', async () => {
    const breakerA = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 60000, store });
    const breakerB = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 60000, store });

    await expect(breakerA.execute('shared-op', fail)).rejects.toThrow('boom');
    await expect(breakerB.execute('shared-op', succeed)).rejects.toThrow(
      'Circuit breaker is open for operation: shared-op',
    );

    breakerA.destroy();
    breakerB.destroy();
  });

  it('only one of several concurrent callers across instances wins the half-open trial', async () => {
    const breakerA = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 30, store });
    const breakerB = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 30, store });

    await expect(breakerA.execute('op', fail)).rejects.toThrow('boom');
    await new Promise((resolve) => setTimeout(resolve, 60));

    const results = await Promise.allSettled([
      breakerA.execute('op', succeed),
      breakerA.execute('op', succeed),
      breakerB.execute('op', succeed),
      breakerB.execute('op', succeed),
    ]);

    const fulfilled = results.filter((r) => r.status === 'fulfilled');
    const rejected = results.filter((r) => r.status === 'rejected');
    expect(fulfilled).toHaveLength(1);
    expect(rejected).toHaveLength(3);
    for (const r of rejected as PromiseRejectedResult[]) {
      expect(r.reason.message).toMatch(/Circuit breaker is open/);
    }

    breakerA.destroy();
    breakerB.destroy();
  });

  it('backs off with a growing wait after each failed trial', async () => {
    const breaker = new CircuitBreaker({
      failureThreshold: 1,
      resetTimeout: 30,
      backoffMultiplier: 3,
      jitter: 0,
      store,
    });

    await expect(breaker.execute('op', fail)).rejects.toThrow(); // trip #1 -> effective 30ms
    await new Promise((resolve) => setTimeout(resolve, 50));
    await expect(breaker.execute('op', fail)).rejects.toThrow(); // trial fails -> openCount 1 -> effective 90ms

    await expect(breaker.execute('op', succeed)).rejects.toThrow('Circuit breaker is open');

    await new Promise((resolve) => setTimeout(resolve, 100));
    await expect(breaker.execute('op', succeed)).resolves.toBe('ok');

    breaker.destroy();
  });

  it('distributed errorRate strategy shares EWMA state across instances via real Lua scripts', async () => {
    const opts = {
      strategy: 'errorRate' as const,
      errorRateThreshold: 0.5,
      minimumCalls: 2,
      errorRateDecay: 0.5,
      resetTimeout: 60000,
      store,
    };
    const breakerA = new CircuitBreaker(opts);
    const breakerB = new CircuitBreaker(opts);

    await expect(breakerA.execute('shared-op', fail)).rejects.toThrow('boom');
    await expect(breakerA.execute('shared-op', fail)).rejects.toThrow('boom');

    await expect(breakerB.execute('shared-op', succeed)).rejects.toThrow(
      'Circuit breaker is open for operation: shared-op',
    );

    breakerA.destroy();
    breakerB.destroy();
  });
});
