import { Redis } from 'ioredis';
import { RedisStore } from '../src/stores/redis-store';
import { CircuitBreaker } from '../src/circuit-breaker';
import { TransitionEvent } from '../src/types';

const EVENTS_KEY = 'circuit_breaker:events';

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
      const first = await store.recordFailureAtomic('op', { ttlSeconds: 30, failureThreshold: 3, now: Date.now(), operation: 'op' });
      expect(first.failures).toBe(1);
      expect(first.isOpen).toBe(false);

      const second = await store.recordFailureAtomic('op', { ttlSeconds: 30, failureThreshold: 3, now: Date.now(), operation: 'op' });
      expect(second.failures).toBe(2);
      expect(second.isOpen).toBe(false);

      const third = await store.recordFailureAtomic('op', { ttlSeconds: 30, failureThreshold: 3, now: Date.now(), operation: 'op' });
      expect(third.failures).toBe(3);
      expect(third.isOpen).toBe(true);
    });

    it('does not lose increments under concurrent callers (the reason this needs to be atomic)', async () => {
      await Promise.all(
        Array.from({ length: 25 }, () =>
          store.recordFailureAtomic('op', { ttlSeconds: 30, failureThreshold: 1000, now: Date.now(), operation: 'op' }),
        ),
      );

      const state = await store.get<{ failures: number }>('op');
      expect(state?.failures).toBe(25);
    });
  });

  describe('recordOutcomeAtomic', () => {
    it('folds outcomes into an EWMA error rate and reports openedNow exactly once', async () => {
      const opts = { decay: 0.5, minimumCalls: 2, errorRateThreshold: 0.5, ttlSeconds: 30, now: Date.now(), operation: 'op' };

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

  describe('claimTrial / releaseTrial', () => {
    it('only the first caller claims the slot, and gets a distinct token each time; it is released once the TTL expires', async () => {
      const tokenA = await store.claimTrial('op:trial', 1);
      expect(typeof tokenA).toBe('string');
      await expect(store.claimTrial('op:trial', 1)).resolves.toBeNull();

      await new Promise((resolve) => setTimeout(resolve, 1100));

      const tokenB = await store.claimTrial('op:trial', 1);
      expect(typeof tokenB).toBe('string');
      expect(tokenB).not.toBe(tokenA);
    });

    it('releaseTrial only releases the claim matching its own token (compare-and-delete)', async () => {
      const staleToken = await store.claimTrial('op:trial', 1);
      expect(staleToken).not.toBeNull();

      // Simulate the real-world sequence from the trial-TTL-expiry limitation:
      // the original claim's TTL elapses, and a different instance claims a
      // fresh trial on the same key before the original caller's (slow) call
      // ever gets around to releasing its now-stale claim.
      await new Promise((resolve) => setTimeout(resolve, 1100));
      const freshToken = await store.claimTrial('op:trial', 30);
      expect(freshToken).not.toBeNull();
      expect(freshToken).not.toBe(staleToken);

      // The original caller finally releases with its stale token. This must
      // not delete the fresh claim -- an unconditional DEL would, a
      // compare-and-delete must not.
      await store.releaseTrial('op:trial', staleToken as string);

      await expect(store.claimTrial('op:trial', 30)).resolves.toBeNull(); // still held by freshToken
    });
  });

  describe('closeAtomic', () => {
    it('is a genuine no-op (no write, version stays implicit 0) when the state was already clean', async () => {
      const prior = await store.closeAtomic('op', EVENTS_KEY, { ttlSeconds: 30, operation: 'op' });
      expect(prior.isOpen).toBe(false);
      expect(prior.version).toBe(0);
      await expect(store.get('op')).resolves.toBeNull(); // nothing was ever written
    });

    it('writes CLOSED_STATE with a bumped version and publishes a transition when there was real state to clear', async () => {
      await store.recordFailureAtomic('op', { ttlSeconds: 30, failureThreshold: 5, now: Date.now(), operation: 'op' });

      const closed = await store.closeAtomic('op', EVENTS_KEY, { ttlSeconds: 30, operation: 'op' });
      expect(closed).toMatchObject({ isOpen: false, failures: 0, version: 1 });

      const entries = await redis.xrange('redeye-test:' + EVENTS_KEY, '-', '+');
      expect(entries).toHaveLength(1);
    });
  });

  describe('reopenTrialFailureAtomic', () => {
    it('reopens with a bumped openCount/version, floors failures at the threshold, and publishes a transition each time', async () => {
      const first = await store.reopenTrialFailureAtomic('op', EVENTS_KEY, {
        ttlSeconds: 30,
        failureThreshold: 5,
        now: Date.now(),
        operation: 'op',
      });
      expect(first).toMatchObject({ isOpen: true, openCount: 1, failures: 5, version: 1 });

      const second = await store.reopenTrialFailureAtomic('op', EVENTS_KEY, {
        ttlSeconds: 30,
        failureThreshold: 5,
        now: Date.now(),
        operation: 'op',
      });
      expect(second).toMatchObject({ openCount: 2, version: 2 });

      const entries = await redis.xrange('redeye-test:' + EVENTS_KEY, '-', '+');
      expect(entries).toHaveLength(2);
    });
  });

  describe('subscribeTransitions', () => {
    it('delivers a decoded event for each published transition, and not for a plain sub-threshold failure increment', async () => {
      const events: TransitionEvent[] = [];
      const unsubscribe = await store.subscribeTransitions(EVENTS_KEY, (event) => {
        if (event) events.push(event);
      });

      // subscribeTransitions() resolves once the duplicated connection is
      // created, not once its blocking XREAD is actually registered on the
      // server -- writing immediately after can race ahead of it (a write
      // before the XREAD is blocked is simply never seen, since XREAD
      // BLOCK STREAMS key $ only sees entries *after* it starts blocking).
      // Give it a moment to actually be listening before publishing.
      await new Promise((resolve) => setTimeout(resolve, 300));

      // Sub-threshold: no isOpen flip, so no transition should be published.
      await store.recordFailureAtomic('op', { ttlSeconds: 30, failureThreshold: 5, now: Date.now(), operation: 'op' });
      // Crosses the threshold: a real transition.
      for (let i = 0; i < 4; i++) {
        await store.recordFailureAtomic('op', { ttlSeconds: 30, failureThreshold: 5, now: Date.now(), operation: 'op' });
      }

      await new Promise((resolve) => setTimeout(resolve, 200)); // let the blocking XREAD pick it up

      expect(events).toHaveLength(1);
      expect(events[0]).toMatchObject({ operation: 'op', isOpen: true, version: 1 });

      await unsubscribe();
    });

    it('invokes the handler with null and keeps delivering after the underlying connection is killed and reconnects', async () => {
      const events: (TransitionEvent | null)[] = [];
      const unsubscribe = await store.subscribeTransitions(EVENTS_KEY, (event) => {
        events.push(event);
      });

      await new Promise((resolve) => setTimeout(resolve, 100)); // let CLIENT SETNAME land

      const list = (await redis.client('LIST')) as unknown as string;
      const subscriberLine = list.split('\n').find((line) => line.includes('name=redeye-subscriber'));
      const idMatch = subscriberLine?.match(/id=(\d+)/);
      expect(idMatch).toBeTruthy();
      await redis.client('KILL', 'ID', idMatch![1]);

      // The read loop's next XREAD fails, delivering a null "distrust
      // everything" signal, then retries with backoff and reconnects.
      await new Promise((resolve) => setTimeout(resolve, 1500));
      expect(events).toContain(null);

      // Once reconnected, a fresh transition is still delivered.
      await store.recordFailureAtomic('op', { ttlSeconds: 30, failureThreshold: 1, now: Date.now(), operation: 'op' });
      await new Promise((resolve) => setTimeout(resolve, 200));
      expect(events.some((e) => e && e.operation === 'op')).toBe(true);

      await unsubscribe();
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

describe('CircuitBreaker + RedisStore, localCache (integration)', () => {
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

  it('propagates a trip from breaker A to breaker B within staleToleranceMs plus a generous delivery bound', async () => {
    const opts = { failureThreshold: 1, resetTimeout: 60000, store, localCache: { staleToleranceMs: 100 } };
    const breakerA = new CircuitBreaker(opts);
    const breakerB = new CircuitBreaker(opts);

    await breakerB.execute('op', succeed); // warms B's closed-state cache and its subscriber connection
    await new Promise((resolve) => setTimeout(resolve, 150)); // let the subscription actually establish

    const start = Date.now();
    await expect(breakerA.execute('op', fail)).rejects.toThrow('boom'); // trips it

    let elapsed = 0;
    for (;;) {
      elapsed = Date.now() - start;
      try {
        await expect(breakerB.execute('op', succeed)).rejects.toThrow('Circuit breaker is open for operation: op');
        break;
      } catch {
        if (elapsed > 250) throw new Error(`breaker B did not observe the trip within 250ms (waited ${elapsed}ms)`);
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
    }
    expect(elapsed).toBeLessThan(250); // staleToleranceMs (100) + a generous delivery bound

    breakerA.destroy();
    breakerB.destroy();
  });

  it('B still catches the trip within its poll interval after its subscriber connection is killed', async () => {
    const breakerA = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 60000, store, localCache: { staleToleranceMs: 100 } });
    const breakerB = new CircuitBreaker({
      failureThreshold: 1,
      resetTimeout: 60000,
      store,
      localCache: { staleToleranceMs: 100 },
      monitorInterval: 1000,
    });

    await breakerB.execute('op', succeed); // warms B's cache and its subscriber connection
    await new Promise((resolve) => setTimeout(resolve, 150));

    const list = (await redis.client('LIST')) as unknown as string;
    const subscriberLine = list.split('\n').find((line) => line.includes('name=redeye-subscriber'));
    const idMatch = subscriberLine?.match(/id=(\d+)/);
    expect(idMatch).toBeTruthy(); // if this fails, the subscriber never connected -- not a reconnect-behavior failure
    await redis.client('KILL', 'ID', idMatch![1]);

    await expect(breakerA.execute('op', fail)).rejects.toThrow('boom'); // trips it while B's subscriber is down

    await new Promise((resolve) => setTimeout(resolve, 1500)); // past B's 1s poll interval

    await expect(breakerB.execute('op', succeed)).rejects.toThrow('Circuit breaker is open for operation: op');

    breakerA.destroy();
    breakerB.destroy();
  });

  it('XTRIM keeps the shared events stream bounded across more than 1,000 transitions', async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 1, jitter: 0, store });

    // Each operation flips isOpen exactly twice per iteration (open, then
    // close on the next successful trial), so this produces > 1,000
    // transitions -- and therefore > 1,000 XADDs -- across a modest number
    // of iterations and distinct operation names, without waiting on
    // resetTimeout in real time (it's 1ms).
    for (let i = 0; i < 600; i++) {
      const op = `op-${i % 20}`;
      await expect(breaker.execute(op, fail)).rejects.toThrow();
      await new Promise((resolve) => setTimeout(resolve, 2));
      await expect(breaker.execute(op, succeed)).resolves.toBe('ok');
    }

    const length = await redis.xlen('redeye-test:' + EVENTS_KEY);
    expect(length).toBeLessThanOrEqual(1100); // MAXLEN ~ 1000, approximate trim allows some slack
    expect(length).toBeGreaterThan(0);

    breaker.destroy();
  });
});
