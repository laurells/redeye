import { CircuitBreaker, StoreUnavailableError } from '../src/circuit-breaker';
import { Store } from '../src/store';
import { CircuitBreakerState } from '../src/types';

class InMemoryStore implements Store {
  private data = new Map<string, unknown>();

  async get<T>(key: string): Promise<T | null> {
    return (this.data.get(key) as T) ?? null;
  }

  async set<T>(key: string, value: T): Promise<void> {
    this.data.set(key, value);
  }

  async del(key: string): Promise<void> {
    this.data.delete(key);
  }
}

/** No atomic capabilities (forces the non-atomic fallback path), and can be told to fail exactly its next `get()` call, to simulate a transient store blip on one specific read. */
class FlakyGateStore extends InMemoryStore {
  private failNextGet = false;

  triggerNextGetFailure(): void {
    this.failNextGet = true;
  }

  async get<T>(key: string): Promise<T | null> {
    if (this.failNextGet) {
      this.failNextGet = false;
      throw new Error('transient blip');
    }
    return super.get<T>(key);
  }
}

/** Implements the optional atomic capabilities, the way RedisStore does. */
class AtomicInMemoryStore implements Store {
  private data = new Map<string, unknown>();

  async get<T>(key: string): Promise<T | null> {
    return (this.data.get(key) as T) ?? null;
  }

  async set<T>(key: string, value: T): Promise<void> {
    this.data.set(key, value);
  }

  async del(key: string): Promise<void> {
    this.data.delete(key);
  }

  async recordFailureAtomic(
    key: string,
    opts: { ttlSeconds: number; failureThreshold: number; now: number },
  ): Promise<CircuitBreakerState> {
    const current = (this.data.get(key) as CircuitBreakerState) ?? {
      failures: 0,
      lastFailure: 0,
      isOpen: false,
      openCount: 0,
      errorRate: 0,
      sampleCount: 0,
    };
    const next: CircuitBreakerState = {
      failures: current.failures + 1,
      lastFailure: opts.now,
      isOpen: current.failures + 1 >= opts.failureThreshold,
      openCount: current.openCount ?? 0,
      errorRate: current.errorRate ?? 0,
      sampleCount: current.sampleCount ?? 0,
    };
    this.data.set(key, next);
    return next;
  }

  async recordOutcomeAtomic(
    key: string,
    opts: { success: boolean; decay: number; minimumCalls: number; errorRateThreshold: number; now: number },
  ): Promise<CircuitBreakerState & { openedNow: boolean }> {
    const current = (this.data.get(key) as CircuitBreakerState) ?? {
      failures: 0,
      lastFailure: 0,
      isOpen: false,
      openCount: 0,
      errorRate: 0,
      sampleCount: 0,
    };
    const errorRate = current.errorRate * opts.decay + (opts.success ? 0 : 1) * (1 - opts.decay);
    const sampleCount = current.sampleCount + 1;
    const isOpen = sampleCount >= opts.minimumCalls && errorRate >= opts.errorRateThreshold;
    const next: CircuitBreakerState = {
      failures: current.failures,
      lastFailure: opts.now,
      isOpen,
      openCount: current.openCount,
      errorRate,
      sampleCount,
    };
    this.data.set(key, next);
    return { ...next, openedNow: isOpen && !current.isOpen };
  }

  async claimTrial(key: string): Promise<string | null> {
    if (this.data.has(key)) return null;
    const token = `token-${Math.random().toString(36).slice(2)}`;
    this.data.set(key, token);
    return token;
  }

  async releaseTrial(key: string, token: string): Promise<void> {
    if (this.data.get(key) === token) {
      this.data.delete(key);
    }
  }
}

/** Implements the atomic capabilities plus claimTrial, but can be told to fail exactly its next claimTrial() call, to simulate a transient error on the claim itself. */
class FlakyClaimStore extends AtomicInMemoryStore {
  private throwNextClaim = false;

  triggerNextClaimFailure(): void {
    this.throwNextClaim = true;
  }

  async claimTrial(key: string): Promise<string | null> {
    if (this.throwNextClaim) {
      this.throwNextClaim = false;
      throw new Error('transient claim blip');
    }
    return super.claimTrial(key);
  }
}

/** Implements claimTrial (a real claim happens) but not releaseTrial, simulating an incomplete custom Store. */
class ClaimOnlyStore implements Store {
  private data = new Map<string, unknown>();

  async get<T>(key: string): Promise<T | null> {
    return (this.data.get(key) as T) ?? null;
  }

  async set<T>(key: string, value: T): Promise<void> {
    this.data.set(key, value);
  }

  async del(key: string): Promise<void> {
    this.data.delete(key);
  }

  async claimTrial(key: string): Promise<string | null> {
    if (this.data.has(key)) return null;
    const token = `token-${Math.random().toString(36).slice(2)}`;
    this.data.set(key, token);
    return token;
  }
}

class UnreachableStore implements Store {
  async get<T>(): Promise<T | null> {
    throw new Error('ECONNREFUSED');
  }
  async set<T>(): Promise<void> {
    throw new Error('ECONNREFUSED');
  }
  async del(): Promise<void> {
    throw new Error('ECONNREFUSED');
  }
}

const fail = () => Promise.reject(new Error('boom'));
const succeed = () => Promise.resolve('ok');

describe('CircuitBreaker (local mode)', () => {
  it('opens after failureThreshold consecutive failures', async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 3, resetTimeout: 60000 });

    for (let i = 0; i < 3; i++) {
      await expect(breaker.execute('op', fail)).rejects.toThrow('boom');
    }

    await expect(breaker.execute('op', succeed)).rejects.toThrow('Circuit breaker is open for operation: op');
    breaker.destroy();
  });

  it('resets failure count on success', async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 3, resetTimeout: 60000 });

    await expect(breaker.execute('op', fail)).rejects.toThrow();
    await expect(breaker.execute('op', fail)).rejects.toThrow();
    await breaker.execute('op', succeed);

    const state = await breaker.getState('op');
    expect(state.failures).toBe(0);
    expect(state.isOpen).toBe(false);
    breaker.destroy();
  });

  it('allows a trial request after resetTimeout elapses', async () => {
    jest.useFakeTimers();
    try {
      const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 1000, jitter: 0 });

      await expect(breaker.execute('op', fail)).rejects.toThrow();
      expect((await breaker.getState('op')).isOpen).toBe(true);

      jest.advanceTimersByTime(1001);

      await expect(breaker.execute('op', succeed)).resolves.toBe('ok');
      breaker.destroy();
    } finally {
      jest.useRealTimers();
    }
  });

  it('tracks separate breakers per operation name', async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });

    await expect(breaker.execute('op-a', fail)).rejects.toThrow();
    await expect(breaker.execute('op-b', succeed)).resolves.toBe('ok');
    breaker.destroy();
  });
});

describe('CircuitBreaker (jitter)', () => {
  it('rolls jitter once per open episode, not on every gate check', async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 1000, jitter: 0.5 });
    await expect(breaker.execute('op', fail)).rejects.toThrow();

    const randomSpy = jest.spyOn(Math, 'random');
    breaker.canExecute('op');
    breaker.canExecute('op');
    breaker.canExecute('op');
    expect(randomSpy).toHaveBeenCalledTimes(1);

    randomSpy.mockRestore();
    breaker.destroy();
  });

  it('rolls a fresh jitter value after a failed trial (new episode, new openCount)', async () => {
    // resetTimeout 30ms + jitter 0.5 means the effective timeout for episode
    // 1 (openCount 0) is at most 45ms; wait comfortably past that so the
    // trial attempt below is never blocked by an unlucky jitter roll.
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 30, jitter: 0.5 });
    await expect(breaker.execute('op', fail)).rejects.toThrow();
    await new Promise((resolve) => setTimeout(resolve, 60));

    const randomSpy = jest.spyOn(Math, 'random');
    await expect(breaker.execute('op', fail)).rejects.toThrow(); // trial fails -> new episode (openCount 1)
    expect(randomSpy).toHaveBeenCalledTimes(1); // rolled once for episode 1, during the trial's own gate claim

    randomSpy.mockClear();
    breaker.canExecute('op'); // first check of episode 2 -> cache miss -> rolls once
    breaker.canExecute('op'); // second check of episode 2 -> cache hit -> no additional roll
    expect(randomSpy).toHaveBeenCalledTimes(1);

    randomSpy.mockRestore();
    breaker.destroy();
  });
});

describe('CircuitBreaker (distributed mode)', () => {
  it('shares open state across breaker instances via the store', async () => {
    const store = new InMemoryStore();
    const breakerA = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 60000, store });
    const breakerB = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 60000, store });

    await expect(breakerA.execute('shared-op', fail)).rejects.toThrow('boom');

    await expect(breakerB.execute('shared-op', succeed)).rejects.toThrow(
      'Circuit breaker is open for operation: shared-op',
    );

    breakerA.destroy();
    breakerB.destroy();
  });

  it('calls onStateChange when the breaker opens', async () => {
    const store = new InMemoryStore();
    const onStateChange = jest.fn();
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 60000, store, onStateChange });

    await expect(breaker.execute('op', fail)).rejects.toThrow();

    expect(onStateChange).toHaveBeenCalledWith('open', 'op');
    breaker.destroy();
  });
});

describe('CircuitBreaker (store unavailable)', () => {
  it('fails open by default: lets calls through and does not misattribute the store error as an operation failure', async () => {
    const store = new UnreachableStore();
    const onStoreError = jest.fn();
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 60000, store, onStoreError });

    await expect(breaker.execute('op', succeed)).resolves.toBe('ok');
    expect(onStoreError).toHaveBeenCalled();
    breaker.destroy();
  });

  it('still lets the wrapped call run and its real failure propagate, even though the store write afterwards fails silently', async () => {
    const store = new UnreachableStore();
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 60000, store });

    await expect(breaker.execute('op', fail)).rejects.toThrow('boom');
    breaker.destroy();
  });

  it('fails closed when configured: throws StoreUnavailableError without calling the wrapped function', async () => {
    const store = new UnreachableStore();
    const fn = jest.fn(succeed);
    const breaker = new CircuitBreaker({
      failureThreshold: 1,
      resetTimeout: 60000,
      store,
      failOpenOnStoreError: false,
    });

    await expect(breaker.execute('op', fn)).rejects.toBeInstanceOf(StoreUnavailableError);
    expect(fn).not.toHaveBeenCalled();
    breaker.destroy();
  });

  it('canExecuteAsync respects failOpenOnStoreError', async () => {
    const store = new UnreachableStore();
    const failOpenBreaker = new CircuitBreaker({ store });
    const failClosedBreaker = new CircuitBreaker({ store, failOpenOnStoreError: false });

    await expect(failOpenBreaker.canExecuteAsync('op')).resolves.toBe(true);
    await expect(failClosedBreaker.canExecuteAsync('op')).rejects.toBeInstanceOf(StoreUnavailableError);

    failOpenBreaker.destroy();
    failClosedBreaker.destroy();
  });

  it('canExecute warns once in distributed mode instead of silently returning a meaningless true', async () => {
    const store = new InMemoryStore();
    const logger = { warn: jest.fn(), log: jest.fn() };
    const breaker = new CircuitBreaker({ store, logger });

    expect(breaker.canExecute('op-a')).toBe(true);
    expect(breaker.canExecute('op-b')).toBe(true);

    const canExecuteWarnings = logger.warn.mock.calls.filter((call) => String(call[0]).includes('canExecute()'));
    expect(canExecuteWarnings).toHaveLength(1);
    breaker.destroy();
  });
});

describe('CircuitBreaker (atomic distributed store)', () => {
  it('uses recordFailureAtomic instead of get-then-set when the store provides it', async () => {
    const store = new AtomicInMemoryStore();
    const spy = jest.spyOn(store, 'recordFailureAtomic');
    const breaker = new CircuitBreaker({ failureThreshold: 3, resetTimeout: 60000, store });

    await expect(breaker.execute('op', fail)).rejects.toThrow('boom');

    expect(spy).toHaveBeenCalledTimes(1);
    breaker.destroy();
  });

  it('only one of several concurrent callers wins the half-open trial after resetTimeout elapses; the rest stay blocked', async () => {
    const store = new AtomicInMemoryStore();
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 30, store });

    await expect(breaker.execute('op', fail)).rejects.toThrow('boom');
    await new Promise((resolve) => setTimeout(resolve, 40));

    const results = await Promise.allSettled([
      breaker.execute('op', succeed),
      breaker.execute('op', succeed),
      breaker.execute('op', succeed),
    ]);

    const fulfilled = results.filter((r) => r.status === 'fulfilled');
    const rejected = results.filter((r) => r.status === 'rejected');
    expect(fulfilled).toHaveLength(1);
    expect(rejected).toHaveLength(2);
    for (const r of rejected as PromiseRejectedResult[]) {
      expect(r.reason.message).toMatch(/Circuit breaker is open/);
    }

    breaker.destroy();
  });

  it('fully closes after a successful trial, letting subsequent calls through normally', async () => {
    const store = new AtomicInMemoryStore();
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 30, store });

    await expect(breaker.execute('op', fail)).rejects.toThrow();
    await new Promise((resolve) => setTimeout(resolve, 40));

    await expect(breaker.execute('op', succeed)).resolves.toBe('ok');
    await expect(breaker.execute('op', succeed)).resolves.toBe('ok');

    const state = await breaker.getState('op');
    expect(state.isOpen).toBe(false);
    expect(state.openCount).toBe(0);
    breaker.destroy();
  });

  it('fires onStateChange with half-open when a caller claims the trial slot via claimTrial', async () => {
    const store = new AtomicInMemoryStore();
    const onStateChange = jest.fn();
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 30, store, onStateChange });

    await expect(breaker.execute('op', fail)).rejects.toThrow();
    await new Promise((resolve) => setTimeout(resolve, 40));

    await expect(breaker.execute('op', succeed)).resolves.toBe('ok');

    const order = onStateChange.mock.calls.map((call) => call[0]);
    expect(order).toEqual(['open', 'half-open', 'closed']);
    breaker.destroy();
  });

  it('backs off with a growing wait after each failed trial', async () => {
    const store = new AtomicInMemoryStore();
    const breaker = new CircuitBreaker({
      failureThreshold: 1,
      resetTimeout: 30,
      backoffMultiplier: 3,
      jitter: 0,
      store,
    });

    await expect(breaker.execute('op', fail)).rejects.toThrow(); // trip #1 (openCount 0 -> effective 30ms)
    await new Promise((resolve) => setTimeout(resolve, 40));
    await expect(breaker.execute('op', fail)).rejects.toThrow(); // trial fails -> openCount 1 -> effective 90ms

    // Immediately after, well within the 90ms backoff window: still blocked.
    await expect(breaker.execute('op', succeed)).rejects.toThrow('Circuit breaker is open');

    // After the backed-off window elapses, a trial is allowed again.
    await new Promise((resolve) => setTimeout(resolve, 100));
    await expect(breaker.execute('op', succeed)).resolves.toBe('ok');

    breaker.destroy();
  });

  it('warns once when the store does not implement the atomic capabilities', async () => {
    const store = new InMemoryStore();
    const logger = { warn: jest.fn(), log: jest.fn() };
    const breaker = new CircuitBreaker({ failureThreshold: 5, resetTimeout: 60000, store, logger });

    await expect(breaker.execute('op-a', fail)).rejects.toThrow();
    await expect(breaker.execute('op-b', fail)).rejects.toThrow();

    const capabilityWarnings = logger.warn.mock.calls.filter((call) => String(call[0]).includes('recordFailureAtomic'));
    expect(capabilityWarnings).toHaveLength(1);
    breaker.destroy();
  });

  it('fires onStateChange with open again when a distributed half-open trial fails, not silence', async () => {
    const store = new AtomicInMemoryStore();
    const onStateChange = jest.fn();
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 30, store, onStateChange });

    await expect(breaker.execute('op', fail)).rejects.toThrow();
    await new Promise((resolve) => setTimeout(resolve, 40));

    await expect(breaker.execute('op', fail)).rejects.toThrow(); // this is the trial, and it fails too

    const order = onStateChange.mock.calls.map((call) => call[0]);
    expect(order).toEqual(['open', 'half-open', 'open']);
    breaker.destroy();
  });

  it('recordSuccess fires onStateChange closed in distributed mode too, matching local mode', async () => {
    const store = new AtomicInMemoryStore();
    const onStateChange = jest.fn();
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 60000, store, onStateChange });

    await expect(breaker.execute('op', fail)).rejects.toThrow();
    expect(await breaker.getState('op')).toMatchObject({ isOpen: true });

    breaker.recordSuccess('op');
    await new Promise((resolve) => setImmediate(resolve)); // recordSuccess is fire-and-forget

    expect(await breaker.getState('op')).toMatchObject({ isOpen: false });
    expect(onStateChange).toHaveBeenCalledWith('closed', 'op');
    breaker.destroy();
  });

  it('does not wipe accumulated backoff (openCount) when a closed-phase failure lands via the non-atomic fallback after a fail-open gate blip', async () => {
    const store = new FlakyGateStore();
    // openCacheRefreshMs: 0 -- this test exercises a store read that's
    // forced to fail; the open-state cache would otherwise serve the gate
    // check locally (correctly) and the triggered failure would never be
    // consumed by a real read.
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 30, jitter: 0, store, openCacheRefreshMs: 0 });

    await expect(breaker.execute('op', fail)).rejects.toThrow(); // trip #1 -> openCount 0
    await new Promise((resolve) => setTimeout(resolve, 40));
    await expect(breaker.execute('op', fail)).rejects.toThrow(); // trial fails -> openCount 1

    expect((await breaker.getState('op')).openCount).toBe(1);

    // Simulate a transient store blip on exactly the next gate read: the
    // call proceeds fail-open even though the breaker is really still open
    // elsewhere, the wrapped fn fails for real, and the failure is recorded
    // via the non-atomic fallback (this store has no recordFailureAtomic).
    store.triggerNextGetFailure();
    await expect(breaker.execute('op', fail)).rejects.toThrow('boom');

    expect((await breaker.getState('op')).openCount).toBe(1); // preserved, not reset to 0
    breaker.destroy();
  });

  it('does not delete the trial key at all when releasing a tokenless trial (claimTrial errored) -- lets the TTL clean up instead of risking a stale delete of someone else\'s claim', async () => {
    const store = new FlakyClaimStore();
    const delSpy = jest.spyOn(store, 'del');
    const releaseSpy = jest.spyOn(store, 'releaseTrial');
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 30, store });

    await expect(breaker.execute('op', fail)).rejects.toThrow();
    await new Promise((resolve) => setTimeout(resolve, 40));

    // Force the upcoming claimTrial call to throw, so this call proceeds as
    // a tokenless trial (fail-open on the trial itself) without ever
    // actually writing a claim to the store.
    store.triggerNextClaimFailure();
    await expect(breaker.execute('op', succeed)).resolves.toBe('ok');

    expect(delSpy).not.toHaveBeenCalled();
    expect(releaseSpy).not.toHaveBeenCalled();
    breaker.destroy();
  });

  it('warns once when the store implements claimTrial but not releaseTrial, and does not fall back to an unconditional delete', async () => {
    const store = new ClaimOnlyStore();
    const delSpy = jest.spyOn(store, 'del');
    const logger = { warn: jest.fn(), log: jest.fn() };
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 30, store, logger });

    await expect(breaker.execute('op', fail)).rejects.toThrow();
    await new Promise((resolve) => setTimeout(resolve, 40));

    await expect(breaker.execute('op', succeed)).resolves.toBe('ok'); // real claim via claimTrial, but no releaseTrial to release it with

    const releaseTrialWarnings = logger.warn.mock.calls.filter((call) => String(call[0]).includes('releaseTrial'));
    expect(releaseTrialWarnings).toHaveLength(1);
    expect(delSpy).not.toHaveBeenCalled();
    breaker.destroy();
  });
});

describe('CircuitBreaker (half-open, local mode)', () => {
  it('only one of several concurrent in-flight calls gets the trial; the rest stay blocked', async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 30 });

    await expect(breaker.execute('op', fail)).rejects.toThrow();
    await new Promise((resolve) => setTimeout(resolve, 40));

    let releaseTrial!: () => void;
    const slowTrial = () => new Promise<string>((resolve) => { releaseTrial = () => resolve('ok'); });

    const trial = breaker.execute('op', slowTrial);
    // Give the trial call a chance to claim the slot before firing more concurrent calls.
    await new Promise((resolve) => setImmediate(resolve));

    const blocked = await Promise.allSettled([breaker.execute('op', succeed), breaker.execute('op', succeed)]);
    expect(blocked.every((r) => r.status === 'rejected')).toBe(true);

    releaseTrial();
    await expect(trial).resolves.toBe('ok');
    breaker.destroy();
  });

  it('fires onStateChange with half-open when a caller claims the trial slot', async () => {
    const onStateChange = jest.fn();
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 30, onStateChange });

    await expect(breaker.execute('op', fail)).rejects.toThrow();
    expect(onStateChange).toHaveBeenCalledWith('open', 'op');
    await new Promise((resolve) => setTimeout(resolve, 40));

    await expect(breaker.execute('op', succeed)).resolves.toBe('ok');
    expect(onStateChange).toHaveBeenCalledWith('half-open', 'op');
    expect(onStateChange).toHaveBeenCalledWith('closed', 'op');

    const order = onStateChange.mock.calls.map((call) => call[0]);
    expect(order).toEqual(['open', 'half-open', 'closed']);
    breaker.destroy();
  });

  it('fires onStateChange with open again when a half-open trial fails, not silence', async () => {
    const onStateChange = jest.fn();
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 30, onStateChange });

    await expect(breaker.execute('op', fail)).rejects.toThrow();
    await new Promise((resolve) => setTimeout(resolve, 40));

    await expect(breaker.execute('op', fail)).rejects.toThrow(); // this is the trial, and it fails too

    const order = onStateChange.mock.calls.map((call) => call[0]);
    expect(order).toEqual(['open', 'half-open', 'open']);
    breaker.destroy();
  });
});

describe('CircuitBreaker (metrics)', () => {
  it('tracks calls, successes, failures, and rejections per operation', async () => {
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 60000 });

    await expect(breaker.execute('op', fail)).rejects.toThrow();
    await expect(breaker.execute('op', succeed)).rejects.toThrow('Circuit breaker is open');

    const metrics = breaker.getMetrics('op');
    expect(metrics.totalCalls).toBe(2);
    expect(metrics.totalFailures).toBe(1);
    expect(metrics.totalRejections).toBe(1);
    expect(metrics.totalSuccesses).toBe(0);
    breaker.destroy();
  });
});

describe('CircuitBreaker (errorRate strategy)', () => {
  it('trips on a flapping dependency that the consecutive strategy would never catch', async () => {
    // 80% failure rate, but never more than 4 failures in a row before a
    // success resets the streak — a 'consecutive' breaker with
    // failureThreshold > 4 never accumulates enough in a row to trip.
    const consecutiveBreaker = new CircuitBreaker({ strategy: 'consecutive', failureThreshold: 5, resetTimeout: 60000 });
    const errorRateBreaker = new CircuitBreaker({
      strategy: 'errorRate',
      errorRateThreshold: 0.5,
      minimumCalls: 10,
      resetTimeout: 60000,
    });

    const pattern = [fail, fail, fail, fail, succeed];
    let consecutiveOpened = false;
    let errorRateOpened = false;

    for (let i = 0; i < 40; i++) {
      const op = pattern[i % pattern.length];

      if (!consecutiveOpened) {
        try {
          await consecutiveBreaker.execute('op', op);
        } catch (e) {
          if ((e as Error).message.includes('Circuit breaker is open')) consecutiveOpened = true;
        }
      }

      if (!errorRateOpened) {
        try {
          await errorRateBreaker.execute('op', op);
        } catch (e) {
          if ((e as Error).message.includes('Circuit breaker is open')) errorRateOpened = true;
        }
      }
    }

    expect(consecutiveOpened).toBe(false);
    expect(errorRateOpened).toBe(true);

    consecutiveBreaker.destroy();
    errorRateBreaker.destroy();
  });

  it('does not trip below minimumCalls even at a 100% failure rate', async () => {
    const breaker = new CircuitBreaker({ strategy: 'errorRate', errorRateThreshold: 0.5, minimumCalls: 10, resetTimeout: 60000 });

    for (let i = 0; i < 9; i++) {
      await expect(breaker.execute('op', fail)).rejects.toThrow('boom');
    }

    const state = await breaker.getState('op');
    expect(state.isOpen).toBe(false);
    expect(state.sampleCount).toBe(9);
    breaker.destroy();
  });

  it('getState reports errorRate and sampleCount', async () => {
    const breaker = new CircuitBreaker({ strategy: 'errorRate', errorRateThreshold: 0.9, minimumCalls: 100, resetTimeout: 60000 });

    await expect(breaker.execute('op', fail)).rejects.toThrow('boom');
    await expect(breaker.execute('op', succeed)).resolves.toBe('ok');

    const state = await breaker.getState('op');
    expect(state.sampleCount).toBe(2);
    expect(state.errorRate).toBeGreaterThan(0);
    expect(state.errorRate).toBeLessThan(1);
    breaker.destroy();
  });

  it('distributed mode: shares error-rate state across instances via recordOutcomeAtomic', async () => {
    const store = new AtomicInMemoryStore();
    const spy = jest.spyOn(store, 'recordOutcomeAtomic');
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

    expect(spy).toHaveBeenCalledTimes(2);
    await expect(breakerB.execute('shared-op', succeed)).rejects.toThrow('Circuit breaker is open for operation: shared-op');

    breakerA.destroy();
    breakerB.destroy();
  });
});

describe('CircuitBreaker (dynamic operation name warnings)', () => {
  it('warns once when an operation name contains a slash', async () => {
    const logger = { warn: jest.fn(), log: jest.fn() };
    const breaker = new CircuitBreaker({ failureThreshold: 5, resetTimeout: 60000, logger });

    await breaker.execute('/api/users/123', succeed);
    await breaker.execute('/api/users/456', succeed);

    const dynamicNameWarnings = logger.warn.mock.calls.filter((call) => String(call[0]).includes('looks dynamic'));
    expect(dynamicNameWarnings).toHaveLength(1);
    expect(dynamicNameWarnings[0][0]).toContain('contains "/"');
    breaker.destroy();
  });

  it('warns once when an operation name is over 100 characters', async () => {
    const logger = { warn: jest.fn(), log: jest.fn() };
    const breaker = new CircuitBreaker({ failureThreshold: 5, resetTimeout: 60000, logger });
    const longName = 'x'.repeat(150);

    await breaker.execute(longName, succeed);

    const dynamicNameWarnings = logger.warn.mock.calls.filter((call) => String(call[0]).includes('looks dynamic'));
    expect(dynamicNameWarnings).toHaveLength(1);
    expect(dynamicNameWarnings[0][0]).toContain('150 chars');
    breaker.destroy();
  });

  it('does not warn for a normal, fixed operation name', async () => {
    const logger = { warn: jest.fn(), log: jest.fn() };
    const breaker = new CircuitBreaker({ failureThreshold: 5, resetTimeout: 60000, logger });

    await breaker.execute('payment-gateway', succeed);

    const dynamicNameWarnings = logger.warn.mock.calls.filter((call) => String(call[0]).includes('looks dynamic'));
    expect(dynamicNameWarnings).toHaveLength(0);
    breaker.destroy();
  });

  it('warns once via recordFailure/recordSuccess too, not just execute', async () => {
    const logger = { warn: jest.fn(), log: jest.fn() };
    const breaker = new CircuitBreaker({ failureThreshold: 5, resetTimeout: 60000, logger });

    breaker.recordFailure('/tenants/42/checkout');
    breaker.recordSuccess('/tenants/42/checkout');

    const dynamicNameWarnings = logger.warn.mock.calls.filter((call) => String(call[0]).includes('looks dynamic'));
    expect(dynamicNameWarnings).toHaveLength(1);
    breaker.destroy();
  });

  it('warns once when the number of distinct operations exceeds maxOperations', async () => {
    const logger = { warn: jest.fn(), log: jest.fn() };
    const breaker = new CircuitBreaker({ failureThreshold: 5, resetTimeout: 60000, logger, maxOperations: 2 });

    await breaker.execute('op-a', succeed);
    await breaker.execute('op-b', succeed);
    let maxOpWarnings = logger.warn.mock.calls.filter((call) => String(call[0]).includes('maxOperations'));
    expect(maxOpWarnings).toHaveLength(0);

    await breaker.execute('op-c', succeed);
    maxOpWarnings = logger.warn.mock.calls.filter((call) => String(call[0]).includes('maxOperations'));
    expect(maxOpWarnings).toHaveLength(1);

    await breaker.execute('op-d', succeed);
    await breaker.execute('op-e', succeed);
    maxOpWarnings = logger.warn.mock.calls.filter((call) => String(call[0]).includes('maxOperations'));
    expect(maxOpWarnings).toHaveLength(1); // still only warned once
    breaker.destroy();
  });

  it('does not warn about maxOperations when the option is unset', async () => {
    const logger = { warn: jest.fn(), log: jest.fn() };
    const breaker = new CircuitBreaker({ failureThreshold: 5, resetTimeout: 60000, logger });

    for (let i = 0; i < 10; i++) {
      await breaker.execute(`op-${i}`, succeed);
    }

    const maxOpWarnings = logger.warn.mock.calls.filter((call) => String(call[0]).includes('maxOperations'));
    expect(maxOpWarnings).toHaveLength(0);
    breaker.destroy();
  });
});

describe('CircuitBreaker (distributed mode, skip redundant close write)', () => {
  it('healthy steady state: N successful execute() calls against a clean key perform 0 writes', async () => {
    const store = new InMemoryStore();
    const setSpy = jest.spyOn(store, 'set');
    const getSpy = jest.spyOn(store, 'get');
    const breaker = new CircuitBreaker({ failureThreshold: 5, resetTimeout: 60000, store });

    const N = 5;
    for (let i = 0; i < N; i++) {
      await expect(breaker.execute('op', succeed)).resolves.toBe('ok');
    }

    expect(setSpy).not.toHaveBeenCalled();
    expect(getSpy).toHaveBeenCalledTimes(N);
    breaker.destroy();
  });

  it('one failure then a success performs exactly one close write, since state was dirty', async () => {
    const store = new InMemoryStore();
    const setSpy = jest.spyOn(store, 'set');
    const breaker = new CircuitBreaker({ failureThreshold: 5, resetTimeout: 60000, store });

    await expect(breaker.execute('op', fail)).rejects.toThrow('boom');
    expect(setSpy).toHaveBeenCalledTimes(1); // the failure write itself

    setSpy.mockClear();
    await expect(breaker.execute('op', succeed)).resolves.toBe('ok');
    expect(setSpy).toHaveBeenCalledTimes(1); // close write, because failures: 1 was observed
    breaker.destroy();
  });

  it('a successful half-open trial still writes the close unconditionally', async () => {
    const store = new InMemoryStore();
    const setSpy = jest.spyOn(store, 'set');
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 30, jitter: 0, store });

    await expect(breaker.execute('op', fail)).rejects.toThrow('boom'); // opens the breaker
    await new Promise((resolve) => setTimeout(resolve, 40));
    setSpy.mockClear();

    await expect(breaker.execute('op', succeed)).resolves.toBe('ok'); // wins the trial
    expect(setSpy).toHaveBeenCalledTimes(1); // trial close is always written, regardless of observedState
    breaker.destroy();
  });

  it('a gate read error under failOpenOnStoreError still writes the close on success, since state could not be observed', async () => {
    const store = new FlakyGateStore();
    const breaker = new CircuitBreaker({ failureThreshold: 5, resetTimeout: 60000, store });
    const setSpy = jest.spyOn(store, 'set');

    store.triggerNextGetFailure();
    await expect(breaker.execute('op', succeed)).resolves.toBe('ok');

    expect(setSpy).toHaveBeenCalledTimes(1); // observedState undefined -> write anyway, can't assume clean
    breaker.destroy();
  });

  it('errorRate strategy is unaffected: every closed-phase call still invokes recordOutcomeAtomic exactly once', async () => {
    const store = new AtomicInMemoryStore();
    const spy = jest.spyOn(store, 'recordOutcomeAtomic');
    const breaker = new CircuitBreaker({
      strategy: 'errorRate',
      errorRateThreshold: 0.9,
      minimumCalls: 100,
      resetTimeout: 60000,
      store,
    });

    await expect(breaker.execute('op', succeed)).resolves.toBe('ok');
    await expect(breaker.execute('op', succeed)).resolves.toBe('ok');
    await expect(breaker.execute('op', succeed)).resolves.toBe('ok');

    expect(spy).toHaveBeenCalledTimes(3);
    breaker.destroy();
  });
});

describe('CircuitBreaker (distributed mode, open-state local cache)', () => {
  it('rejects a burst of 1,000 calls over a simulated 10s open window with reads capped by the refresh interval, not the call count', async () => {
    jest.useFakeTimers();
    try {
      const store = new InMemoryStore();
      const getSpy = jest.spyOn(store, 'get');
      const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 60000, jitter: 0, store, openCacheRefreshMs: 2000 });

      await expect(breaker.execute('op', fail)).rejects.toThrow('boom'); // opens + caches
      getSpy.mockClear();

      const totalCalls = 1000;
      const windowMs = 10000;
      let rejections = 0;
      for (let i = 0; i < totalCalls; i++) {
        await expect(breaker.execute('op', succeed)).rejects.toThrow('Circuit breaker is open for operation: op');
        rejections++;
        jest.advanceTimersByTime(windowMs / totalCalls);
      }

      expect(rejections).toBe(totalCalls);
      // Reads should be bounded by how many refresh windows elapsed (~5 for
      // a 10s window at a 2s refresh interval), not by the 1,000 calls.
      expect(getSpy.mock.calls.length).toBeLessThanOrEqual(Math.ceil(windowMs / 2000) + 2);
      breaker.destroy();
    } finally {
      jest.useRealTimers();
    }
  });

  it('detects an early close by another instance once the refresh window elapses', async () => {
    jest.useFakeTimers();
    try {
      const store = new InMemoryStore();
      const breakerA = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 60000, jitter: 0, store, openCacheRefreshMs: 1000 });
      const breakerB = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 60000, jitter: 0, store, openCacheRefreshMs: 1000 });

      await expect(breakerA.execute('op', fail)).rejects.toThrow('boom'); // A opens it, caches open locally

      await breakerB.reset('op'); // another instance closes it directly in the shared store

      // A is still within its refresh window -- serves the stale cached rejection
      await expect(breakerA.execute('op', succeed)).rejects.toThrow('Circuit breaker is open for operation: op');

      jest.advanceTimersByTime(1001); // refresh window elapses

      // A's next call re-reads the store, observes the close, and lets the call through
      await expect(breakerA.execute('op', succeed)).resolves.toBe('ok');

      breakerA.destroy();
      breakerB.destroy();
    } finally {
      jest.useRealTimers();
    }
  });

  it('does not delay half-open eligibility past the jittered reset timeout, even with a long refresh interval', async () => {
    const store = new AtomicInMemoryStore();
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 30, store, openCacheRefreshMs: 10000 });

    await expect(breaker.execute('op', fail)).rejects.toThrow('boom'); // opens + caches open
    await new Promise((resolve) => setTimeout(resolve, 40)); // past resetTimeout, well within the 10s refresh window

    const results = await Promise.allSettled([
      breaker.execute('op', succeed),
      breaker.execute('op', succeed),
      breaker.execute('op', succeed),
    ]);

    const fulfilled = results.filter((r) => r.status === 'fulfilled');
    const rejected = results.filter((r) => r.status === 'rejected');
    expect(fulfilled).toHaveLength(1); // exactly one trial claimed, despite the 10s refresh interval
    expect(rejected).toHaveLength(2);
    breaker.destroy();
  });

  it('openCacheRefreshMs: 0 disables the cache, reading the store on every call', async () => {
    const store = new InMemoryStore();
    const getSpy = jest.spyOn(store, 'get');
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 60000, store, openCacheRefreshMs: 0 });

    await expect(breaker.execute('op', fail)).rejects.toThrow('boom');
    getSpy.mockClear();

    await expect(breaker.execute('op', succeed)).rejects.toThrow('Circuit breaker is open for operation: op');
    await expect(breaker.execute('op', succeed)).rejects.toThrow('Circuit breaker is open for operation: op');
    await expect(breaker.execute('op', succeed)).rejects.toThrow('Circuit breaker is open for operation: op');

    expect(getSpy).toHaveBeenCalledTimes(3); // one real read per call, cache never consulted
    breaker.destroy();
  });

  it('canExecute() returns false while the open state is cached, true otherwise', async () => {
    const store = new InMemoryStore();
    const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeout: 60000, store });

    expect(breaker.canExecute('op')).toBe(true); // nothing cached yet -- advisory true

    await expect(breaker.execute('op', fail)).rejects.toThrow('boom'); // opens + caches

    expect(breaker.canExecute('op')).toBe(false); // real, cached-open result

    breaker.destroy();
  });
});
