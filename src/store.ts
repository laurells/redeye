import { CircuitBreakerState } from './types';

export interface Store {
  get<T>(key: string): Promise<T | null>;
  set<T>(key: string, value: T, ttlSeconds: number): Promise<void>;
  del(key: string): Promise<void>;

  /**
   * Atomically increments the stored failure count for `key` and decides
   * `isOpen` in a single round trip, so concurrent callers across instances
   * can't race and lose an increment the way a plain get-then-set would.
   *
   * If omitted, the breaker falls back to a non-atomic get-then-set, which
   * has a narrow race window under concurrent failures (see README).
   */
  recordFailureAtomic?(
    key: string,
    opts: { ttlSeconds: number; failureThreshold: number; now: number },
  ): Promise<CircuitBreakerState>;

  /**
   * Atomically claims a short-lived, exclusive "trial" slot for `key`
   * (conceptually a SET-if-not-exists). Returns `true` if this call won the
   * claim. Used to implement half-open: only the winner gets to make the
   * trial call while a breaker is transitioning from open back to closed;
   * everyone else stays blocked until the trial resolves.
   *
   * If omitted, the breaker falls back to letting every caller through
   * once the reset window elapses (a thundering herd on recovery — see
   * README).
   */
  claimTrial?(key: string, ttlSeconds: number): Promise<boolean>;

  /**
   * `strategy: 'errorRate'` only. Atomically folds one call's outcome into
   * an EWMA failure-rate estimate and decides `isOpen` in a single round
   * trip, the same atomicity guarantee `recordFailureAtomic` provides for
   * the `'consecutive'` strategy. Called on every closed-phase call
   * (success or failure), not just failures.
   *
   * `openedNow` distinguishes "this call is what tripped the breaker" from
   * "the breaker was already open," so the caller can fire `onStateChange`
   * exactly once per transition.
   *
   * If omitted, the breaker falls back to a non-atomic get-then-set.
   */
  recordOutcomeAtomic?(
    key: string,
    opts: { success: boolean; decay: number; minimumCalls: number; errorRateThreshold: number; ttlSeconds: number; now: number },
  ): Promise<CircuitBreakerState & { openedNow: boolean }>;
}
