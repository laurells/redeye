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
   * (conceptually a SET-if-not-exists). Returns an ownership token (an
   * opaque string) if this call won the claim, or `null` if someone else
   * already holds it. Used to implement half-open: only the winner gets to
   * make the trial call while a breaker is transitioning from open back to
   * closed; everyone else stays blocked until the trial resolves.
   *
   * Release the claim via `releaseTrial` with this same token when the
   * trial resolves — never delete the key unconditionally. If the trial
   * outran its TTL, another instance may have since claimed a *new* trial
   * on the same key (see README limitation on trial-TTL expiry, which is
   * by design); an unconditional delete at that point would destroy that
   * other instance's active claim instead of the caller's own expired one.
   *
   * If omitted, the breaker falls back to letting every caller through
   * once the reset window elapses (a thundering herd on recovery — see
   * README).
   */
  claimTrial?(key: string, ttlSeconds: number): Promise<string | null>;

  /**
   * Releases a trial slot claimed via `claimTrial`, but only if `token`
   * still matches what's currently stored under `key` — a compare-and-
   * delete, not an unconditional delete. This is what prevents a trial
   * that outran its TTL from later deleting a *different* instance's newer
   * claim on the same key.
   *
   * If omitted, the breaker logs a one-time warning and does not delete the
   * key at all — claims are then released only by TTL expiry, so recovery
   * after a resolved trial can lag by up to the trial TTL.
   */
  releaseTrial?(key: string, token: string): Promise<void>;

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
