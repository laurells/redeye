import { CircuitBreakerState, TransitionEvent } from './types';

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
    opts: { ttlSeconds: number; failureThreshold: number; now: number; operation: string },
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
    opts: {
      success: boolean;
      decay: number;
      minimumCalls: number;
      errorRateThreshold: number;
      ttlSeconds: number;
      now: number;
      operation: string;
    },
  ): Promise<CircuitBreakerState & { openedNow: boolean }>;

  /**
   * Atomically closes `key`, but only if it wasn't already clean (open, or
   * had accumulated failures) — a no-op, no-write "return the current
   * state" when it was already closed with nothing to clear. Bumps
   * `version` and publishes a transition event to `eventsKey` when it does
   * write. This is what lets `closeDistributed` skip a write on the healthy
   * path *atomically* (server-side), rather than relying on the caller's
   * own possibly-stale `observedState` the way the non-atomic fallback does.
   *
   * If omitted, the breaker falls back to an unconditional non-atomic
   * get-then-set with no version bump and no published event — correctness
   * is unaffected (the caller's own `observedState` check still guards the
   * write), but other instances relying on `subscribeTransitions` won't
   * observe this particular close.
   */
  closeAtomic?(key: string, eventsKey: string, opts: { ttlSeconds: number; operation: string }): Promise<CircuitBreakerState>;

  /**
   * Atomically reopens `key` after a failed half-open trial: `isOpen: true`,
   * `openCount + 1`, `failures` raised to at least `failureThreshold`,
   * `lastFailure: now`. Bumps `version` and publishes a transition event to
   * `eventsKey`.
   *
   * If omitted, the breaker falls back to a non-atomic get-then-set with no
   * version bump and no published event.
   */
  reopenTrialFailureAtomic?(
    key: string,
    eventsKey: string,
    opts: { ttlSeconds: number; failureThreshold: number; now: number; operation: string },
  ): Promise<CircuitBreakerState>;

  /**
   * Subscribes to the shared transition-event stream at `eventsKey` — one
   * stream per store/prefix, carrying every state change any instance
   * writes via `recordFailureAtomic`, `recordOutcomeAtomic`,
   * `closeAtomic`, or `reopenTrialFailureAtomic` (never on a plain
   * counter/EWMA update that doesn't flip `isOpen` or clear state). Used to
   * invalidate the optional closed-state local cache (`localCache`)
   * push-style instead of polling.
   *
   * `handler` is called once per delivered event, and with `null` whenever
   * the subscription's underlying connection drops or errors — callers
   * must treat `null` as "distrust every locally cached entry until it's
   * been re-verified by a real read," since events may have been missed
   * during the gap. Implementations should retry the underlying
   * subscription with backoff after such a drop; callers are not expected
   * to re-subscribe themselves.
   *
   * Returns an unsubscribe function that tears down the subscription.
   * Implementations needing a dedicated (e.g. blocking) connection should
   * establish it lazily on first call.
   *
   * Must support more than one concurrent call on the same store instance:
   * multiple `CircuitBreaker`s sharing one store (a natural thing to do)
   * each call this independently with `localCache` set, and every one of
   * them needs to actually receive events, not just the first to subscribe.
   * `RedisStore` does this by fanning out one shared connection/read loop
   * to every registered handler, tearing down only once the last one
   * unsubscribes — implementations that instead throw or silently replace
   * the previous subscription on a second call will leave every caller
   * after the first with a permanently disabled `localCache`.
   *
   * If omitted, the breaker logs a one-time warning and `localCache` stays
   * disabled entirely — behavior is then identical to not setting
   * `localCache` at all (the open-state cache from a prior release, if
   * configured, is unaffected).
   */
  subscribeTransitions?(eventsKey: string, handler: (event: TransitionEvent | null) => void): Promise<() => void>;
}
