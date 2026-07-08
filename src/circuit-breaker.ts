import { Store } from './store';
import { CircuitBreakerState, CircuitMetrics, CLOSED_STATE, emptyMetrics } from './types';

export type CircuitState = 'closed' | 'open' | 'half-open';

export interface BreakerLogger {
  warn(message: string): void;
  log(message: string): void;
}

export type { CircuitBreakerState, CircuitMetrics };

/**
 * Thrown in distributed mode when the backing store is unreachable and
 * `failOpenOnStoreError` is `false`. Distinguishable from a downstream
 * failure so callers (and metrics) can tell "the dependency failed" apart
 * from "we couldn't tell whether the dependency is healthy."
 */
export class StoreUnavailableError extends Error {
  readonly cause: unknown;

  constructor(operation: string, cause: unknown) {
    super(`Circuit breaker store unavailable for operation: ${operation}`);
    this.name = 'StoreUnavailableError';
    this.cause = cause;
  }
}

export interface CircuitBreakerOptions {
  /**
   * `'consecutive'` (default): trips after `failureThreshold` failures in a
   * row; any success resets the count to zero. Good for hard drops (a
   * dependency going fully down) — bad for flapping/degrading dependencies,
   * since a single interspersed success resets the streak no matter how bad
   * the overall failure rate is.
   *
   * `'errorRate'`: trips when an EWMA-smoothed failure rate crosses
   * `errorRateThreshold`, once at least `minimumCalls` samples have been
   * observed. Tracks both successes and failures, so a dependency that
   * fails most — but not all — of the time still trips the breaker.
   */
  strategy?: 'consecutive' | 'errorRate';
  /** Consecutive failures before the breaker opens. Only used by `strategy: 'consecutive'`. Default: 5 */
  failureThreshold?: number;
  /**
   * Failure rate (0-1) at or above which the breaker opens. Only used by
   * `strategy: 'errorRate'`. Default: 0.5 (50%).
   */
  errorRateThreshold?: number;
  /**
   * Minimum number of samples before `errorRate` can trip the breaker, so a
   * couple of early failures in a cold start don't trip it on a tiny
   * sample size. Only used by `strategy: 'errorRate'`. Default: 10.
   */
  minimumCalls?: number;
  /**
   * EWMA decay factor (0-1) for the `errorRate` strategy: closer to 1
   * weighs history more heavily (slow to react, resistant to noise);
   * closer to 0 reacts faster to recent calls. Default: 0.9.
   */
  errorRateDecay?: number;
  /** Milliseconds to keep the breaker open before allowing a single trial request. Default: 60000 */
  resetTimeout?: number;
  /** How often the local-mode monitor sweeps for stuck trial claims, in ms. Default: 5000 */
  monitorInterval?: number;
  /** Optional per-call timeout in milliseconds. Unset = no timeout (also used to size the trial claim window — see `trialTimeout`). */
  timeout?: number;
  /**
   * Upper bound in ms on how long a claimed half-open trial is allowed to
   * run before its claim is released (local mode) or expires (distributed
   * mode, via the store's TTL). Defaults to `min(timeout ?? 10000, resetTimeout)`.
   * This is what prevents a trial call that never settles from permanently
   * wedging the breaker open.
   */
  trialTimeout?: number;
  /**
   * Multiplier applied to `resetTimeout` each time a half-open trial fails,
   * so a persistently unhealthy dependency is retried less and less often
   * instead of at a fixed cadence forever. Default: 2. Set to 1 to disable backoff.
   */
  backoffMultiplier?: number;
  /** Ceiling for the backed-off reset timeout, in ms. Default: `resetTimeout * 8`. */
  maxResetTimeout?: number;
  /**
   * Randomizes the effective reset timeout by up to this fraction (0-1) so
   * multiple instances don't all attempt their trial at the exact same
   * moment. Default: 0.1 (±10%).
   */
  jitter?: number;
  onStateChange?: (state: CircuitState, operation: string) => void;
  /**
   * Supplying a Store switches the breaker into distributed mode: state is
   * read/written through the store (e.g. Redis) instead of in-process
   * memory, so all instances sharing that store observe the same breaker
   * state. Omit it to keep state local to this process.
   */
  store?: Store;
  /**
   * What to do in distributed mode when the store itself throws (e.g. Redis
   * is unreachable) while checking whether a call is allowed.
   *
   * `true` (default, "fail open"): treat the breaker as closed and let the
   * call through. Favors availability — a store outage degrades you to "no
   * protection," not "everything blocked."
   *
   * `false` ("fail closed"): throw `StoreUnavailableError` without calling
   * the wrapped function. Favors safety.
   *
   * Writes back to the store are always best-effort and logged via
   * `onStoreError` — they never override the real result of a call that
   * already happened.
   */
  failOpenOnStoreError?: boolean;
  onStoreError?: (error: unknown, operation: string) => void;
  logger?: BreakerLogger;
  /**
   * Warns (once) once the number of distinct `operation` names this breaker
   * has seen exceeds this count. `operation` is meant to be a small, fixed
   * set of dependency names — every distinct value gets its own entry in
   * several per-instance maps that are never evicted, so a dynamic name
   * (a tenant ID, a URL, a user ID interpolated in) leaks memory
   * indefinitely. Unset (default): no limit, no warning. See README.
   */
  maxOperations?: number;
}

const noopLogger: BreakerLogger = { warn: () => {}, log: () => {} };

interface Gate {
  allowed: boolean;
  /** True if this call claimed the exclusive half-open trial slot. */
  isTrial: boolean;
  /**
   * The ownership token returned by `Store.claimTrial`, when a real claim
   * was made. Used to release the claim via compare-and-delete. Undefined
   * when `isTrial` is true without a real claim (the store lacks
   * `claimTrial`, or `claimTrial` itself errored and the gate failed open
   * on the trial) — those paths have nothing to compare against, so no
   * release is attempted at all; the claim (if any exists) expires via its
   * TTL instead.
   */
  trialToken?: string;
}

/**
 * A circuit breaker with an optional distributed mode.
 *
 * Local mode (default) tracks failures per-process in memory. Distributed
 * mode (pass a `store`) persists state through the store so a failure burst
 * trips the breaker for every instance sharing it.
 *
 * When the store implements the optional `recordFailureAtomic` and
 * `claimTrial` methods (as `RedisStore` does), distributed mode gets exact
 * failure counts and real single-trial half-open behavior instead of
 * best-effort approximations. See the README for the full reliability
 * model and the handful of tradeoffs that are fundamental to any
 * distributed system (store TTL trust, clock skew on informational
 * timestamps, etc.) rather than fixable engineering gaps.
 */
export class CircuitBreaker {
  private readonly options: Required<
    Omit<
      CircuitBreakerOptions,
      | 'onStateChange'
      | 'store'
      | 'timeout'
      | 'logger'
      | 'onStoreError'
      | 'trialTimeout'
      | 'maxResetTimeout'
      | 'maxOperations'
    >
  > & {
    onStateChange?: (state: CircuitState, operation: string) => void;
    store?: Store;
    timeout?: number;
    logger: BreakerLogger;
    onStoreError?: (error: unknown, operation: string) => void;
    trialTimeout: number;
    maxResetTimeout: number;
    maxOperations?: number;
  };

  private readonly distributed: boolean;
  private readonly warnedCapabilities = new Set<string>();
  private warnedCanExecuteInDistributedMode = false;
  private readonly knownOperations = new Set<string>();
  private warnedMaxOperations = false;
  private warnedDynamicOperationName = false;
  private readonly metrics = new Map<string, CircuitMetrics>();
  private readonly cachedResetTimeouts = new Map<string, { openCount: number; lastFailure: number; value: number }>();

  // local mode state
  private readonly failures = new Map<string, number>();
  private readonly lastFailureTime = new Map<string, number>();
  private readonly openOperations = new Set<string>();
  private readonly openCounts = new Map<string, number>();
  private readonly trialInProgress = new Set<string>();
  private readonly trialClaimedAt = new Map<string, number>();
  private readonly errorRates = new Map<string, number>();
  private readonly sampleCounts = new Map<string, number>();

  private readonly monitorHandle: ReturnType<typeof setInterval>;

  constructor(options: CircuitBreakerOptions = {}) {
    const resetTimeout = options.resetTimeout ?? 60000;
    this.options = {
      strategy: options.strategy ?? 'consecutive',
      failureThreshold: options.failureThreshold ?? 5,
      errorRateThreshold: options.errorRateThreshold ?? 0.5,
      minimumCalls: options.minimumCalls ?? 10,
      errorRateDecay: options.errorRateDecay ?? 0.9,
      resetTimeout,
      monitorInterval: options.monitorInterval ?? 5000,
      timeout: options.timeout,
      trialTimeout: options.trialTimeout ?? Math.min(options.timeout ?? 10000, resetTimeout),
      backoffMultiplier: options.backoffMultiplier ?? 2,
      maxResetTimeout: options.maxResetTimeout ?? resetTimeout * 8,
      jitter: options.jitter ?? 0.1,
      onStateChange: options.onStateChange,
      store: options.store,
      failOpenOnStoreError: options.failOpenOnStoreError ?? true,
      onStoreError: options.onStoreError,
      logger: options.logger ?? noopLogger,
      maxOperations: options.maxOperations,
    };

    this.distributed = !!options.store;

    this.monitorHandle = setInterval(() => this.monitor(), this.options.monitorInterval);
    this.monitorHandle.unref?.();
  }

  /** Stops the local-mode monitor interval. Call this when the breaker is no longer needed. */
  destroy(): void {
    clearInterval(this.monitorHandle);
  }

  async execute<T>(operation: string, fn: () => Promise<T>): Promise<T> {
    this.checkOperationName(operation);
    return this.distributed ? this.executeDistributed(operation, fn) : this.executeLocal(operation, fn);
  }

  /**
   * One-time, best-effort warnings for `operation` values that look like a
   * per-request value (a URL, a tenant/user ID) rather than a small, fixed
   * dependency name — see the README note on why that leaks memory. Cheap
   * heuristics only (length, slashes); not a validator, and never rejects a
   * call.
   */
  private checkOperationName(operation: string): void {
    if (!this.warnedDynamicOperationName && (operation.length > 100 || operation.includes('/'))) {
      this.warnedDynamicOperationName = true;
      const shown = operation.length > 100 ? `${operation.slice(0, 100)}…` : operation;
      this.options.logger.warn(
        `Circuit breaker operation name "${shown}" looks dynamic (${
          operation.length > 100 ? `${operation.length} chars` : 'contains "/"'
        }). operation should be a small, fixed set of names (e.g. "payment-gateway"), not a value interpolated per request (a URL, tenant ID, user ID) — every distinct name is tracked forever and never evicted. See README.`,
      );
    }

    if (this.options.maxOperations !== undefined && !this.warnedMaxOperations && !this.knownOperations.has(operation)) {
      this.knownOperations.add(operation);
      if (this.knownOperations.size > this.options.maxOperations) {
        this.warnedMaxOperations = true;
        this.options.logger.warn(
          `Circuit breaker has seen ${this.knownOperations.size} distinct operation names, exceeding maxOperations (${this.options.maxOperations}). This usually means operation names are dynamic instead of a small fixed set, which leaks memory indefinitely. See README.`,
        );
      }
    }
  }

  /**
   * Synchronous, best-effort, read-only check (never claims a trial slot).
   * In distributed mode this cannot await the store, so it always returns
   * `true` — use `canExecuteAsync` there.
   */
  canExecute(operation: string): boolean {
    if (this.distributed) {
      if (!this.warnedCanExecuteInDistributedMode) {
        this.warnedCanExecuteInDistributedMode = true;
        this.options.logger.warn(
          'canExecute() always returns true in distributed mode (it cannot await the store) and does not reflect real breaker state; use canExecuteAsync() instead.',
        );
      }
      return true;
    }
    return this.peekLocalGate(operation);
  }

  /**
   * Async, store-aware, read-only check (never claims a trial slot — use
   * `execute` for that). Honors `failOpenOnStoreError`. Because it's
   * read-only, a `true` result is advisory: another caller (or `execute`
   * itself, immediately after) can still claim the only trial slot first.
   */
  async canExecuteAsync(operation: string): Promise<boolean> {
    if (!this.distributed) return this.peekLocalGate(operation);
    const gate = await this.gateDistributed(operation, false);
    return gate.allowed;
  }

  recordFailure(operation: string): void {
    this.checkOperationName(operation);
    if (this.distributed) {
      if (this.options.strategy === 'errorRate') {
        void this.recordOutcomeDistributed(operation, false);
      } else {
        void this.recordFailureDistributedCounting(operation);
      }
    } else if (this.options.strategy === 'errorRate') {
      this.recordRateOutcomeLocal(operation, false);
    } else {
      this.recordFailureLocalCounting(operation);
    }
  }

  recordSuccess(operation: string): void {
    this.checkOperationName(operation);
    if (this.distributed) {
      void this.closeDistributedAndNotify(operation);
    } else {
      this.resetLocal(operation);
    }
  }

  /**
   * Used by the manual `recordSuccess()` API only — `execute()`'s own
   * success paths already know locally whether they're closing from a real
   * trial (and fire `onStateChange` directly) or from an already-closed
   * state (where firing would be spurious), so they call `closeDistributed`
   * without this extra read. `recordSuccess()` has no such context — the
   * caller might invoke it on a breaker that's actually open — so it pays
   * for one extra read to detect a real transition, matching what `reset()`
   * already does.
   */
  private async closeDistributedAndNotify(operation: string): Promise<void> {
    const prior = await this.safeGet(operation);
    await this.closeDistributed(operation);
    if (prior?.isOpen) {
      this.options.onStateChange?.('closed', operation);
    }
  }

  async getState(operation: string): Promise<CircuitBreakerState> {
    if (this.distributed) {
      return (await this.safeGet(operation)) ?? { ...CLOSED_STATE };
    }
    const failures = this.failures.get(operation) ?? 0;
    const lastFailure = this.lastFailureTime.get(operation) ?? 0;
    const openCount = this.openCounts.get(operation) ?? 0;
    const errorRate = this.errorRates.get(operation) ?? 0;
    const sampleCount = this.sampleCounts.get(operation) ?? 0;
    return { failures, lastFailure, isOpen: this.openOperations.has(operation), openCount, errorRate, sampleCount };
  }

  async reset(operation: string): Promise<void> {
    if (this.distributed) {
      const prior = await this.safeGet(operation);
      await this.safeDelMain(operation);
      await this.safeDelTrial(operation);
      this.cachedResetTimeouts.delete(operation);
      if (prior?.isOpen) {
        this.options.onStateChange?.('closed', operation);
      }
      this.options.logger.log(`Circuit breaker reset for operation: ${operation}`);
    } else {
      this.resetLocal(operation);
    }
  }

  /** Per-instance call counters for `operation`. In distributed mode this reflects only calls this instance handled — export it via your own metrics system and aggregate across instances, the same way you would any per-process Prometheus counter. */
  getMetrics(operation: string): CircuitMetrics {
    const m = this.metrics.get(operation);
    return m ? { ...m } : emptyMetrics();
  }

  getAllMetrics(): Record<string, CircuitMetrics> {
    const result: Record<string, CircuitMetrics> = {};
    for (const [operation, m] of this.metrics.entries()) result[operation] = { ...m };
    return result;
  }

  private metricsFor(operation: string): CircuitMetrics {
    let m = this.metrics.get(operation);
    if (!m) {
      m = emptyMetrics();
      this.metrics.set(operation, m);
    }
    return m;
  }

  // ---- backoff / jitter -------------------------------------------------

  private backoffCapped(openCount: number): number {
    const backed = this.options.resetTimeout * Math.pow(this.options.backoffMultiplier, openCount);
    return Math.min(backed, this.options.maxResetTimeout);
  }

  /**
   * Jitter is rolled once per open "episode" — identified by (openCount,
   * lastFailure), which only change when the breaker opens or a trial fails
   * — and cached, not re-rolled on every gate check. Re-rolling per call
   * would let callers keep sampling until a roll happens to pass, which
   * systematically biases the effective earliest trial time toward
   * `capped * (1 - jitter)` instead of spreading it around the nominal
   * timeout, and could flip a call between "blocked" and "eligible" and
   * back on consecutive checks milliseconds apart.
   */
  private effectiveResetTimeout(operation: string, openCount: number, lastFailure: number): number {
    const cached = this.cachedResetTimeouts.get(operation);
    if (cached && cached.openCount === openCount && cached.lastFailure === lastFailure) {
      return cached.value;
    }
    const capped = this.backoffCapped(openCount);
    let value = capped;
    if (this.options.jitter) {
      const delta = capped * this.options.jitter;
      value = capped + (Math.random() * 2 - 1) * delta;
    }
    this.cachedResetTimeouts.set(operation, { openCount, lastFailure, value });
    return value;
  }

  // ---- distributed mode -----------------------------------------------

  private key(operation: string): string {
    return `circuit_breaker:${operation}`;
  }

  private trialKey(operation: string): string {
    return `circuit_breaker:${operation}:trial`;
  }

  private stateTtlSeconds(openCount: number): number {
    // A generous safety-net TTL, decoupled from the actual gating decision
    // (which uses lastFailure + effectiveResetTimeout below) so early
    // eviction degrades gracefully rather than being the sole correctness
    // mechanism. See README limitation on store TTL trust. Uses the
    // unjittered bound — jitter exists to spread out gating decisions, not
    // to vary a generous cleanup TTL.
    return Math.max(1, Math.ceil((this.backoffCapped(openCount) * 3) / 1000));
  }

  private trialTtlSeconds(): number {
    return Math.max(1, Math.ceil(this.options.trialTimeout / 1000));
  }

  private reportStoreError(error: unknown, operation: string): void {
    this.metricsFor(operation).totalStoreErrors++;
    this.options.logger.warn(`Circuit breaker store error for operation: ${operation}: ${(error as Error)?.message ?? error}`);
    this.options.onStoreError?.(error, operation);
  }

  private warnMissingCapability(capability: 'recordFailureAtomic' | 'recordOutcomeAtomic' | 'claimTrial' | 'releaseTrial'): void {
    if (this.warnedCapabilities.has(capability)) return;
    this.warnedCapabilities.add(capability);
    this.options.logger.warn(
      `Store does not implement ${capability}(); falling back to best-effort distributed semantics for this capability. See README limitations.`,
    );
  }

  private async safeGet(operation: string): Promise<CircuitBreakerState | null> {
    try {
      return await this.options.store!.get<CircuitBreakerState>(this.key(operation));
    } catch (error) {
      this.reportStoreError(error, operation);
      return null;
    }
  }

  private async safeSet(operation: string, state: CircuitBreakerState, ttlSeconds: number): Promise<void> {
    try {
      await this.options.store!.set(this.key(operation), state, ttlSeconds);
    } catch (error) {
      this.reportStoreError(error, operation);
    }
  }

  /** Deletes the main state key for `operation`. Best-effort: logged, never thrown. */
  private async safeDelMain(operation: string): Promise<void> {
    try {
      await this.options.store!.del(this.key(operation));
    } catch (error) {
      this.reportStoreError(error, operation);
    }
  }

  /** Deletes the trial-claim key for `operation` unconditionally, releasing it early instead of waiting for its TTL. Only for a full reset/wipe — see `safeReleaseTrial` for releasing a specific held claim. Best-effort. */
  private async safeDelTrial(operation: string): Promise<void> {
    try {
      await this.options.store!.del(this.trialKey(operation));
    } catch (error) {
      this.reportStoreError(error, operation);
    }
  }

  /**
   * Releases a trial claim this call actually won, early instead of
   * waiting for its TTL. Uses a compare-and-delete (`Store.releaseTrial`)
   * so a trial that outran its TTL can't delete a *different* instance's
   * newer claim on the same key (see README limitation on trial-TTL
   * expiry).
   *
   * Deliberately does *not* fall back to an unconditional delete when
   * there's no token (the gate failed open on the trial without a
   * confirmed claim — e.g. `claimTrial` itself errored) or the store lacks
   * `releaseTrial` (an incomplete custom `Store`) — an unconditional delete
   * in either case would recreate the exact bug this token scheme exists to
   * prevent: deleting a claim we never confirmed is ours, which may by now
   * belong to a different instance. The worst case for doing nothing is the
   * slot staying occupied up to `trialTimeout` longer than it needs to —
   * bounded and safe. Best-effort: logged, never thrown.
   */
  private async safeReleaseTrial(operation: string, token: string | undefined): Promise<void> {
    if (!token) return;
    if (!this.options.store!.releaseTrial) {
      this.warnMissingCapability('releaseTrial');
      return;
    }
    try {
      await this.options.store!.releaseTrial(this.trialKey(operation), token);
    } catch (error) {
      this.reportStoreError(error, operation);
    }
  }

  private async closeDistributed(operation: string): Promise<void> {
    await this.safeSet(operation, { ...CLOSED_STATE }, this.stateTtlSeconds(0));
    this.cachedResetTimeouts.delete(operation);
  }

  /**
   * Determines whether a call is allowed through in distributed mode.
   * `claim: true` (used by `execute`) attempts to atomically claim the
   * single half-open trial slot when eligible. `claim: false` (used by
   * `canExecuteAsync`) is read-only and never claims.
   */
  private async gateDistributed(operation: string, claim: boolean): Promise<Gate> {
    let state: CircuitBreakerState | null;
    try {
      state = await this.options.store!.get<CircuitBreakerState>(this.key(operation));
    } catch (error) {
      this.reportStoreError(error, operation);
      if (this.options.failOpenOnStoreError) return { allowed: true, isTrial: false };
      throw new StoreUnavailableError(operation, error);
    }

    if (!state?.isOpen) return { allowed: true, isTrial: false };

    const effective = this.effectiveResetTimeout(operation, state.openCount ?? 0, state.lastFailure);
    if (Date.now() - state.lastFailure < effective) {
      return { allowed: false, isTrial: false };
    }

    if (!claim) return { allowed: true, isTrial: false };

    if (this.options.store!.claimTrial) {
      try {
        const token = await this.options.store!.claimTrial(this.trialKey(operation), this.trialTtlSeconds());
        if (token) this.options.onStateChange?.('half-open', operation);
        return { allowed: token !== null, isTrial: token !== null, trialToken: token ?? undefined };
      } catch (error) {
        this.reportStoreError(error, operation);
        if (!this.options.failOpenOnStoreError) throw new StoreUnavailableError(operation, error);
        // Can't confirm exclusivity — fail open on the trial itself rather than deadlocking forever.
        this.options.onStateChange?.('half-open', operation);
        return { allowed: true, isTrial: true };
      }
    }

    this.warnMissingCapability('claimTrial');
    this.options.onStateChange?.('half-open', operation);
    return { allowed: true, isTrial: true };
  }

  private async executeDistributed<T>(operation: string, fn: () => Promise<T>): Promise<T> {
    const metrics = this.metricsFor(operation);
    metrics.totalCalls++;

    const gate = await this.gateDistributed(operation, true);
    if (!gate.allowed) {
      metrics.totalRejections++;
      throw new Error(`Circuit breaker is open for operation: ${operation}`);
    }

    try {
      const result = await this.executeWithTimeout(fn);
      metrics.totalSuccesses++;
      if (gate.isTrial) {
        await this.closeDistributed(operation);
        await this.safeReleaseTrial(operation, gate.trialToken);
        this.options.onStateChange?.('closed', operation);
      } else if (this.options.strategy === 'errorRate') {
        await this.recordOutcomeDistributed(operation, true);
      } else {
        await this.closeDistributed(operation);
      }
      return result;
    } catch (error) {
      metrics.totalFailures++;
      if (gate.isTrial) {
        await this.reopenAfterFailedTrialDistributed(operation);
        await this.safeReleaseTrial(operation, gate.trialToken);
      } else if (this.options.strategy === 'errorRate') {
        await this.recordOutcomeDistributed(operation, false);
      } else {
        await this.recordFailureDistributedCounting(operation);
      }
      throw error;
    }
  }

  private async recordFailureDistributedCounting(operation: string): Promise<void> {
    if (this.options.store!.recordFailureAtomic) {
      try {
        const next = await this.options.store!.recordFailureAtomic(this.key(operation), {
          ttlSeconds: this.stateTtlSeconds(0),
          failureThreshold: this.options.failureThreshold,
          now: Date.now(),
        });
        this.options.logger.warn(`Recorded failure for operation: ${operation}, total failures: ${next.failures}`);
        if (next.isOpen && next.failures === this.options.failureThreshold) {
          this.options.logger.warn(`Circuit breaker opened for operation: ${operation}`);
          this.options.onStateChange?.('open', operation);
        }
        return;
      } catch (error) {
        this.reportStoreError(error, operation);
        // fall through to non-atomic fallback below
      }
    } else {
      this.warnMissingCapability('recordFailureAtomic');
    }

    const current = (await this.safeGet(operation)) ?? { ...CLOSED_STATE };
    const next: CircuitBreakerState = {
      failures: current.failures + 1,
      lastFailure: Date.now(),
      isOpen: current.failures + 1 >= this.options.failureThreshold,
      // Preserve, don't reset: a closed-phase failure can't normally happen
      // while the breaker is open, but under failOpenOnStoreError a store
      // blip on the gate read can let a call proceed while state.isOpen is
      // still true elsewhere, and this write must not wipe accumulated
      // backoff if that call then fails and lands here.
      openCount: current.openCount ?? 0,
      errorRate: current.errorRate ?? 0,
      sampleCount: current.sampleCount ?? 0,
    };
    await this.safeSet(operation, next, this.stateTtlSeconds(0));
    this.options.logger.warn(`Recorded failure for operation: ${operation}, total failures: ${next.failures}`);

    if (next.isOpen && !current.isOpen) {
      this.options.logger.warn(`Circuit breaker opened for operation: ${operation}`);
      this.options.onStateChange?.('open', operation);
    }
  }

  /**
   * `strategy: 'errorRate'` counterpart to `recordFailureDistributedCounting`
   * — called on every closed-phase call (success or failure), not just
   * failures, since the rate needs both to be meaningful.
   */
  private async recordOutcomeDistributed(operation: string, success: boolean): Promise<void> {
    if (this.options.store!.recordOutcomeAtomic) {
      try {
        const next = await this.options.store!.recordOutcomeAtomic(this.key(operation), {
          success,
          decay: this.options.errorRateDecay,
          minimumCalls: this.options.minimumCalls,
          errorRateThreshold: this.options.errorRateThreshold,
          ttlSeconds: this.stateTtlSeconds(0),
          now: Date.now(),
        });
        if (next.openedNow) {
          this.options.logger.warn(
            `Circuit breaker opened for operation: ${operation} (error rate ${(next.errorRate * 100).toFixed(1)}% over ${next.sampleCount} calls)`,
          );
          this.options.onStateChange?.('open', operation);
        }
        return;
      } catch (error) {
        this.reportStoreError(error, operation);
        // fall through to non-atomic fallback below
      }
    } else {
      this.warnMissingCapability('recordOutcomeAtomic');
    }

    const current = (await this.safeGet(operation)) ?? { ...CLOSED_STATE };
    const decay = this.options.errorRateDecay;
    const errorRate = (current.errorRate ?? 0) * decay + (success ? 0 : 1) * (1 - decay);
    const sampleCount = (current.sampleCount ?? 0) + 1;
    const isOpen = sampleCount >= this.options.minimumCalls && errorRate >= this.options.errorRateThreshold;

    const next: CircuitBreakerState = {
      failures: current.failures,
      lastFailure: Date.now(),
      isOpen,
      openCount: current.openCount ?? 0,
      errorRate,
      sampleCount,
    };
    await this.safeSet(operation, next, this.stateTtlSeconds(0));

    if (isOpen && !current.isOpen) {
      this.options.logger.warn(
        `Circuit breaker opened for operation: ${operation} (error rate ${(errorRate * 100).toFixed(1)}% over ${sampleCount} calls)`,
      );
      this.options.onStateChange?.('open', operation);
    }
  }

  private async reopenAfterFailedTrialDistributed(operation: string): Promise<void> {
    const current = (await this.safeGet(operation)) ?? { ...CLOSED_STATE, isOpen: true };
    const openCount = (current.openCount ?? 0) + 1;
    const next: CircuitBreakerState = {
      failures: Math.max(current.failures, this.options.failureThreshold),
      lastFailure: Date.now(),
      isOpen: true,
      openCount,
      errorRate: current.errorRate ?? 0,
      sampleCount: current.sampleCount ?? 0,
    };
    await this.safeSet(operation, next, this.stateTtlSeconds(openCount));
    this.options.logger.warn(`Half-open trial failed for operation: ${operation}; backing off (attempt ${openCount + 1})`);
    this.options.onStateChange?.('open', operation);
  }

  // ---- local mode -------------------------------------------------------

  private async executeLocal<T>(operation: string, fn: () => Promise<T>): Promise<T> {
    const metrics = this.metricsFor(operation);
    metrics.totalCalls++;

    const gate = this.claimLocalGate(operation);
    if (!gate.allowed) {
      metrics.totalRejections++;
      throw new Error(`Circuit breaker is open for operation: ${operation}`);
    }

    try {
      const result = await this.executeWithTimeout(fn);
      metrics.totalSuccesses++;
      if (gate.isTrial) {
        this.resetLocal(operation);
      } else if (this.options.strategy === 'errorRate') {
        this.recordRateOutcomeLocal(operation, true);
      } else {
        this.resetLocal(operation);
      }
      return result;
    } catch (error) {
      metrics.totalFailures++;
      if (gate.isTrial) {
        this.reopenAfterFailedTrialLocal(operation);
      } else if (this.options.strategy === 'errorRate') {
        this.recordRateOutcomeLocal(operation, false);
      } else {
        this.recordFailureLocalCounting(operation);
      }
      throw error;
    }
  }

  /** Read-only gate check — never claims the trial slot. Used by `canExecute`. */
  private peekLocalGate(operation: string): boolean {
    if (!this.openOperations.has(operation)) return true;

    const openCount = this.openCounts.get(operation) ?? 0;
    const lastFailure = this.lastFailureTime.get(operation) ?? 0;
    const effective = this.effectiveResetTimeout(operation, openCount, lastFailure);

    if (Date.now() - lastFailure < effective) {
      this.options.logger.warn(`Circuit breaker open for operation: ${operation}`);
      return false;
    }
    return true;
  }

  /** Gate check used by `execute` — claims the exclusive trial slot when eligible. */
  private claimLocalGate(operation: string): Gate {
    if (!this.openOperations.has(operation)) return { allowed: true, isTrial: false };

    const openCount = this.openCounts.get(operation) ?? 0;
    const lastFailure = this.lastFailureTime.get(operation) ?? 0;
    const effective = this.effectiveResetTimeout(operation, openCount, lastFailure);

    if (Date.now() - lastFailure < effective) {
      this.options.logger.warn(`Circuit breaker open for operation: ${operation}`);
      return { allowed: false, isTrial: false };
    }

    if (this.trialInProgress.has(operation)) {
      // Another in-flight call already claimed the trial; stay blocked until it resolves.
      return { allowed: false, isTrial: false };
    }

    this.trialInProgress.add(operation);
    this.trialClaimedAt.set(operation, Date.now());
    this.options.onStateChange?.('half-open', operation);
    return { allowed: true, isTrial: true };
  }

  private recordFailureLocalCounting(operation: string): void {
    const failures = (this.failures.get(operation) ?? 0) + 1;
    this.failures.set(operation, failures);
    this.lastFailureTime.set(operation, Date.now());
    this.options.logger.warn(`Recorded failure for operation: ${operation}, total failures: ${failures}`);

    if (failures >= this.options.failureThreshold && !this.openOperations.has(operation)) {
      this.openOperations.add(operation);
      this.openCounts.set(operation, 0);
      this.options.logger.warn(`Circuit breaker opened for operation: ${operation}`);
      this.options.onStateChange?.('open', operation);
    }
  }

  /** `strategy: 'errorRate'` counterpart to `recordFailureLocalCounting` — called on every closed-phase call, not just failures. */
  private recordRateOutcomeLocal(operation: string, success: boolean): void {
    const decay = this.options.errorRateDecay;
    const rate = (this.errorRates.get(operation) ?? 0) * decay + (success ? 0 : 1) * (1 - decay);
    const count = (this.sampleCounts.get(operation) ?? 0) + 1;
    this.errorRates.set(operation, rate);
    this.sampleCounts.set(operation, count);

    if (count >= this.options.minimumCalls && rate >= this.options.errorRateThreshold && !this.openOperations.has(operation)) {
      this.openOperations.add(operation);
      this.openCounts.set(operation, 0);
      this.lastFailureTime.set(operation, Date.now());
      this.options.logger.warn(
        `Circuit breaker opened for operation: ${operation} (error rate ${(rate * 100).toFixed(1)}% over ${count} calls)`,
      );
      this.options.onStateChange?.('open', operation);
    }
  }

  private reopenAfterFailedTrialLocal(operation: string): void {
    this.trialInProgress.delete(operation);
    this.trialClaimedAt.delete(operation);
    const openCount = (this.openCounts.get(operation) ?? 0) + 1;
    this.openCounts.set(operation, openCount);
    this.lastFailureTime.set(operation, Date.now());
    this.options.logger.warn(`Half-open trial failed for operation: ${operation}; backing off (attempt ${openCount + 1})`);
    this.options.onStateChange?.('open', operation);
  }

  private resetLocal(operation: string): void {
    const wasOpen = this.openOperations.has(operation);
    this.failures.delete(operation);
    this.lastFailureTime.delete(operation);
    this.openOperations.delete(operation);
    this.openCounts.delete(operation);
    this.trialInProgress.delete(operation);
    this.trialClaimedAt.delete(operation);
    this.errorRates.delete(operation);
    this.sampleCounts.delete(operation);
    this.cachedResetTimeouts.delete(operation);
    if (wasOpen) {
      this.options.onStateChange?.('closed', operation);
    }
    this.options.logger.log(`Circuit breaker reset for operation: ${operation}`);
  }

  /**
   * Safety net only: force-releases a trial claim that's been held far
   * longer than `trialTimeout` should ever allow, which can only happen if
   * the wrapped function never settles (no timeout configured and it hangs
   * forever). Without this, a single hung call would permanently wedge the
   * breaker open with no way to ever attempt another trial.
   */
  private monitor(): void {
    const now = Date.now();
    const staleBound = Math.max(this.options.trialTimeout, 1000) * 3;
    for (const [operation, claimedAt] of this.trialClaimedAt.entries()) {
      if (now - claimedAt > staleBound) {
        this.trialInProgress.delete(operation);
        this.trialClaimedAt.delete(operation);
        this.options.logger.warn(
          `Circuit breaker trial for operation: ${operation} appears stuck (wrapped function never settled); force-releasing after ${staleBound}ms. Consider configuring a timeout.`,
        );
      }
    }
  }

  // ---- shared -------------------------------------------------------------

  private executeWithTimeout<T>(fn: () => Promise<T>): Promise<T> {
    if (!this.options.timeout) return fn();

    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`Operation timed out after ${this.options.timeout}ms`)), this.options.timeout);
      fn().then(
        (value) => {
          clearTimeout(timer);
          resolve(value);
        },
        (error) => {
          clearTimeout(timer);
          reject(error);
        },
      );
    });
  }
}
