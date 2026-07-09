export interface CircuitBreakerState {
  failures: number;
  lastFailure: number;
  isOpen: boolean;
  /** Number of consecutive times a half-open trial has failed since the breaker last fully closed. Drives backoff growth. */
  openCount: number;
  /** `errorRate` strategy only: current EWMA estimate of the failure rate, 0-1. */
  errorRate: number;
  /** `errorRate` strategy only: number of calls folded into `errorRate` since the breaker last fully closed. */
  sampleCount: number;
  /**
   * Monotonic transition counter. Increments only when `isOpen` flips or a
   * close clears accumulated state — never on a plain counter/EWMA update —
   * so it identifies state *transitions*, not writes. Absent on states
   * written by older library versions (pre-`localCache`) that don't know
   * about it: treat absence as "unknown provenance, distrust any local
   * cache of this key," never as version `0`. See README's local caching
   * section for the mixed-fleet rolling-deploy behavior this enables.
   */
  version?: number;
}

/**
 * A decoded entry from the store's shared transition stream (see
 * `Store.subscribeTransitions`), published only when a write actually
 * changes `isOpen` or clears accumulated state — not on every write.
 * `null` is a distinct signal from the subscription itself (a dropped or
 * reconnecting connection), not a decoded event; see `subscribeTransitions`.
 */
export interface TransitionEvent {
  operation: string;
  version: number;
  isOpen: boolean;
  lastFailure: number;
  openCount: number;
}

export interface CircuitMetrics {
  totalCalls: number;
  totalSuccesses: number;
  totalFailures: number;
  /** Calls rejected without invoking the wrapped function because the breaker was open. */
  totalRejections: number;
  /** Distributed mode only: store read/write failures encountered. */
  totalStoreErrors: number;
}

export const CLOSED_STATE: CircuitBreakerState = {
  failures: 0,
  lastFailure: 0,
  isOpen: false,
  openCount: 0,
  errorRate: 0,
  sampleCount: 0,
  version: 0,
};

export const emptyMetrics = (): CircuitMetrics => ({
  totalCalls: 0,
  totalSuccesses: 0,
  totalFailures: 0,
  totalRejections: 0,
  totalStoreErrors: 0,
});
