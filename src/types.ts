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
};

export const emptyMetrics = (): CircuitMetrics => ({
  totalCalls: 0,
  totalSuccesses: 0,
  totalFailures: 0,
  totalRejections: 0,
  totalStoreErrors: 0,
});
