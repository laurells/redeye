import type { Redis, Result } from 'ioredis';
import { randomUUID } from 'node:crypto';
import { Store } from '../store';
import { CircuitBreakerState } from '../types';

declare module 'ioredis' {
  interface RedisCommander<Context> {
    redeyeRecordFailure(
      key: string,
      ttlSeconds: number,
      failureThreshold: number,
      now: number,
    ): Result<string, Context>;
    redeyeRecordOutcome(
      key: string,
      decay: number,
      isFailure: number,
      minimumCalls: number,
      errorRateThreshold: number,
      ttlSeconds: number,
      now: number,
    ): Result<string, Context>;
    redeyeReleaseTrial(key: string, token: string): Result<number, Context>;
  }
}

/**
 * Atomically increments the stored failure count and decides `isOpen` in one
 * round trip: GET -> decode -> increment -> compute isOpen -> SET, all inside
 * Redis's single-threaded script execution, so two instances failing at the
 * same instant cannot race and lose an increment. Used by the `'consecutive'`
 * strategy.
 */
const RECORD_FAILURE_SCRIPT = `
local raw = redis.call('GET', KEYS[1])
local failures = 0
local openCount = 0
local errorRate = 0
local sampleCount = 0
if raw then
  local ok, decoded = pcall(cjson.decode, raw)
  if ok and decoded then
    failures = tonumber(decoded.failures) or 0
    openCount = tonumber(decoded.openCount) or 0
    errorRate = tonumber(decoded.errorRate) or 0
    sampleCount = tonumber(decoded.sampleCount) or 0
  end
end
failures = failures + 1
local threshold = tonumber(ARGV[2])
local state = {
  failures = failures,
  lastFailure = tonumber(ARGV[3]),
  isOpen = failures >= threshold,
  openCount = openCount,
  errorRate = errorRate,
  sampleCount = sampleCount
}
redis.call('SET', KEYS[1], cjson.encode(state), 'EX', tonumber(ARGV[1]))
return cjson.encode(state)
`;

/**
 * Atomically folds one call's outcome into an EWMA failure-rate estimate and
 * decides `isOpen` in one round trip. Used by the `'errorRate'` strategy;
 * called on every closed-phase call, not just failures. Also reports
 * whether this specific call is what tripped the breaker (`openedNow`), so
 * the caller can fire `onStateChange` exactly once per transition without a
 * separate read.
 */
const RECORD_OUTCOME_SCRIPT = `
local raw = redis.call('GET', KEYS[1])
local failures = 0
local openCount = 0
local errorRate = 0
local sampleCount = 0
local wasOpen = false
if raw then
  local ok, decoded = pcall(cjson.decode, raw)
  if ok and decoded then
    failures = tonumber(decoded.failures) or 0
    openCount = tonumber(decoded.openCount) or 0
    errorRate = tonumber(decoded.errorRate) or 0
    sampleCount = tonumber(decoded.sampleCount) or 0
    wasOpen = decoded.isOpen or false
  end
end
local decay = tonumber(ARGV[1])
local isFailure = tonumber(ARGV[2])
local minimumCalls = tonumber(ARGV[3])
local threshold = tonumber(ARGV[4])
local ttlSeconds = tonumber(ARGV[5])
local now = tonumber(ARGV[6])

errorRate = errorRate * decay + isFailure * (1 - decay)
sampleCount = sampleCount + 1
local isOpen = (sampleCount >= minimumCalls) and (errorRate >= threshold)

local state = {
  failures = failures,
  lastFailure = now,
  isOpen = isOpen,
  openCount = openCount,
  errorRate = errorRate,
  sampleCount = sampleCount
}
redis.call('SET', KEYS[1], cjson.encode(state), 'EX', ttlSeconds)

local result = {}
for k, v in pairs(state) do result[k] = v end
result.openedNow = isOpen and not wasOpen
return cjson.encode(result)
`;

/**
 * Compare-and-delete: releases a trial claim only if `token` still matches
 * what's stored. Without this, releasing with a plain DEL can destroy a
 * *different* instance's newer claim if the original trial outran its TTL
 * and someone else has since claimed the key (see README limitation on
 * trial-TTL expiry) — a stale release would otherwise cascade into an extra
 * concurrent trial beyond the one that scenario already accepts by design.
 */
const RELEASE_TRIAL_SCRIPT = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
else
  return 0
end
`;

/**
 * Redis-backed Store implementation. Import this from
 * `redeye/redis-store` so the core package has no hard dependency on
 * `ioredis` — only consumers who use RedisStore need it installed.
 *
 * Implements all three optional atomic capabilities (`recordFailureAtomic`
 * and `recordOutcomeAtomic` via Lua scripts, `claimTrial`/`releaseTrial` via
 * a tokened `SET ... NX` and compare-and-delete), so breakers backed by
 * RedisStore get exact counting and real single-trial half-open behavior
 * under both strategies instead of best-effort fallbacks.
 *
 * Scripts are registered once per Redis client via `defineCommand`, so
 * ioredis sends a cached script hash (`EVALSHA`) on every call instead of
 * the full script body, with automatic fallback to `EVAL` if the script
 * cache is ever flushed.
 */
export class RedisStore implements Store {
  private readonly prefix: string;

  constructor(private readonly redis: Redis, options: { keyPrefix?: string } = {}) {
    this.prefix = options.keyPrefix ?? '';
    redis.defineCommand('redeyeRecordFailure', { numberOfKeys: 1, lua: RECORD_FAILURE_SCRIPT });
    redis.defineCommand('redeyeRecordOutcome', { numberOfKeys: 1, lua: RECORD_OUTCOME_SCRIPT });
    redis.defineCommand('redeyeReleaseTrial', { numberOfKeys: 1, lua: RELEASE_TRIAL_SCRIPT });
  }

  async get<T>(key: string): Promise<T | null> {
    const raw = await this.redis.get(this.prefix + key);
    if (raw === null) return null;
    return JSON.parse(raw) as T;
  }

  async set<T>(key: string, value: T, ttlSeconds: number): Promise<void> {
    await this.redis.set(this.prefix + key, JSON.stringify(value), 'EX', Math.max(1, Math.ceil(ttlSeconds)));
  }

  async del(key: string): Promise<void> {
    await this.redis.del(this.prefix + key);
  }

  async recordFailureAtomic(
    key: string,
    opts: { ttlSeconds: number; failureThreshold: number; now: number },
  ): Promise<CircuitBreakerState> {
    const raw = await this.redis.redeyeRecordFailure(
      this.prefix + key,
      Math.max(1, Math.ceil(opts.ttlSeconds)),
      opts.failureThreshold,
      opts.now,
    );
    return JSON.parse(raw) as CircuitBreakerState;
  }

  async recordOutcomeAtomic(
    key: string,
    opts: { success: boolean; decay: number; minimumCalls: number; errorRateThreshold: number; ttlSeconds: number; now: number },
  ): Promise<CircuitBreakerState & { openedNow: boolean }> {
    const raw = await this.redis.redeyeRecordOutcome(
      this.prefix + key,
      opts.decay,
      opts.success ? 0 : 1,
      opts.minimumCalls,
      opts.errorRateThreshold,
      Math.max(1, Math.ceil(opts.ttlSeconds)),
      opts.now,
    );
    return JSON.parse(raw) as CircuitBreakerState & { openedNow: boolean };
  }

  async claimTrial(key: string, ttlSeconds: number): Promise<string | null> {
    const token = randomUUID();
    const result = await this.redis.set(this.prefix + key, token, 'EX', Math.max(1, Math.ceil(ttlSeconds)), 'NX');
    return result === 'OK' ? token : null;
  }

  async releaseTrial(key: string, token: string): Promise<void> {
    await this.redis.redeyeReleaseTrial(this.prefix + key, token);
  }
}
