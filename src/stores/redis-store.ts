import type { Redis, Result } from 'ioredis';
import { randomUUID } from 'node:crypto';
import { Store } from '../store';
import { CircuitBreakerState, TransitionEvent } from '../types';

declare module 'ioredis' {
  interface RedisCommander<Context> {
    redeyeRecordFailure(
      key: string,
      eventsKey: string,
      ttlSeconds: number,
      failureThreshold: number,
      now: number,
      operation: string,
    ): Result<string, Context>;
    redeyeRecordOutcome(
      key: string,
      eventsKey: string,
      decay: number,
      isFailure: number,
      minimumCalls: number,
      errorRateThreshold: number,
      ttlSeconds: number,
      now: number,
      operation: string,
    ): Result<string, Context>;
    redeyeReleaseTrial(key: string, token: string): Result<number, Context>;
    redeyeClose(key: string, eventsKey: string, ttlSeconds: number, operation: string): Result<string, Context>;
    redeyeReopenTrialFailure(
      key: string,
      eventsKey: string,
      ttlSeconds: number,
      failureThreshold: number,
      now: number,
      operation: string,
    ): Result<string, Context>;
  }
}

/** The stream key every transition-publishing script writes to, one per `RedisStore` (i.e. one per `keyPrefix`), not one per operation. */
const EVENTS_KEY = 'circuit_breaker:events';

/**
 * Atomically increments the stored failure count and decides `isOpen` in one
 * round trip: GET -> decode -> increment -> compute isOpen -> SET, all inside
 * Redis's single-threaded script execution, so two instances failing at the
 * same instant cannot race and lose an increment. Used by the `'consecutive'`
 * strategy.
 *
 * Also bumps `version` and publishes a transition event to the shared events
 * stream (KEYS[2]), but only when this call flips `isOpen` -- a plain
 * sub-threshold failure count increment is not a transition and stays
 * silent, so the closed-state cache (which only cares about transitions)
 * isn't invalidated on every single failure, just the ones that matter.
 */
const RECORD_FAILURE_SCRIPT = `
local raw = redis.call('GET', KEYS[1])
local failures = 0
local openCount = 0
local errorRate = 0
local sampleCount = 0
local version = 0
local wasOpen = false
if raw then
  local ok, decoded = pcall(cjson.decode, raw)
  if ok and decoded then
    failures = tonumber(decoded.failures) or 0
    openCount = tonumber(decoded.openCount) or 0
    errorRate = tonumber(decoded.errorRate) or 0
    sampleCount = tonumber(decoded.sampleCount) or 0
    version = tonumber(decoded.version) or 0
    wasOpen = decoded.isOpen or false
  end
end
failures = failures + 1
local threshold = tonumber(ARGV[2])
local isOpen = failures >= threshold
if isOpen ~= wasOpen then
  version = version + 1
end
local state = {
  failures = failures,
  lastFailure = tonumber(ARGV[3]),
  isOpen = isOpen,
  openCount = openCount,
  errorRate = errorRate,
  sampleCount = sampleCount,
  version = version
}
redis.call('SET', KEYS[1], cjson.encode(state), 'EX', tonumber(ARGV[1]))
if isOpen ~= wasOpen then
  redis.call('XADD', KEYS[2], '*', 'op', ARGV[4], 'version', version, 'state', isOpen and 'open' or 'closed', 'lastFailure', state.lastFailure, 'openCount', openCount)
  redis.call('XTRIM', KEYS[2], 'MAXLEN', '~', 1000)
end
return cjson.encode(state)
`;

/**
 * Atomically folds one call's outcome into an EWMA failure-rate estimate and
 * decides `isOpen` in one round trip. Used by the `'errorRate'` strategy;
 * called on every closed-phase call, not just failures. Also reports
 * whether this specific call is what tripped the breaker (`openedNow`), so
 * the caller can fire `onStateChange` exactly once per transition without a
 * separate read.
 *
 * Bumps `version` and publishes a transition event the same way
 * `RECORD_FAILURE_SCRIPT` does, only on an `isOpen` flip.
 */
const RECORD_OUTCOME_SCRIPT = `
local raw = redis.call('GET', KEYS[1])
local failures = 0
local openCount = 0
local errorRate = 0
local sampleCount = 0
local version = 0
local wasOpen = false
if raw then
  local ok, decoded = pcall(cjson.decode, raw)
  if ok and decoded then
    failures = tonumber(decoded.failures) or 0
    openCount = tonumber(decoded.openCount) or 0
    errorRate = tonumber(decoded.errorRate) or 0
    sampleCount = tonumber(decoded.sampleCount) or 0
    version = tonumber(decoded.version) or 0
    wasOpen = decoded.isOpen or false
  end
end
local decay = tonumber(ARGV[1])
local isFailure = tonumber(ARGV[2])
local minimumCalls = tonumber(ARGV[3])
local threshold = tonumber(ARGV[4])
local ttlSeconds = tonumber(ARGV[5])
local now = tonumber(ARGV[6])
local operation = ARGV[7]

errorRate = errorRate * decay + isFailure * (1 - decay)
sampleCount = sampleCount + 1
local isOpen = (sampleCount >= minimumCalls) and (errorRate >= threshold)
if isOpen ~= wasOpen then
  version = version + 1
end

local state = {
  failures = failures,
  lastFailure = now,
  isOpen = isOpen,
  openCount = openCount,
  errorRate = errorRate,
  sampleCount = sampleCount,
  version = version
}
redis.call('SET', KEYS[1], cjson.encode(state), 'EX', ttlSeconds)

if isOpen ~= wasOpen then
  redis.call('XADD', KEYS[2], '*', 'op', operation, 'version', version, 'state', isOpen and 'open' or 'closed', 'lastFailure', now, 'openCount', openCount)
  redis.call('XTRIM', KEYS[2], 'MAXLEN', '~', 1000)
end

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
 * Atomically closes `key`, but only writes (and only bumps `version` /
 * publishes a transition) if there was anything to clear: it was open, or
 * had accumulated sub-threshold failures. Otherwise it's a genuine no-op --
 * it returns the current (already-clean) state without a write, which is
 * what lets the healthy path skip a write *atomically*, server-side,
 * instead of relying on the caller's own possibly-stale `observedState`.
 */
const CLOSE_SCRIPT = `
local raw = redis.call('GET', KEYS[1])
local version = 0
local wasOpen = false
local wasDirty = false
if raw then
  local ok, decoded = pcall(cjson.decode, raw)
  if ok and decoded then
    version = tonumber(decoded.version) or 0
    wasOpen = decoded.isOpen or false
    local failures = tonumber(decoded.failures) or 0
    wasDirty = wasOpen or (failures > 0)
  end
end

if not wasDirty then
  if raw then
    return raw
  end
  local clean = { failures = 0, lastFailure = 0, isOpen = false, openCount = 0, errorRate = 0, sampleCount = 0, version = 0 }
  return cjson.encode(clean)
end

version = version + 1
local ttlSeconds = tonumber(ARGV[1])
local operation = ARGV[2]
local state = { failures = 0, lastFailure = 0, isOpen = false, openCount = 0, errorRate = 0, sampleCount = 0, version = version }
redis.call('SET', KEYS[1], cjson.encode(state), 'EX', ttlSeconds)
redis.call('XADD', KEYS[2], '*', 'op', operation, 'version', version, 'state', 'closed', 'lastFailure', 0, 'openCount', 0)
redis.call('XTRIM', KEYS[2], 'MAXLEN', '~', 1000)
return cjson.encode(state)
`;

/**
 * Atomically reopens `key` after a failed half-open trial: `isOpen: true`,
 * `openCount + 1`, `failures` raised to at least `failureThreshold`,
 * `lastFailure: now`, `version + 1`, and a published transition event --
 * this is always a real transition (a trial failing always reopens), so it
 * always writes and always publishes, unlike `CLOSE_SCRIPT`.
 */
const REOPEN_SCRIPT = `
local raw = redis.call('GET', KEYS[1])
local failures = 0
local openCount = 0
local errorRate = 0
local sampleCount = 0
local version = 0
if raw then
  local ok, decoded = pcall(cjson.decode, raw)
  if ok and decoded then
    failures = tonumber(decoded.failures) or 0
    openCount = tonumber(decoded.openCount) or 0
    errorRate = tonumber(decoded.errorRate) or 0
    sampleCount = tonumber(decoded.sampleCount) or 0
    version = tonumber(decoded.version) or 0
  end
end

local threshold = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local operation = ARGV[4]

openCount = openCount + 1
version = version + 1
local state = {
  failures = math.max(failures, threshold),
  lastFailure = now,
  isOpen = true,
  openCount = openCount,
  errorRate = errorRate,
  sampleCount = sampleCount,
  version = version
}
redis.call('SET', KEYS[1], cjson.encode(state), 'EX', tonumber(ARGV[1]))
redis.call('XADD', KEYS[2], '*', 'op', operation, 'version', version, 'state', 'open', 'lastFailure', now, 'openCount', openCount)
redis.call('XTRIM', KEYS[2], 'MAXLEN', '~', 1000)
return cjson.encode(state)
`;

/** Decodes one XREAD/XREADGROUP entry's flat `[field, value, field, value, ...]` array into a `TransitionEvent`, or `null` if it's missing a required field (defensive only -- every script this store writes always includes all of them). */
function decodeTransitionEvent(fields: string[]): TransitionEvent | null {
  const map: Record<string, string> = {};
  for (let i = 0; i + 1 < fields.length; i += 2) {
    map[fields[i]] = fields[i + 1];
  }
  const operation = map.op;
  const version = Number(map.version);
  const lastFailure = Number(map.lastFailure);
  const openCount = Number(map.openCount);
  if (operation === undefined || !Number.isFinite(version) || map.state === undefined) return null;
  return {
    operation,
    version,
    isOpen: map.state === 'open',
    lastFailure: Number.isFinite(lastFailure) ? lastFailure : 0,
    openCount: Number.isFinite(openCount) ? openCount : 0,
  };
}

/**
 * Redis-backed Store implementation. Import this from
 * `redeye/redis-store` so the core package has no hard dependency on
 * `ioredis` — only consumers who use RedisStore need it installed.
 *
 * Implements all seven optional capabilities: `recordFailureAtomic` /
 * `recordOutcomeAtomic` (exact counting), `claimTrial`/`releaseTrial`
 * (single-trial half-open recovery), `closeAtomic`/`reopenTrialFailureAtomic`
 * (versioned, transition-publishing writes for the two remaining
 * get-then-set paths), and `subscribeTransitions` (push invalidation for the
 * optional closed-state local cache). Custom `Store` implementations can
 * pick and choose; each is independently optional and falls back to a
 * best-effort non-atomic path with a one-time warning if omitted.
 *
 * Scripts are registered once per Redis client via `defineCommand`, so
 * ioredis sends a cached script hash (`EVALSHA`) on every call instead of
 * the full script body, with automatic fallback to `EVAL` if the script
 * cache is ever flushed.
 */
export class RedisStore implements Store {
  private readonly prefix: string;
  private subscriberConn: Redis | undefined;

  constructor(private readonly redis: Redis, options: { keyPrefix?: string } = {}) {
    this.prefix = options.keyPrefix ?? '';
    redis.defineCommand('redeyeRecordFailure', { numberOfKeys: 2, lua: RECORD_FAILURE_SCRIPT });
    redis.defineCommand('redeyeRecordOutcome', { numberOfKeys: 2, lua: RECORD_OUTCOME_SCRIPT });
    redis.defineCommand('redeyeReleaseTrial', { numberOfKeys: 1, lua: RELEASE_TRIAL_SCRIPT });
    redis.defineCommand('redeyeClose', { numberOfKeys: 2, lua: CLOSE_SCRIPT });
    redis.defineCommand('redeyeReopenTrialFailure', { numberOfKeys: 2, lua: REOPEN_SCRIPT });
  }

  private eventsKey(): string {
    return this.prefix + EVENTS_KEY;
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
    opts: { ttlSeconds: number; failureThreshold: number; now: number; operation: string },
  ): Promise<CircuitBreakerState> {
    const raw = await this.redis.redeyeRecordFailure(
      this.prefix + key,
      this.eventsKey(),
      Math.max(1, Math.ceil(opts.ttlSeconds)),
      opts.failureThreshold,
      opts.now,
      opts.operation,
    );
    return JSON.parse(raw) as CircuitBreakerState;
  }

  async recordOutcomeAtomic(
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
  ): Promise<CircuitBreakerState & { openedNow: boolean }> {
    const raw = await this.redis.redeyeRecordOutcome(
      this.prefix + key,
      this.eventsKey(),
      opts.decay,
      opts.success ? 0 : 1,
      opts.minimumCalls,
      opts.errorRateThreshold,
      Math.max(1, Math.ceil(opts.ttlSeconds)),
      opts.now,
      opts.operation,
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

  async closeAtomic(key: string, eventsKey: string, opts: { ttlSeconds: number; operation: string }): Promise<CircuitBreakerState> {
    const raw = await this.redis.redeyeClose(
      this.prefix + key,
      this.prefix + eventsKey,
      Math.max(1, Math.ceil(opts.ttlSeconds)),
      opts.operation,
    );
    return JSON.parse(raw) as CircuitBreakerState;
  }

  async reopenTrialFailureAtomic(
    key: string,
    eventsKey: string,
    opts: { ttlSeconds: number; failureThreshold: number; now: number; operation: string },
  ): Promise<CircuitBreakerState> {
    const raw = await this.redis.redeyeReopenTrialFailure(
      this.prefix + key,
      this.prefix + eventsKey,
      Math.max(1, Math.ceil(opts.ttlSeconds)),
      opts.failureThreshold,
      opts.now,
      opts.operation,
    );
    return JSON.parse(raw) as CircuitBreakerState;
  }

  /**
   * Subscribes to the shared transition stream via a blocking `XREAD`, on a
   * lazily-created duplicated connection (`ioredis` requires a connection
   * dedicated to blocking commands — reusing the main client would stall
   * every other command behind the block). Only one subscription per
   * `RedisStore` instance is supported; call the returned unsubscribe
   * function before subscribing again.
   *
   * On a connection error, the handler is invoked once with `null` (see the
   * `Store.subscribeTransitions` doc for what callers must do with that),
   * and the read loop retries with a fixed backoff — callers never need to
   * re-subscribe themselves.
   */
  async subscribeTransitions(eventsKey: string, handler: (event: TransitionEvent | null) => void): Promise<() => void> {
    if (this.subscriberConn) {
      throw new Error('RedisStore.subscribeTransitions: a subscription already exists on this instance; unsubscribe first.');
    }

    const fullKey = this.prefix + eventsKey;
    const conn = this.redis.duplicate();
    this.subscriberConn = conn;
    // Named so an operator (or a test) can identify and target this specific
    // blocking connection via CLIENT LIST, distinct from the main client and
    // any other connection sharing the same Redis server -- e.g. to verify
    // reconnect behavior by killing just this one. Best-effort: connections
    // that error before the SETNAME completes are still functional, just
    // unnamed.
    conn.client('SETNAME', 'redeye-subscriber').catch(() => {});

    let stopped = false;
    let lastId = '$';

    // ioredis auto-reconnects by default, and can do so transparently enough
    // that a command already in flight when the connection drops (e.g. this
    // blocking XREAD, killed from the server side) doesn't necessarily
    // reject its promise at all -- it can just resolve late once
    // reconnected. Catching only around `xread()` therefore isn't reliable
    // for detecting a drop; the connection's own error/close events are.
    // `stopped` guards against firing on our *own* deliberate teardown.
    conn.on('error', () => {
      if (!stopped) handler(null);
    });
    conn.on('close', () => {
      if (!stopped) handler(null);
    });

    const loop = async (): Promise<void> => {
      while (!stopped) {
        let result: [string, [string, string[]][]][] | null;
        try {
          result = (await conn.xread('BLOCK', 5000, 'STREAMS', fullKey, lastId)) as unknown as
            | [string, [string, string[]][]][]
            | null;
        } catch (error) {
          if (stopped) break;
          handler(null);
          await new Promise((resolve) => setTimeout(resolve, 1000));
          continue;
        }

        if (!result) continue; // BLOCK timeout, no new entries -- loop again

        for (const [, entries] of result) {
          for (const [id, fields] of entries) {
            lastId = id;
            const event = decodeTransitionEvent(fields);
            if (event) handler(event);
          }
        }
      }
    };

    void loop();

    return async () => {
      if (stopped) return;
      stopped = true;
      this.subscriberConn = undefined;
      // disconnect(), not quit(): quit() sends a command and waits for a
      // reply, which can't happen promptly while this connection is
      // (almost always) sitting inside a blocking XREAD -- Redis won't
      // process it until the current BLOCK cycle ends on its own.
      // disconnect() tears down the socket immediately.
      conn.disconnect();
    };
  }
}
