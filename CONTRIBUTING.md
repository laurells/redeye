# Contributing to redeye

Thanks for considering a contribution. This project is small in scope on purpose (see [README: What redeye deliberately does not try to be](README.md#what-redeye-deliberately-does-not-try-to-be)) — the fastest way to get a PR merged is to fix a bug, tighten a reliability guarantee, or improve docs/tests, rather than add new surface area.

## Setup

```bash
npm install
```

Requires Node.js >= 18.

## Running tests

Unit tests run against in-memory fakes and need nothing external:

```bash
npm test
```

Integration tests run against a real Redis instance and exercise `RedisStore`'s actual Lua scripts and `SET ... NX` trial-claim logic — not a reimplementation of them:

```bash
docker compose up -d
npm run test:integration
docker compose down
```

Both must pass before a PR is merged; CI runs both automatically (see `.github/workflows/ci.yml`) across Node 18/20/22.

## Other useful commands

```bash
npm run lint   # tsc --noEmit
npm run build  # compiles src/ to dist/
```

## Making a change

1. Fork and branch from `main`.
2. If you're changing behavior (not just docs/tests), add or update tests — ideally at the level that actually exercises the guarantee. If you're touching `RedisStore` or anything claiming atomicity, add an integration test against real Redis, not just an in-memory fake, since fakes can't catch a subtly wrong Lua script.
3. Update the README if you're changing documented behavior, options, or the reliability model. If you're changing the public API, add an entry under `[Unreleased]` in `CHANGELOG.md` (see [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format).
4. Open a PR describing *why* the change is needed, not just what it does — especially for anything touching the distributed-mode reliability guarantees (atomic counting, single-trial half-open, backoff). Correctness reasoning matters more than the diff here.

## Reporting bugs / reliability issues

Open a GitHub issue. If it's a correctness bug in distributed mode (e.g. a race in the atomic operations, or the half-open guarantee not holding), please include:

- Whether it reproduces with `RedisStore` or a custom `Store` implementation.
- Whether it reproduces in the unit suite (fakes) or only against real Redis.
- The `strategy` (`consecutive` or `errorRate`) and relevant options.

## Versioning

This project follows [Semantic Versioning](https://semver.org/). Breaking changes to the public API (anything exported from `src/index.ts` or `src/stores/redis-store.ts`) require a major version bump.
