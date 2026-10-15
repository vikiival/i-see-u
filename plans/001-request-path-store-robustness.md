# Plan 001: First-request tracking is race-safe, bad network names 404, and the Postgres pool fails fast

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat a2522b7..HEAD -- src/store/store.ts src/store/postgres.ts src/discovery/service.ts src/config.ts src/server.ts src/api/routes.ts test/store.test.ts test/discovery.test.ts test/api.test.ts .env.example`
> If any in-scope file changed since this plan was written, compare the
> "Current state" excerpts against the live code before proceeding; on a
> mismatch, treat it as a STOP condition.

## Status

- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: bug
- **Planned at**: commit `a2522b7`, 2026-10-15

## Why this matters

This is a TypeScript PoC service (ERC-20/721 token discovery on Ethereum over JSON-RPC, Hono API, Postgres via Drizzle) that must serve 5k users polling once a minute, possibly from several processes behind a load balancer. Three small defects on the request path turn normal traffic into HTTP 500s or unbounded hangs:

1. Two concurrent *first* requests for the same address both try to `INSERT` into `tracked_accounts`; the second hits the primary-key unique violation and the client gets a 500.
2. Network names that exist on `Object.prototype` (`constructor`, `toString`, `__proto__`) pass the "unknown network" guard and crash with a TypeError → 500 instead of 404.
3. The `pg.Pool` uses defaults (10 connections, `connectionTimeoutMillis: 0` = wait forever). Under load, requests queue silently forever instead of failing fast with a 503.

Also, a small hot-loop inefficiency in `erc20Pairs` (`Array.includes` inside a loop over tail rows) is fixed here because it lives next to the tracking code.

## Current state

- `src/store/store.ts` — Drizzle store, all DB access. `trackAccount` at lines 194–214:

```ts
    async trackAccount(network, account, at) {
      return db.transaction(async (tx): Promise<Tracking> => {
        // FOR SHARE conflicts with the follower's FOR UPDATE: its commit is entirely before or after this one.
        const cursor = await getFollowerCursor(tx, network, 'share');
        if (!cursor) throw Object.assign(new Error('Follower has no cursor yet'), { statusCode: 503, retryAfterS: 5 });
        const existing = await getTarget(tx, network, account);
        if (existing !== undefined) return { target: existing, progress: await getProgress(tx, network, account) };
        ...
        await tx.insert(schema.trackedAccounts).values({ network, account, backfillTarget: cursor.number, trackedAt: at });
        return { target: cursor.number, progress };
      });
    },
```

  Two transactions both holding `FOR SHARE` on the cursor do not block each other, so both can see `existing === undefined`. The PK is `(network, account)` (see `src/store/schema.ts`).

  `erc20Pairs` at lines 420–452 contains `if (!batch.includes(account as Address)) continue;` inside a loop over every tail row (`batch` has up to 1000 entries).

- `src/discovery/service.ts:155-162`:

```ts
    async discover(network: string, input: string, requestedFinality?: 'latest' | 'finalized'): Promise<DiscoveryResult | SyncingResult> {
      const adapter = deps.adapters[network];
      const follower = deps.followers[network];
      if (!adapter || !follower) {
        throw Object.assign(new Error(`Unknown network: ${network}`), { statusCode: 404 });
      }
```

  `deps.adapters` / `deps.followers` are built with `Object.fromEntries` in `src/server.ts`, so they inherit `Object.prototype`.

- `src/api/routes.ts` — besides calling `discover`, the routes index `deps.balances[result.network]` and `deps.adapters[result.network]` only *after* discover succeeded, so they are safe once discover validates.
- `src/store/postgres.ts:22-23`: `const pool = new Pool({ connectionString: databaseUrl });`
- `src/api/routes.ts:99-115` — `app.onError` maps any error carrying a numeric `statusCode` (400–599) to that status and sets `Retry-After` from `retryAfterS`. Unknown errors become 500. Error helper to reuse: `unavailable(message, retryAfterS)` in `src/core/limiter.ts:2` returns an Error with `statusCode: 503`.
- `src/config.ts` — env config, pattern `int('NAME', 'default')`; see the `request:` block around lines 100–110 for style. `.env.example` documents env vars with commented defaults.

Conventions: Node 26 native TypeScript type stripping — use `.ts` import extensions, `import type` for types, no enums, no parameter properties, no namespaces. Compact style, sparse one-line comments explaining *why*. Tests use vitest + PGlite (`test/store-fixture.ts` → `createTestStore()`); PGlite has a single connection so true concurrency cannot be reproduced — tests must simulate the race directly (see Test plan).

## Commands you will need

| Purpose   | Command | Expected on success |
|-----------|---------|---------------------|
| Install (fresh worktree only) | `npm ci` (or symlink `node_modules` from the main checkout if offline) | exit 0 |
| Typecheck | `npx tsc --noEmit -p .` | exit 0, no output |
| Targeted tests | `npx vitest run test/store.test.ts test/discovery.test.ts test/api.test.ts` | all pass |
| Full tests | `npx vitest run --testTimeout=600000` | all pass (one 5k-account test can take minutes on a loaded machine; that is not a failure) |

## Scope

**In scope**:
- `src/store/store.ts` (only `trackAccount` and `erc20Pairs`)
- `src/store/postgres.ts`
- `src/discovery/service.ts` (only the network lookup at the top of `discover`)
- `src/config.ts`, `src/server.ts` (only to pass pool settings), `.env.example`
- `src/api/routes.ts` (only to map pool timeout errors to 503, if done there)
- `test/store.test.ts`, `test/discovery.test.ts`, `test/api.test.ts`

**Out of scope**:
- The follower, tail, balance worker, schema and migrations (`drizzle/`). No schema change is needed.
- The handoff semantics: the target written by the *winning* insert must stay the follower cursor read under `FOR SHARE`.

## Git workflow

- Branch: `advisor/001-request-path-store-robustness`
- Conventional commits, matching `git log` (e.g. `fix: load follower cursor before serving to avoid startup 503`). No Co-Authored-By or "Generated with" trailers.
- Do NOT push.

## Steps

### Step 1: Make `trackAccount` idempotent

In `trackAccount`, change the insert to `.onConflictDoNothing().returning({ target: schema.trackedAccounts.backfillTarget })`. If it returned no row, another transaction won: re-read with `getTarget(tx, ...)` and `getProgress(tx, ...)` and return those (the winner's target). Keep the legacy-progress cleanup branch as is (it runs only when no tracking row was visible).

Note: under READ COMMITTED, a conflicting insert waits for the other transaction and then does nothing; the subsequent `getTarget` in the same transaction then sees the committed row (each statement gets a fresh snapshot). If the re-read still returns undefined, throw `unavailable('Account tracking raced; retry', 1)` (import from `../core/limiter.ts`).

**Verify**: `npx tsc --noEmit -p .` → exit 0.

### Step 2: Own-property network lookup

In `discover`, look up with `Object.hasOwn(deps.adapters, network) ? deps.adapters[network] : undefined` (same for `deps.followers` and `deps.tails?.[network]`). Keep the 404 error unchanged.

**Verify**: `npx tsc --noEmit -p .` → exit 0.

### Step 3: Pool sizing and fail-fast

- `src/config.ts`: add a `db:` block with `poolMax: int('DB_POOL_MAX', '20')` and `connectTimeoutMs: int('DB_CONNECT_TIMEOUT_MS', '5000')`, with one-line doc comments in the existing style.
- `src/store/postgres.ts`: `openPostgresStore(databaseUrl, opts?: { max?: number; connectionTimeoutMillis?: number })` → `new Pool({ connectionString: databaseUrl, max, connectionTimeoutMillis })`. Wire it from `src/server.ts` (find the call to `openPostgresStore`).
- Map the pool-acquire timeout to 503: in `routes.ts` `onError`, before the 500 branch, treat an error whose message matches `/timeout exceeded when trying to connect/i` (the message `pg-pool` uses) as `503` with `Retry-After: 2` and body `{ error: 'Database busy' }`.
- `.env.example`: add commented `# DB_POOL_MAX=20` and `# DB_CONNECT_TIMEOUT_MS=5000` lines next to `DATABASE_URL`.

**Verify**: `npx tsc --noEmit -p .` → exit 0; `grep -n "DB_POOL_MAX" src/config.ts .env.example` → 2 matches.

### Step 4: `erc20Pairs` membership via Set

Inside the batch callback build `const members = new Set(batch);` and replace `batch.includes(...)` with `members.has(...)`.

**Verify**: `npx vitest run test/store.test.ts` → all pass.

## Test plan

- `test/store.test.ts`: "trackAccount is idempotent when another transaction already inserted the row" — call `store.trackAccount` once, then directly insert is impossible to race on PGlite; instead simulate: insert a `tracked_accounts` row via `db.insert(schema.trackedAccounts)` *with a different backfillTarget*, then advance nothing and call `trackAccount` — it must return the existing target (this already passes via the `existing` path). The real regression test: monkey-patch is not available, so add a test that calls `trackAccount` twice concurrently with `Promise.all` and asserts both resolve to the same target and no throw. (On PGlite they serialize; the test documents the contract.) Model after the existing trackAccount tests in `test/store.test.ts` (search `trackAccount`).
- `test/api.test.ts`: `GET /v1/constructor/accounts/<valid address>/erc20` → 404 with `{ error: 'Unknown network: constructor' }`; same for `toString`. Model after the existing unknown-network test (search `Unknown network` in `test/api.test.ts` or `test/discovery.test.ts`).
- `test/api.test.ts`: an error thrown by discover with message `timeout exceeded when trying to connect` → 503 with `Retry-After: 2`. Model after an existing onError mapping test (search `Retry-After`).

## Done criteria

- [ ] `npx tsc --noEmit -p .` exits 0
- [ ] `npx vitest run test/store.test.ts test/discovery.test.ts test/api.test.ts` passes, including the new tests
- [ ] `grep -n "onConflictDoNothing" src/store/store.ts` shows the trackAccount insert
- [ ] `grep -n "batch.includes" src/store/store.ts` → no matches
- [ ] `git status` shows only in-scope files modified

## STOP conditions

- `trackAccount` or `discover` don't match the excerpts.
- Making the insert idempotent seems to require changing the cursor lock mode (`FOR SHARE`) — that lock is the handoff invariant; stop.
- Any existing test needs its *assertions* changed (not just setup) to pass.

## Maintenance notes

- The handoff invariant: an account's `backfill_target` is the follower cursor at the moment it was tracked, and the follower refuses to commit a range if a tracked account was unknown to it. Anything touching `trackAccount` must preserve that.
- The pool limit interacts with the advisory leader lock, which holds one connection permanently (`src/store/advisory-lock.ts`).
- Deferred: folding `getTracking` into the read snapshot to cut round trips per GET.
