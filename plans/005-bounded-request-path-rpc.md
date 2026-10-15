# Plan 005: Ownership verification is bounded and shared, expired queued RPC work is dropped, and NFT responses are linear

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat a2522b7..HEAD -- src/api/routes.ts src/core/limiter.ts src/config.ts src/server.ts .env.example README.md test/api.test.ts test/limiter.test.ts`
> On changes, compare excerpts below with the live code; a mismatch in code you must edit is a STOP condition.

## Status

- **Priority**: P2
- **Effort**: S–M
- **Risk**: LOW
- **Depends on**: none
- **Category**: perf
- **Planned at**: commit `a2522b7`, 2026-10-15

## Why this matters

The service (TypeScript, Hono API, ERC-20/721 discovery on Ethereum over JSON-RPC billed **per call** at a 20 req/s client cap, 80M credits/month budget) has one request-path RPC feature with no bound: `GET /v1/:network/accounts/:address/erc721?verifyOwnership=true` calls `ownerOf` (via Multicall3, 1000 per call) for **every** held NFT on **every** request — no cache, no single-flight, no size cap. 5k users polling once a minute with that flag would need ~83 calls/s (4× the cap); one account holding 50k items costs 50 calls per request. On top of that, `withDeadline` only stops *waiting*: jobs still queued in the limiter when their request already timed out are executed later anyway, burning credits during exactly the slowdowns that caused the timeouts. Finally, the erc721 route groups items per collection with a nested `filter` (O(collections × items)), which blocks the event loop on large accounts.

## Current state

- `src/api/routes.ts:161-209` — the erc721 route:
```ts
    const heldItems = result.nftItems.filter((item) => item.heldBySeenLogs);
    const adapter = deps.adapters[result.network];
    const owners = verifyOwnership && heldItems.length > 0
      ? await withDeadline(
        deps.requestRpc.limiter.run(() => adapter.erc721Owners(heldItems, result.blockHash)),
        deps.requestRpc.deadlineMs,
        () => { throw unavailable('Ownership verification did not finish within the request deadline', 2); },
      )
      : [];
    ...
    const collections = [...bounds].map(([contract, { first, last }]) => ({
      contract, ...,
      tokens: result.nftItems.filter((item) => item.token === contract).map((item) => { ... }),
    }));
```
  Per-item output when verifying: `{ ...token, owner, ownedByAccount, ownerStatus }` or `{ ...token, ownerStatus: 'error' }`.
- `src/core/limiter.ts` (55 lines) — `createLimiter(maxActive, maxQueued, retryAfterS)` with `run(fn)`; queued entries are plain resolvers in `queue`. `withDeadline(promise, ms, onTimeout)` races a timer.
- `src/balances/worker.ts:107-115` — the other request-path user of `requestRpc.limiter` (first balance read). Its read is intentionally *kept* running after the deadline (it fills a cache that the next request uses), so dropping expired queued jobs must be **opt-in per call**.
- `src/config.ts` `request:` block (~lines 100–110) — pattern `int('NAME', 'default')` with one-line doc comments; `.env.example` lists env vars commented.
- Tests: `test/api.test.ts` `'verifies only ERC-721 items currently held according to seen logs'` (~line 194) — fake adapter with `erc721Owners`; there is no `test/limiter.test.ts` yet (create it).

Conventions: Node 26 native TS type stripping (`.ts` imports, `import type`, no enums/parameter properties). Compact style; one-line *why* comments. Error helper: `unavailable(message, retryAfterS)` → 503 with `Retry-After`.

## Commands you will need

| Purpose | Command | Expected |
|---|---|---|
| Install (fresh worktree) | `npm ci` or symlink `node_modules` from the main checkout | exit 0 |
| Typecheck | `npx tsc --noEmit -p .` | exit 0 |
| Targeted tests | `npx vitest run test/api.test.ts test/limiter.test.ts` | all pass |
| Full tests | `npx vitest run --testTimeout=600000` | all pass |

## Scope

**In scope**: `src/api/routes.ts` (erc721 route only), `src/core/limiter.ts`, `src/config.ts`, `src/server.ts` (only to pass new config), `.env.example`, `README.md` (API/env/Limits lines for the new cap only), `test/api.test.ts`, `test/limiter.test.ts` (create).

**Out of scope**: `src/balances/worker.ts` behaviour, discovery, store, multicall internals, the erc20 route.

## Git workflow

- Branch `advisor/005-bounded-request-path-rpc`; conventional commits (`perf: ...` / `fix: ...`), no Co-Authored-By/"Generated with" trailers; do not push.

## Steps

### Step 1: Opt-in expiry for queued limiter jobs

Add an optional second parameter `run(fn, opts?: { deadlineAt?: number })`. If a queued job's turn comes after `deadlineAt` (compare with `Date.now()`), do not call `fn`; reject with `unavailable('Request deadline passed while queued', retryAfterS)` and release the slot to the next waiter. Jobs without `deadlineAt` behave exactly as today.

**Verify**: `npx vitest run test/limiter.test.ts` → new tests pass.

### Step 2: Cap, cache and single-flight ownership verification

- Config: `MAX_VERIFY_OWNERSHIP_ITEMS` default `2000` (`request.maxVerifyOwnershipItems`), wired into the routes deps (add an optional field to `RouteDeps`, default 2000 when absent).
- In the route: if `heldItems.length > cap`, skip the RPC entirely (status stays 200) and add top-level `ownershipError: \`Too many held items to verify (${n} > ${cap})\``; each held item gets `ownerStatus: 'skipped'`.
- Single-flight + cache: module-level (per `createApp`) `Map<string, Promise<OwnerResult[]>>` keyed by `${network}:${account}:${blockHash}`; keep at most the last ~1000 keys (evict oldest by insertion order). A cached promise that rejects is removed.
- Pass `{ deadlineAt: Date.now() + deps.requestRpc.deadlineMs }` to `limiter.run`.

**Verify**: `npx vitest run test/api.test.ts` → all pass.

### Step 3: Linear grouping

Build `const byToken = new Map<Address, typeof result.nftItems>()` once, then use `byToken.get(contract) ?? []` instead of the nested `filter`. Output order and shape must be unchanged.

**Verify**: `npx vitest run test/api.test.ts` → all pass (existing erc721 tests unchanged).

## Test plan

- `test/limiter.test.ts` (new; model style on `test/api.test.ts` imports): (1) a job queued with `deadlineAt` in the past when its slot frees is rejected with statusCode 503 and `fn` is never called; (2) the next queued job still runs; (3) jobs without `deadlineAt` run as before.
- `test/api.test.ts` (pattern: `'verifies only ERC-721 items currently held...'`): (1) two concurrent verify requests for the same account/block call `erc721Owners` once; (2) a second request at the same block reuses the cache (still one call); (3) held items above the cap → no `erc721Owners` call, `ownershipError` present, items `ownerStatus: 'skipped'`.

## Done criteria

- [ ] `npx tsc --noEmit -p .` exits 0
- [ ] `npx vitest run --testTimeout=600000` passes, with ≥6 new tests
- [ ] `grep -n "MAX_VERIFY_OWNERSHIP_ITEMS" src/config.ts .env.example README.md` → matches in all three
- [ ] `grep -n "nftItems.filter((item) => item.token === contract)" src/api/routes.ts` → no match
- [ ] Only in-scope files changed

## STOP conditions

- The balance worker's limiter usage would change behaviour.
- The erc721 response shape for requests *without* `verifyOwnership` would change.

## Maintenance notes

- The ownership cache is per process and keyed by block hash, so it naturally invalidates when the served block changes (every 12 s with the tail).
- Deferred: a global credit budget / admission control for cold backfills of arbitrary addresses (the largest DoS lever; needs a product decision on denylists and budgets).
