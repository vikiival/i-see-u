# Plan 004: Balances recover after a reorg, survive a failed batch, and first reads fetch only missing tokens

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat a2522b7..HEAD -- src/balances/worker.ts src/chains/evm/multicall.ts src/store/store.ts src/core/types.ts src/server.ts test/multicall.test.ts test/follower.test.ts test/tail.test.ts test/store.test.ts`
> On changes, compare excerpts below with the live code; a mismatch in code you must edit is a STOP condition.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: MED
- **Depends on**: none
- **Category**: bug / perf
- **Planned at**: commit `a2522b7`, 2026-10-15

## Why this matters

The service (TypeScript, ERC-20/721 discovery on Ethereum over JSON-RPC billed **per call**, Postgres via Drizzle) keeps a balance snapshot per (account, token) in `balance_snapshots`. A shared worker re-reads every active account's ERC-20 balances every 60 s through Multicall3 `aggregate3` (1000 `balanceOf` per `eth_call`), pinned by block hash at the realtime tail tip. Four defects:

1. **Orphaned balances after a reorg to a shorter chain.** The upsert only overwrites when `excluded.block_number >= stored.block_number`. After a reorg from tip 100 to new tip 99, the reorg-triggered refresh at 99 is rejected, so balances read at the orphaned block 100 keep being served until the chain passes 100 again — exactly the case the reorg refresh exists for.
2. **One failed batch discards the whole sweep.** Batches run in parallel and results are saved only after *all* succeed; a batch that exhausts retries throws, the other (billed) batches are thrown away, and every balance goes stale until the next sweep repeats all of them.
3. **First-read amplification.** When any single token of an account lacks a snapshot (e.g. a new spam airdrop seen in the tail), the request path re-reads **all** of the account's tokens instead of only the missing ones.
4. **Malformed outer multicall result → HTTP 500.** `aggregateChunk` decodes the `aggregate3` result *after* the RPC client accepted it (it only checks "is hex"), so a truncated response throws a plain Error that is neither retried nor failed over to another endpoint.

## Current state

- `src/balances/worker.ts` (148 lines):
  - `readNow(account, tokens, at)` lines 53–56 reads `tokens` and saves them.
  - `refresh()` lines 58–76: `const at = await o.tailTip?.() ?? cursor; const pairs = await store.erc20Pairs(...); const results = await adapter.erc20BalancePairs(pairs, at.hash); await store.saveBalances(network, at, pairs.map(...));`
  - `kick()` lines 128–132 — called by `src/server.ts` on a tail reorg: `onAdvance: (_tip, reorg) => { if (reorg) balances[adapter.network].kick(); }` (server.ts ~line 71).
  - `forAccount(account, tokens, at)` lines 95–124: if `tokens.some((token) => !byToken.has(token))` it calls `readNow(account, tokens, at)` with **all** tokens (single-flight per account per block via the `reads` map).
- `src/store/store.ts:454-473` `saveBalances(network, block, rows)`:
```ts
      }))).onConflictDoUpdate({
        target: [schema.balanceSnapshots.network, schema.balanceSnapshots.account, schema.balanceSnapshots.token],
        set: { balance: sql`excluded.balance`, error: sql`excluded.error`, blockNumber: sql`excluded.block_number`, blockHash: sql`excluded.block_hash` },
        setWhere: sql`excluded.block_number >= ${schema.balanceSnapshots.blockNumber}`,
      }));
```
  `Store.saveBalances` type: `src/core/types.ts:310`.
- `src/chains/evm/multicall.ts`:
  - `aggregateChunk(rpc, calls, blockHash, multicall3)` lines ~26–50: `rpc.request<Hex>('eth_call', ..., { validate: (value) => typeof value === 'string' && isHex(value) })`, then `decodeFunctionResult(...)` and a length check **outside** `validate`; the catch rethrows non-splittable errors.
  - `readCalls(...)` lines ~53–83: chunks of `batchSize`, two workers, `await Promise.all(...)`, then maps outcomes.
  - `readErc20BalancePairs` / `readErc721Owners` wrap `readCalls`.
  - The RPC client (`src/rpc/client.ts`) treats a `validate` returning `false` as a bad response from that endpoint and retries / fails over (see how `eth_getLogs` uses `validate: Array.isArray` in `src/chains/evm/logs.ts`).
- Tests: `test/multicall.test.ts` (pattern: `'out-of-gas failures recursively halve and preserve input order'` at line ~116 uses a scripted fake RPC); `test/follower.test.ts` `'balances: concurrent first requests share one read; different accounts are batched by the refresh'` (~line 261) exercises the worker end-to-end; `test/tail.test.ts` `'API finality overlays tail-only tokens and balances at the tip hash...'` (~line 258).

Conventions: Node 26 native TS type stripping (`.ts` imports, `import type`, no enums/parameter properties). Compact style; one-line *why* comments.

## Commands you will need

| Purpose | Command | Expected |
|---|---|---|
| Install (fresh worktree) | `npm ci` or symlink `node_modules` from the main checkout | exit 0 |
| Typecheck | `npx tsc --noEmit -p .` | exit 0 |
| Targeted tests | `npx vitest run test/multicall.test.ts test/store.test.ts test/tail.test.ts test/follower.test.ts --testTimeout=600000` | all pass |
| Full tests | `npx vitest run --testTimeout=600000` | all pass |

## Scope

**In scope**: `src/balances/worker.ts`, `src/chains/evm/multicall.ts`, `src/store/store.ts` (only `saveBalances`), `src/core/types.ts` (only the `saveBalances` signature and `BalanceWorker`-adjacent types if any), `src/server.ts` (only the reorg kick wiring), `test/multicall.test.ts`, `test/store.test.ts`, `test/follower.test.ts`, `test/tail.test.ts`.

**Out of scope**: `src/tail/*`, `src/follower/*`, `erc20Pairs`, routes, schema/migrations (no schema change).

## Git workflow

- Branch `advisor/004-balances-reorg-safe-and-partial`; conventional commits (`fix: ...`), no Co-Authored-By/"Generated with" trailers; do not push.

## Steps

### Step 1: Validate the outer multicall result inside the RPC boundary

In `aggregateChunk`, move `decodeFunctionResult` + the length check into the `validate` callback (return `false` if decoding throws or `decoded.length !== calls.length`), keep the decoded value (e.g. decode again after the request, or capture it in a closure variable set by `validate`). A malformed outer result must now surface as an RPC-client validation failure (retried / failed over), not a plain Error.

**Verify**: `npx vitest run test/multicall.test.ts` → all pass.

### Step 2: Per-batch results instead of all-or-nothing

Change `readCalls` so a chunk whose `aggregateChunk` throws yields `{ status: 'error', error: <short reason> }` for every call in that chunk (use the existing `shortReason`), instead of rejecting the whole `Promise.all`. **Exception**: if *every* chunk failed, rethrow the first error (so callers still see an outage as an error, e.g. 503 on the request path).

**Verify**: `npx vitest run test/multicall.test.ts` → all pass.

### Step 3: Reorg-aware snapshot overwrite

- `saveBalances(network, block, rows, opts?: { replaceNewer?: boolean })`. With `replaceNewer: true` the `setWhere` becomes `excluded.block_number >= stored.block_number OR stored.block_number > excluded.block_number` — i.e. always overwrite (the read is at the current canonical tip, so any higher stored block is orphaned). Without the flag, keep today's rule.
- In the worker: add `kick(opts?: { reorg?: boolean })`. Track a `pendingReorg` flag; the next `refresh()` that starts after a reorg kick passes `replaceNewer: true` (and clears the flag). In `src/server.ts` change the tail wiring to `kick({ reorg: true })`. The 60 s sweep and the request path keep the default rule.
- Also: if a *saved* error result (`status: 'error'`) would overwrite an `ok` snapshot at the same or lower block in a normal sweep, keep that behaviour unchanged (out of scope).

**Verify**: `npx vitest run test/store.test.ts test/tail.test.ts` → all pass.

### Step 4: Read only missing tokens on the request path

In `forAccount`, compute `missing = tokens.filter((t) => !byToken.has(t))` and call `readNow(account, missing, at)`. Keep the per-account single-flight; since a concurrent request might miss a different subset, key the `reads` map by `account` as today but, if a read is already in flight for the account, await it and then read any *still* missing tokens once more (at most one extra read).

**Verify**: `npx vitest run test/follower.test.ts --testTimeout=600000` → all pass (the existing "concurrent first requests share one read" test must still see one read).

## Test plan

- `test/multicall.test.ts` (pattern: the out-of-gas test): (a) a truncated-but-hex outer result on the first endpoint is rejected by validation and the call succeeds on retry/second response; (b) with 3 chunks where the 2nd throws a transient error after retries, the result has `ok` for chunks 1 and 3 and `error` for every call in chunk 2, in input order; (c) all chunks failing rejects.
- `test/store.test.ts`: `saveBalances` at block 100 then at 99 without the flag keeps 100; with `{ replaceNewer: true }` stores 99.
- `test/tail.test.ts` (pattern: the `'API finality overlays ... balances at the tip hash'` test): after a reorg to a shorter chain and the worker refresh with the reorg kick, served balances carry the new tip's `blockNumber`/`blockHash`.
- `test/follower.test.ts` or a worker-level test: an account with 3 snapshotted tokens and 1 new token → the request-path read requests exactly 1 pair (assert via the fake chain's call/pair counter).

## Done criteria

- [ ] `npx tsc --noEmit -p .` exits 0
- [ ] `npx vitest run --testTimeout=600000` passes, with ≥5 new tests
- [ ] `grep -n "replaceNewer" src/store/store.ts src/balances/worker.ts` → matches in both
- [ ] Only in-scope files changed

## STOP conditions

- Moving decode into `validate` is impossible with the RPC client's `validate` signature (check `src/rpc/client.ts` first) — report instead of reworking the client.
- An existing test's assertions (not setup) must change.
- Partial-results change would alter `erc721Owners` semantics in a way the API test `'verifies only ERC-721 items currently held...'` rejects.

## Maintenance notes

- `replaceNewer` is safe only because a reorg refresh reads at the newest canonical tip; never pass it from the request path.
- Partial-batch results mean balances can be a mix of fresh `ok` and `error` rows within one sweep; the API already labels each balance with its block.
- Deferred: refreshing only pairs touched since the last sweep, and lower-rate refresh of zero/never-moved pairs (credit savings at scale).
