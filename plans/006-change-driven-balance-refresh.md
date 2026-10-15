# Plan 006: Balances are re-read only when a Transfer touched the pair (plus an hourly sweep that skips zero balances)

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat bd4de97..HEAD -- src/balances/worker.ts src/store/store.ts src/core/types.ts src/api/routes.ts src/config.ts src/server.ts .env.example README.md REPORT.md test/follower.test.ts test/tail.test.ts test/store.test.ts test/api.test.ts`
> On changes, compare excerpts below with the live code; a mismatch in code you must edit is a STOP condition.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: MED
- **Depends on**: none
- **Category**: perf (RPC credits)
- **Planned at**: commit `bd4de97`, 2026-10-15

## Why this matters

This is a TypeScript PoC (ERC-20/721 discovery on Ethereum over Quicknode JSON-RPC, **billed per call**: 20 credits each, plan budget 80M credits/month). The service keeps a balance snapshot per (account, token) and a shared worker re-reads **every** ERC-20 pair of every *active* account (requested balances in the last 5 min) every 60 s via Multicall3 (1000 `balanceOf` per `eth_call`). At 5k users × 10 tokens that is ~50 calls/min ≈ **43M credits/month — ~80% of the whole budget** — and it grows linearly with tokens per account (Vitalik's address has 158; at 50 tokens/account the sweep alone is ~215M). Almost all of those reads return the same value: an ERC-20 balance changes (nearly) only through a `Transfer`, and the service already stores every Transfer of tracked accounts — finalized ones in `holdings.last_block`, unfinalized ones in `tail_transfers`.

After this plan:
- **Lever 1 (change-driven):** each 60 s cycle re-reads only pairs with a Transfer *after* their snapshot's block, pairs with no snapshot, and pairs of a short always-refresh token list (tokens whose balance changes without `Transfer`: WETH deposits emit `Deposit`, stETH rebases).
- **Lever 3 (skip zeros):** an hourly full sweep (`BALANCE_FULL_SWEEP_MS`) catches other non-Transfer changes, but skips pairs whose snapshot is an `ok` zero balance (a zero balance can only become non-zero through a Transfer, which lever 1 catches; rebasing tokens are on the always list).
- Reorg handling is unchanged: a reorg-kicked refresh still re-reads **all** active pairs with `replaceNewer`.
- Responses gain a per-token `balanceCurrent` flag so a balance read a few blocks ago, with no Transfer since, is honestly reported as current.

## Current state

- `src/balances/worker.ts` (~160 lines) — `createBalanceWorker(o)`. `refresh()` (around lines 62–92):
```ts
  const refresh = (): Promise<{ pairs: number; block?: bigint }> => {
    refreshing ??= (async () => {
      const replaceNewer = pendingReorg;
      pendingReorg = false;
      const cutoff = now() - o.activeTtlMs;
      for (const [account, seenAt] of active) if (seenAt < cutoff) active.delete(account);
      const cursor = await store.getFollowerCursor(network);
      if (!cursor || active.size === 0) return { pairs: 0 };
      const at = await o.tailTip?.() ?? cursor;
      const pairs = await store.erc20Pairs(network, [...active.keys()]);
      if (pairs.length === 0) return { pairs: 0, block: at.number };
      // One block for every pair, pinned by hash; a balance read at N is never reported for a later block.
      const results = await adapter.erc20BalancePairs(pairs, at.hash);
      const rows = pairs.map((pair, i) => ({ ...pair, result: results[i] }))
        .filter((row) => row.result.status !== 'error' || !row.result.transient);
      if (rows.length > 0) await store.saveBalances(network, at, rows, replaceNewer ? { replaceNewer: true } : undefined);
      return { pairs: pairs.length, block: at.number };
    })().then((result) => { lastRefresh = { at: now(), ...result }; return result; })
      .finally(() => { refreshing = undefined; });
    return refreshing;
  };
```
  `forAccount(account, tokens, at)` (request path) reads only tokens with **no** snapshot; keep it as is. `stats()` returns `{ activeAccounts, lastRefresh }` and is exposed on `/health` (`src/api/routes.ts`, `balances:` block of the health route).
- `src/store/store.ts` `erc20Pairs(network, accounts)` (~line 438) — in batches of 1000 accounts: selects `(holder, token)` from `holdings` where `standard = 'erc20'`, then `(from, to, token)` from `tail_transfers` where `standard = 'erc20'` and from/to in the batch; returns distinct pairs. `holdings.last_block` is the max block of any finalized Transfer of that (holder, token) (upsert uses `GREATEST`). `tail_transfers.block_number` is the block of each unfinalized Transfer; rows ≤ the follower cursor are pruned in the same transaction that folds them into `holdings`, so `max(holdings.last_block, max tail block)` is always the pair's latest Transfer block.
- `store.balances(network, account)` returns `BalanceSnapshot { token, result, blockNumber, blockHash }` (`src/core/types.ts:245`); `result` is `{ status: 'ok', balance: bigint } | { status: 'error', error, transient? }`.
- `src/api/routes.ts` erc20 route (~lines 145–183) builds per-token fields `balance`, `balanceStatus`, `balanceBlock`, `balanceBlockHash` (tail on) and the top-level `balancesAtBlockHeight`. `holding.lastBlock` there already includes the tail overlay when `finality=latest`.
- `src/config.ts` `balances:` block (~line 100): `activeAccountTtlMs` (`ACTIVE_ACCOUNT_TTL_MS`), `refreshMs` (`BALANCE_REFRESH_MS`). Wiring in `src/server.ts` ~line 43.
- Tests: `test/follower.test.ts` — `'5k accounts x 10 tokens: one follower tick and one refresh cost O(ranges + pairs/1000) calls; 5k requests then cost zero'` (line ~76), `'balances: concurrent first requests share one read; different accounts are batched by the refresh'` (~345), `'balances: a first read fetches only tokens without snapshots'` (~389), `'balances: a transient failed refresh chunk does not replace its last good snapshot'` (~416). `test/tail.test.ts` has `'a reorg refresh replaces balances from a newer orphaned tip'` and the latest-overlay balance test. Use the `stack()` helper + `FakeChain` (`test/fake-chain.ts`) and its RPC/pair counters.

Conventions: Node 26 native TS type stripping (`.ts` imports, `import type`, no enums/parameter properties). Compact style, one-line *why* comments. Drizzle query builder as in `store.ts` (no raw SQL strings beyond `sql\`\`` fragments already used there). No schema change is needed.

## Commands you will need

| Purpose | Command | Expected |
|---|---|---|
| Install (fresh worktree) | symlink `node_modules` from the main checkout | — |
| Typecheck | `npx tsc --noEmit -p .` | exit 0 |
| Targeted | `npx vitest run test/follower.test.ts test/tail.test.ts test/store.test.ts test/api.test.ts --testTimeout=600000` | all pass |
| Full | `npx vitest run --testTimeout=600000` | all pass (currently 120) |

## Scope

**In scope**: `src/balances/worker.ts`, `src/store/store.ts` (new read method only; `erc20Pairs` may be refactored to share code), `src/core/types.ts` (Store method + stats/snapshot types), `src/api/routes.ts` (erc20 route fields + `/health` balance stats), `src/config.ts`, `src/server.ts`, `.env.example`, `README.md` (env table, API fields, "Shared balance worker", Limits "Balance cadence"/"Polling cost"/"Freshness of balances"/"Balances without Transfer"), `REPORT.md` §4 cost (balance line) only, tests in `test/`.

**Out of scope**: follower, tail, discovery service, multicall internals, `saveBalances` semantics, schema/migrations, the request-path `forAccount` behaviour.

## Git workflow

- Conventional commits (`perf: ...`, `docs: ...`, `test: ...`), no Co-Authored-By/"Generated with" trailers; do not push.

## Steps

### Step 1: Store read for the refresh decision

Add `Store.erc20RefreshState(network, accounts): Promise<{ account, token, lastTransferBlock: bigint, snapshot?: { blockNumber: bigint; zero: boolean; ok: boolean } }[]>` — for each distinct ERC-20 pair of these accounts (same pair set as `erc20Pairs`): `lastTransferBlock = max(holdings.last_block, max tail_transfers.block_number for that pair)` and the pair's `balance_snapshots` row if any (`zero` = ok and balance = 0; `ok` = balance not null). Batch by 1000 accounts like `erc20Pairs`; at most 3 queries per batch (holdings, tail grouped by pair with `max(block_number)` — from-side and to-side, snapshots). Keep `erc20Pairs` working (it is used elsewhere; you may implement it on top of the new method).

**Verify**: `npx tsc --noEmit -p .` → exit 0; add a store test (pattern: existing balance tests in `test/store.test.ts`) asserting `lastTransferBlock` takes the tail max over holdings and that `zero`/`ok` are reported; `npx vitest run test/store.test.ts` → pass.

### Step 2: Change-driven selection in `refresh()`

Config (one-line doc comments, `.env.example` commented defaults, README env table):
- `BALANCE_FULL_SWEEP_MS` default `3600000`.
- `BALANCE_ALWAYS_REFRESH_TOKENS` default `0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,0xae7ab96520de3a18e5e111b5eaab095312d7fe84` (WETH, stETH; lowercase, comma-separated, may be empty).

In `refresh()`, after computing `at`, select pairs to read:
- **reorg pending** (`replaceNewer`): all pairs (today's behaviour).
- otherwise a pair is read when ANY holds:
  1. no snapshot;
  2. `lastTransferBlock > snapshot.blockNumber` (a Transfer after the read — lever 1);
  3. token ∈ always-refresh list;
  4. full sweep due (`now() - lastFullSweepAt >= fullSweepMs`, or never swept) AND NOT (`snapshot.ok && snapshot.zero`) (lever 3). Error snapshots are retried in the full sweep. Set `lastFullSweepAt = now()` only after a full-sweep cycle's reads completed (or there was nothing to read).
- Read and save exactly as today (same `at`, same transient-row filter, same `replaceNewer` flag).
- `lastRefresh` gains `candidates` (pairs considered), `read` (pairs read; keep `pairs` = read for backward compatibility) and `fullSweep: boolean`; expose them in `/health`.

**Verify**: `npx vitest run test/follower.test.ts --testTimeout=600000` → pass. If an existing test asserted that a second refresh with no new Transfers re-reads everything, update only that expectation and note it.

### Step 3: Honest freshness flag in the erc20 response

In the erc20 route, per token with a snapshot, add `balanceCurrent: boolean` = `true` when the snapshot was read at the served block (number and hash equal), OR (`balanceStatus === 'ok'` AND `holding.lastBlock <= snapshot.blockNumber` AND the token is not in the always-refresh list). Pass the always-refresh list into `RouteDeps` (optional field; default empty). Keep `balancesAtBlockHeight` exactly as today; add top-level `balancesCurrent` = every token's `balanceCurrent` (only when balances were requested). Document both in the README API section: "`balanceCurrent` means no Transfer of this token touched the account between `balanceBlock` and `blockHeight`; tokens that change without Transfer (always-refresh list) are only current when read at `blockHeight`".

**Verify**: `npx vitest run test/api.test.ts test/tail.test.ts` → pass.

### Step 4: Docs

README: rewrite the "Shared balance worker" bullet and the Limits bullets listed in Scope to describe change-driven refresh, the hourly sweep that skips zero balances, and the always-refresh list. REPORT.md §4: replace the balance-sweep cost line with the new estimate and its assumption, e.g. "≈ touched pairs per minute (typically ≪ 1% of pairs) + hourly sweep of non-zero pairs (50k/1000 = 50 calls/h ≈ 0.7M/month) + always-refresh pairs held (≤ 1 call/min per 1000 such pairs) — a few M/month instead of ~43M, and independent of spam tokens with zero balances". Keep numbers labelled as estimates.

**Verify**: `grep -n "BALANCE_FULL_SWEEP_MS\|BALANCE_ALWAYS_REFRESH_TOKENS" README.md .env.example src/config.ts` → matches in all three.

## Test plan

In `test/follower.test.ts` (pattern: the `'balances: …'` tests and the 5k test, using `stack()`, `FakeChain` call counters, and the test clock `clock.t`):
1. "a refresh with no new Transfers reads nothing": request balances (first read), run `balances.refresh()` twice with the clock advanced < full-sweep interval → second refresh issues 0 `eth_call`.
2. "a Transfer after the snapshot re-reads only that pair": add a transfer for one (account, token) in a new finalized or tail block, advance the follower/tail, refresh → exactly that pair is read (assert pair count via the fake chain), and its snapshot block advances.
3. "the hourly sweep re-reads non-zero pairs and skips zero balances": snapshots with one zero and one non-zero balance, advance the clock past `fullSweepMs`, refresh → only the non-zero pair is read; `lastRefresh.fullSweep === true`.
4. "always-refresh tokens are read every cycle".
5. "a reorg-kicked refresh still re-reads every pair" (or rely on and keep the existing tail reorg balance test; add an assertion of the read count).
6. Extend the 5k test: after the first refresh, a second refresh with no new Transfers costs 0 calls (record the number in the test name/comment).
In `test/api.test.ts`: `balanceCurrent` true for a snapshot older than `blockHeight` with `lastSeenBlock <= balanceBlock`; false when `lastSeenBlock > balanceBlock`; false for an always-refresh token read at an older block.

## Done criteria

- [ ] `npx tsc --noEmit -p .` exits 0
- [ ] `npx vitest run --testTimeout=600000` passes, with ≥7 new tests
- [ ] Test 1 proves 0 `eth_call` for an unchanged refresh; the 5k test proves the second refresh costs 0
- [ ] Only in-scope files changed

## STOP conditions

- `holdings.last_block` turns out not to be the max finalized Transfer block for the pair (check the upsert in `upsertAggregates`), or tail rows are not pruned atomically with the fold — the change detection would be unsound.
- Any change to `saveBalances` semantics or the request path seems necessary.
- An existing test assertion other than "second refresh re-reads everything" must change.

## Maintenance notes

- Soundness rests on "an ERC-20 balance changes only via Transfer, except listed tokens"; the hourly sweep bounds the error for unlisted non-standard tokens (rebasing, fee-on-transfer edge cases, admin mints without events). Extend the list rather than lowering the sweep interval.
- A reorg always forces a full re-read, so a snapshot from an orphaned block is never trusted by the change test.
- Deferred: packing more pairs per call (2000 per `aggregate3`, or a state-override balance-checker contract) — lever 2, measured separately.
