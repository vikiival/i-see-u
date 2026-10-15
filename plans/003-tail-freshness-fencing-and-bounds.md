# Plan 003: "latest" is never served from a frozen tail, tail writes are fenced, and the tail cannot grow without bound

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat a2522b7..HEAD -- src/tail/tail.ts src/discovery/service.ts src/store/store.ts src/core/types.ts src/config.ts src/server.ts .env.example test/tail.test.ts test/store.test.ts README.md`
> On changes, compare excerpts below with the live code; a mismatch in the code you must edit is a STOP condition.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: MED
- **Depends on**: none (plan 002 makes `follower.status().leader` fresh; this plan must not rely on that)
- **Category**: bug
- **Planned at**: commit `a2522b7`, 2026-10-15

## Why this matters

The service (TypeScript, ERC-20/721 discovery on Ethereum, Postgres via Drizzle) answers with `finality: "latest"` by default: finalized aggregates plus a persisted *tail* of every Transfer in the unfinalized blocks `(cursor, tip]`, overlaid at read time. Only the follower's leader process runs the tail loop; every process serves reads from the shared `tail_blocks` / `tail_transfers` tables. Today:

1. **Non-leader processes always report the tail healthy.** `tail.run()` returns immediately when not leader, so its in-memory counters never move: `lagBlocks` is 0, `stale` false, `healthy` true. If the leader's tail stalls, every other process keeps serving the old DB tip as `finality: latest, stale: false` (until the follower prunes it, ~13 min). A non-leader with an empty tail reports `lastProgressAt` 1970-01-01. The same happens on the leader if the RPC keeps returning the same old `latest` (no higher number is ever observed, so lag stays 0).
2. **Unfenced tail mutations.** `rewindTail` and `wipeTail` delete unconditionally. During leader failover two processes can both run the tail for a while; one can delete rows the other just appended.
3. **Unbounded growth when finality stalls.** If Ethereum finality stops (it has happened on mainnet, e.g. May 2023) while `latest` keeps advancing, the tail keeps appending forever: millions of rows, ever more expensive per-request overlays.
4. **Restart re-validation never runs in production**: `tail.load()` only re-validates the stored tip when `follower.status().leader` is true, but at startup no follower tick has acquired the lock yet. (The first tail tick's normal checks do detect a mismatch; this plan just removes the dead gate and makes freshness checks cover the gap.)

## Current state

- `src/tail/tail.ts` (255 lines) — `createTail(o)`. Key parts:
  - `status()` lines 236–253:
```ts
      const lagBlocks = tip && latestNumber && latestNumber > tip.number ? latestNumber - tip.number : 0n;
      const stale = lagBlocks > 0n && lastAdvanceAt > 0 && now() - lastAdvanceAt > staleAfterMs;
      const healthy = tailError === undefined && consecutiveErrors < errorThreshold && !stale;
```
  - `run()` lines 154–194: `if (!follower.leader) return;` then reads cursor and tip from the store, `adapter.latestHeader()`, then append / `handleReorg`.
  - `handleReorg` lines 121–152 calls `store.wipeTail(network)` (no ancestor) or `store.rewindTail(network, ancestor.number)`.
  - `load()` lines 211–224: re-validation gated on `o.follower.status().leader`.
  - `append(anchor, head)` lines 77–119: fetches at most `adapter.followerMaxRange` headers per call, then `store.appendTailBlocks(network, anchor.hash, blocks)` (which is already fenced by an expected parent hash and throws `TailConflictError` from `src/store/store.ts`).
- `src/discovery/service.ts:189-254` — after the backfill, `const view = await deps.store.readAccount(network, account);` returns `{ cursor, holdings, nftItems, tailTransfers, tailTip }` from one repeatable-read snapshot. Then:
```ts
      const tailStatus = deps.tails?.[network]?.status();
      const wantsLatest = requestedFinality !== 'finalized' && tailStatus !== undefined;
      const serveLatest = wantsLatest && tailStatus.healthy && view.tailTip !== undefined;
      ...
        ...(wantsLatest && !serveLatest && { tailError: tailStatus?.tailError ?? 'Tail has no complete snapshot yet' }),
        lastProgressAt: serveLatest ? tailStatus!.lastAdvanceAt : cursor.updatedAt,
        stale: serveLatest ? false : status.stale,
```
  `view.tailTip` is a `BlockHeader { number, hash, parentHash, timestamp }` where `timestamp` is **seconds** (bigint).
- `src/store/store.ts:321-334`:
```ts
    async rewindTail(network, aboveNumber) {
      await db.transaction(async (tx) => {
        await tx.delete(schema.tailTransfers).where(and(eq(...network), gt(schema.tailTransfers.blockNumber, aboveNumber)));
        await tx.delete(schema.tailBlocks).where(and(eq(...network), gt(schema.tailBlocks.number, aboveNumber)));
      });
    },
    async wipeTail(network) {
      await db.transaction((tx) => wipeTail(tx, network));
    },
```
  The private helper `getTailTip(tx, network, 'update')` (line 168) locks the tip row; `appendTailBlocks` (line 271) is the fencing exemplar — copy its pattern (`TailConflictError` on a hash mismatch).
- `Store` interface: `src/core/types.ts:289-290` (`rewindTail`, `wipeTail`). `TailStatus`: `src/core/types.ts:348`.
- `src/config.ts` `tail:` block (around line 90) and `src/server.ts` (around lines 64–82) construct the tail and discovery service. `.env.example` lists `TAIL_*` vars.
- Tests: `test/tail.test.ts` — `stack(chain)` helper (top of file) builds store + tail + discovery over `FakeChain` (`test/fake-chain.ts`). **FakeChain block timestamps are `number * 12` seconds** (e.g. block 100 → 1200 s, i.e. 1970), far from the test clock — so any tip-age check must be *opt-in* (disabled unless a threshold is passed) so existing tests keep their behaviour; new tests pass a `now` and threshold that fit FakeChain timestamps.

Conventions: Node 26 native TS type stripping (`.ts` imports, `import type`, no enums/parameter properties). Compact code, one-line *why* comments. Match `tail.ts` style.

## Commands you will need

| Purpose | Command | Expected |
|---|---|---|
| Install (fresh worktree) | `npm ci` or symlink `node_modules` from the main checkout | exit 0 |
| Typecheck | `npx tsc --noEmit -p .` | exit 0 |
| Targeted tests | `npx vitest run test/tail.test.ts test/store.test.ts test/api.test.ts` | all pass |
| Full tests | `npx vitest run --testTimeout=600000` | all pass (a 5k-accounts test can take minutes on a loaded machine) |

## Scope

**In scope**: `src/tail/tail.ts`, `src/discovery/service.ts` (only the latest/finalized decision and the fields it sets), `src/store/store.ts` (only `rewindTail`, `wipeTail`), `src/core/types.ts` (Store tail methods, `TailStatus`, discovery deps if typed there), `src/config.ts`, `src/server.ts`, `.env.example`, `README.md` (env var table / Limits lines only), `test/tail.test.ts`, `test/store.test.ts`.

**Out of scope**: `src/follower/*` (another plan changes leadership reporting), `applyFollowerRange`, the balance worker, schema/migrations (no schema change needed), the response *shape* beyond what is listed below.

## Git workflow

- Branch `advisor/003-tail-freshness-fencing-and-bounds`; conventional commits (`fix: ...`), no Co-Authored-By/"Generated with" trailers; do not push.

## Steps

### Step 1: Fence `rewindTail` and `wipeTail`

Change signatures to `rewindTail(network, aboveNumber, expectedTipHash: Hex)` and `wipeTail(network, expectedTipHash?: Hex)`. In each transaction first `getTailTip(tx, network, 'update')`; if `expectedTipHash` is given and the locked tip's hash differs (or the tail is empty when a hash was expected), throw `new TailConflictError(...)`. The follower's internal `wipeTail(tx, ...)` helper used inside `applyFollowerRange` stays unfenced (it runs under the cursor lock). Update `tail.ts` callers to pass the tip they observed (`oldTip.hash` in `handleReorg`). A `TailConflictError` in the tail loop is just a failed tick (already counted in `consecutiveErrors`); the next tick re-reads the tip.

**Verify**: `npx tsc --noEmit -p .` → exit 0; `npx vitest run test/tail.test.ts test/store.test.ts` → all pass.

### Step 2: Freshness from the shared tip, not process memory

- Add an optional discovery dependency `maxTailTipAgeMs?: number` (undefined = check disabled). In `discover`, when `wantsLatest`, compute `tipAgeMs = now() - Number(view.tailTip.timestamp) * 1000`; if the check is enabled and `tipAgeMs > maxTailTipAgeMs`, do not serve latest and set `tailError: \`Tail tip is ${Math.round(tipAgeMs / 1000)} s old\``.
- `lastProgressAt` when serving latest: `tailStatus.lastAdvanceAt || Number(view.tailTip.timestamp) * 1000` (never 0).
- Config: `TAIL_MAX_TIP_AGE_MS` default `60000` (5 slots: tolerates up to 4 consecutive missed slots); wire it from `server.ts`; document in `.env.example` and the README env table.
- In `tail.ts` `load()`: drop the `o.follower.status().leader` gate's *effect* problem by simply removing the re-validation block from `load()` (keep loading `tip`/`epoch`) — the first leader tick already compares the stored tip with the canonical chain (append parent check / same-height hash check / lower-head lookup). Keep the existing restart test passing (it calls `tick()` after `load()`).

**Verify**: `npx vitest run test/tail.test.ts` → all pass.

### Step 3: Bound the tail when finality stalls

- Add tail option `maxAheadBlocks?: bigint` (default `1024n`, ~3.4 h; normal span is 64–95 blocks). In `run()`, after reading the cursor and tip: if `(tip?.number ?? cursor.number) - cursor.number >= maxAheadBlocks`, do not append; set `tailError = \`Tail is ${n} blocks above the finalized cursor; finality may be stalled\`` (making status unhealthy, so reads fall back to finalized with that `tailError`), call `o.onEpoch?.()` to wake the follower, and return. Clear it (`tailError = undefined`) on the next successful append (append already does this).
- Also cap each catch-up so the tail never appends past `cursor + maxAheadBlocks` (clip `head` handling in `append`'s `end` computation via an extra bound).
- Config `TAIL_MAX_AHEAD_BLOCKS` default `1024`, wired via `server.ts`, documented in `.env.example` and README.

**Verify**: `npx vitest run test/tail.test.ts` → all pass.

## Test plan

In `test/tail.test.ts`, following the `stack()` helper and existing tests (e.g. `'restart re-validates a stored tip through the normal reorg path'`):
1. "a tail tip older than maxTailTipAgeMs is served as finalized with tailError": build discovery with `maxTailTipAgeMs` and a `now` such that the FakeChain tip timestamp (`number * 12` s) is older than the threshold; assert `finality: 'finalized'` and `tailError` matches `/Tail tip is \d+ s old/`; with a `now` within the threshold, `finality: 'latest'`.
2. "lastProgressAt is the tip time when the serving process never advanced the tail itself": a second tail instance (never ticked, like a non-leader) over the same store; served latest result has `lastProgressAt === Number(tip.timestamp) * 1000`.
3. "rewindTail/wipeTail with a stale expected tip throw TailConflictError and delete nothing" (put in `test/store.test.ts`, modelled after existing tail store tests there; search `appendTailBlocks`).
4. "the tail stops appending maxAheadBlocks above the cursor and degrades to finalized": `createTail({ ..., maxAheadBlocks: 4n })`, chain latest far ahead, tick until it stops; assert `tipNumber - cursor <= 4n`, `status().healthy === false`, `tailError` mentions finality.

## Done criteria

- [ ] `npx tsc --noEmit -p .` exits 0
- [ ] `npx vitest run --testTimeout=600000` passes, with ≥4 new tests
- [ ] `grep -n "TAIL_MAX_TIP_AGE_MS\|TAIL_MAX_AHEAD_BLOCKS" src/config.ts .env.example README.md` → matches in all three files
- [ ] `grep -n "expectedTipHash" src/store/store.ts` → matches in rewindTail and wipeTail
- [ ] Only in-scope files changed (`git diff --stat`)

## STOP conditions

- Fencing requires changing `applyFollowerRange` or the schema.
- An existing tail test's assertions (not setup) must change to pass — report which and why.
- `view.tailTip` is not available in `discover` at the point of the latest/finalized decision.

## Maintenance notes

- Freshness is now derived from shared state (the tip block's own timestamp), so it is identical in every process; the in-process `TailStatus` still carries leader-only diagnostics (`reorgs`, `lastError`).
- `TAIL_MAX_TIP_AGE_MS` trades false degradations on missed slots against staleness; 60 s ≈ 5 slots.
- Deferred: a leadership generation token for full fencing of appends across failover (appends are already parent-hash fenced).
