# Plan 002: The follower never refetches a range because of new sign-ups, rejects out-of-range logs, and reports leadership truthfully

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat a2522b7..HEAD -- src/follower/follower.ts src/chains/evm/logs.ts src/chains/evm/adapter.ts src/core/types.ts test/follower.test.ts test/logs.test.ts test/fake-chain.ts`
> On any change to these files since the planned commit, compare the excerpts below with the live code; a mismatch is a STOP condition.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: MED
- **Depends on**: none
- **Category**: bug / perf
- **Planned at**: commit `a2522b7`, 2026-10-15

## Why this matters

This is a TypeScript PoC (ERC-20/721 token discovery on Ethereum mainnet over JSON-RPC billed **per call**; Postgres via Drizzle). A single *finalized follower* per network (leader elected by a Postgres advisory lock) fetches every `Transfer` log of each new finalized range with one unfiltered `eth_getLogs` (up to `followerMaxRange` = 500 blocks, ~450 KB of JSON per block) and folds the logs of *tracked* accounts into per-account aggregates.

Three problems:

1. **Refetch livelock.** The follower snapshots the tracked set before fetching. If any account is tracked (by any process) while the range is being fetched/decoded, the store refuses the commit (`applied: false`) and the follower **refetches the whole range** from the RPC. Logs of non-tracked addresses were skipped before decoding, so they cannot be re-folded. With a steady stream of new users, a 500-block catch-up can be discarded and rebilled again and again; the cursor stalls, lag exceeds 96 blocks, and every request gets 503.
2. **No range validation.** Logs returned by `eth_getLogs` are aggregated without checking that their `blockNumber` lies inside the requested range. A misbehaving endpoint returning an out-of-range or duplicate block would double count permanently (the finalized base is never revisited).
3. **Stale leadership flag.** `leader` is refreshed only at the start of a tick (every ~6.5 minutes with the tail enabled). When the lock's DB session dies, `LeaderLock.held()` turns false immediately but `status().leader` keeps saying `true`; and if `tryAcquire()` throws, `leader` keeps its previous value. The realtime tail (another module) trusts `follower.status().leader` to decide whether it may write, so two processes can believe they lead.

## Current state

- `src/follower/follower.ts` — the follower. `applyRange` (lines 59–82):

```ts
  const applyRange = async (range: BlockRange, head: BlockRef): Promise<boolean> => {
    const known = new Set(tracked);
    const aggregates = new Map<Address, ChunkAggregate>();
    const add = (account: Address, event: TransferEvent): void => { ... addTransfer(aggregate, account, event); };
    await adapter.scanAllTransfers(range, (event) => {
      if (known.has(event.from)) add(event.from, event);
      if (event.to !== event.from && known.has(event.to)) add(event.to, event);
    }, (address) => known.has(address));
    const to = range.toBlock === head.number ? head : await adapter.blockByNumber(range.toBlock);
    const at = now();
    const result = await store.applyFollowerRange(network, range, to, at, aggregates, known);
    if (!result.applied) {
      // Tracked (by another request or process) while this range was decoded: refetch it with them included.
      for (const account of result.missing) tracked!.add(account);
      return false;
    }
    cursor = { ... };
    return true;
  };
```

  `run()` (lines 84–109) sets `leader = o.lock ? await o.lock.tryAcquire() : true;` inside a `try`; the `catch` only records `lastError`. The `while` loop calls `applyRange` repeatedly; a `false` return just loops and refetches. `status()` (lines 154–174) returns `leader` as-is.

- `src/store/store.ts:228-261` `applyFollowerRange(network, range, to, at, aggregates, known)` — in one transaction: locks the cursor `FOR UPDATE`, checks continuity, selects all tracked accounts, returns `{ applied: false, missing }` if any is not in `known`, else upserts aggregates, prunes the tail, and advances the cursor. **Do not change this function**; its contract stays.

- `src/chains/evm/logs.ts`:
  - `decodeTransferLog(log)` (line ~36) — strict decode; returns `undefined` for non-standard logs (deliberately skipped: e.g. ERC-20s with non-32-byte data).
  - `scanLogs(rpc, topics, range, onLog, shouldStop)` (~line 135) — fetches `[fromBlock,toBlock]`, splits on `too_large` / timeout, calls `onLog(raw)` per log of each successful part `part`.
  - `scanAllTransfers(rpc, range, onTransfer, accept?)` (~line 209):

```ts
  const { calls } = await scanLogs(rpc, [TRANSFER_TOPIC], range, (log) => {
    if (accept && Array.isArray(log.topics)) {
      const from = topicAddress(log.topics[1]);
      const to = topicAddress(log.topics[2]);
      if (from && to && !accept(from) && !accept(to)) return;
    }
    const event = decodeTransferLog(log);
    if (event) onTransfer(event);
  });
```

  - `transfersInRange` (~line 95) already throws `new RpcError('transient', ...)` for logs that don't match the expected block — follow that pattern (`RpcError` from `src/rpc/errors.ts`; `transient` errors are retried/failed over by the RPC client and make the follower tick fail without committing).
- `src/chains/evm/adapter.ts:44` wires `scanAllTransfers: (range, onTransfer, accept) => scanAllTransfers(rpc, range, onTransfer, accept)`; the `ChainAdapter` interface is in `src/core/types.ts` (~line 135).
- `src/store/advisory-lock.ts:30-34` `LeaderLock { tryAcquire(): Promise<boolean>; held(): boolean; release(): Promise<void> }`.
- Tests: `test/follower.test.ts` builds a full stack over `FakeChain` (`test/fake-chain.ts`); the existing test `'handoff: an account tracked while the follower is decoding a range is refetched, not missed'` (line ~183) covers today's refetch behaviour and uses `gate` from `fake-chain.ts` to pause a fetch. `FakeChain` counts RPC calls via `chain.rpc.stats()`.

Conventions: Node 26 native TS type stripping (`.ts` imports, `import type`, no enums/parameter properties/namespaces). Compact style, one-line *why* comments. Match `follower.ts`.

## Commands you will need

| Purpose | Command | Expected |
|---|---|---|
| Install (fresh worktree) | `npm ci` or symlink `node_modules` from the main checkout | exit 0 |
| Typecheck | `npx tsc --noEmit -p .` | exit 0 |
| Targeted tests | `npx vitest run test/follower.test.ts test/logs.test.ts test/tail.test.ts --testTimeout=600000` | all pass (the 5k-accounts test may take minutes on a loaded machine) |
| Full tests | `npx vitest run --testTimeout=600000` | all pass |

## Scope

**In scope**: `src/follower/follower.ts`, `src/chains/evm/logs.ts`, `src/chains/evm/adapter.ts`, `src/core/types.ts` (only the `scanAllTransfers` signature), `test/follower.test.ts`, `test/logs.test.ts`, `test/fake-chain.ts` (only if the fake adapter must forward the new parameter).

**Out of scope**: `src/store/*` (the commit contract stays), `src/tail/*`, `src/discovery/*`, `src/balances/*`, HyperSync sources. Don't change `followerMaxRange` defaults.

## Git workflow

- Branch `advisor/002-follower-refold-and-leader-freshness`; conventional commits (`fix: ...`), no Co-Authored-By/"Generated with" trailers; do not push.

## Steps

### Step 1: Expose skipped logs from `scanAllTransfers`

Add an optional 5th parameter `onSkipped?: (from: Address, to: Address, decode: () => TransferEvent | undefined) => void`. When a log is rejected by `accept`, call `onSkipped(from, to, () => decodeTransferLog(log))` instead of dropping it. Forward it through `adapter.ts` and the `ChainAdapter.scanAllTransfers` type (document it in the existing doc comment: "logs skipped by accept, decodable later"). Update `test/fake-chain.ts` only if its adapter implements `scanAllTransfers` itself.

**Verify**: `npx tsc --noEmit -p .` → exit 0.

### Step 2: Reject out-of-range logs

In `scanLogs`, for each successful `part`, before calling `onLog`, check `log.blockNumber` parses (use the local `quantity` helper) and `part.fromBlock <= n <= part.toBlock`; otherwise throw `new RpcError('transient', \`Log block ${n} outside requested range ${part.fromBlock}-${part.toBlock}\`)`. Don't check `removed` logs (they're filtered by decode). Do this for the whole `raw` array *before* the first `onLog` call so a bad response folds nothing.

**Verify**: `npx vitest run test/logs.test.ts` → all pass.

### Step 3: Re-fold instead of refetch in `applyRange`

- During the scan, retain skipped logs in an array `skipped: { from, to, decode }[]` (via `onSkipped`).
- Loop up to `MAX_REFOLDS = 8` times: call `store.applyFollowerRange(...)`. On `applied: false`, add `result.missing` to `tracked` and `known`; then for each retained skipped entry where `from` or `to` is newly known, `decode()` it once and fold it with the same `add` rules as the scan callback (from → add(from); to !== from → add(to)), and remove that entry from `skipped`. Then retry the commit (no RPC).
- Only after `MAX_REFOLDS` failed commits fall back to `return false` (today's refetch).
- Keep `to` (the block header) computed once before the loop.

The fold rules must be identical to the scan callback so self-transfers (from === to) are counted once.

**Verify**: `npx tsc --noEmit -p .` → exit 0; `npx vitest run test/follower.test.ts --testTimeout=600000` → all pass (the existing handoff test may now see *fewer* getLogs calls; if it asserts an exact call count that assumed a refetch, update only that number and say so in NOTES).

### Step 4: Truthful leadership

- In `run()`, set `leader = false` in the `catch` block when the failure happened in `tryAcquire` (wrap the acquire in its own try or set `leader = false` before awaiting it).
- In `status()`, report `leader: leader && (o.lock?.held() ?? true)`.
- Optionally (keep it if trivial): also export nothing new — the tail reads `status().leader`.

**Verify**: `npx vitest run test/follower.test.ts --testTimeout=600000` → all pass.

## Test plan

In `test/follower.test.ts`, modelled on the existing handoff test at line ~183 (same `stack()` helper and `gate`):
1. "an account tracked while a range is decoded is folded from the retained logs without refetching": pause the follower's fetch, track a new account with transfers inside the range, release; assert `expectTruth(...)` holds for the new account over the range and that the number of `eth_getLogs` calls equals the no-race baseline (no refetch).
2. "a self-transfer of a newly tracked account is counted once after a refold".
3. "status().leader turns false as soon as the lock is lost": use a fake `LeaderLock` object (`{ tryAcquire: async () => true, held: () => heldFlag, release: async () => {} }`), tick once, flip `heldFlag = false`, assert `follower.status().leader === false` without another tick; and a lock whose `tryAcquire` throws → after the tick, `leader === false`.

In `test/logs.test.ts`, modelled on `'a non-array eth_getLogs result fails over through RpcClient'` (line ~171): a getLogs response containing a log with `blockNumber` outside the requested range rejects with a transient RpcError (or, through the client, fails over) and calls `onLog` zero times.

## Done criteria

- [ ] `npx tsc --noEmit -p .` exits 0
- [ ] `npx vitest run --testTimeout=600000` passes, including ≥4 new tests
- [ ] `grep -n "MAX_REFOLDS" src/follower/follower.ts` matches
- [ ] `git diff --stat` touches only in-scope files

## STOP conditions

- `applyFollowerRange`'s contract would have to change.
- The retained-skipped approach needs more than the parsed response already in memory (e.g. you find `scanLogs` streams and frees raw logs) — report instead.
- Any existing test assertion other than an RPC call count must change.

## Maintenance notes

- Correctness invariant: every block's logs are applied for an account by exactly one of backfill (≤ its `backfill_target`) or follower (> target). Refolding preserves this because the commit still re-checks the tracked set under the cursor lock.
- Memory: retained skipped entries are closures over raw logs already parsed; they are released when `applyRange` returns. A future streaming parser must keep this property.
- Deferred: proving that a lagging endpoint actually had the whole finalized range (a lagging node 64+ blocks behind is unlikely on the finalized path).
