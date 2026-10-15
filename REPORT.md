# Token discovery PoC: architecture and rationale

Status: describes the system **as built** on branch `poc/token-discovery` (2026-10-14, 93 tests). How we got here, including the dead ends: [PROGRESS.md](PROGRESS.md). Running it, API fields and configuration: [README.md](README.md). Values marked *(estimate)* are derived, not measured.

## 0. Scope decisions

Answers from the scope round with Jakub and Tali, plus Viktor's later decisions.

| Question | Decision |
|---|---|
| What "not a full indexer" means | Clarified by Jakub and Tali: no Blockbook-style discovery over blocks, transactions and native transfers. An index of `Transfer` logs is in scope. They found no existing service that does token discovery over plain RPC, hence the PoC |
| Primary data source | Quicknode JSON-RPC. Envio HyperSync is an experimental, opt-in backfill source (`LOG_SOURCE=auto|hypersync`) with whole-chunk RPC fallback; off by default after a stream hang on the Binance hot wallet. Follower, heads and balances stay on RPC |
| Tokens held before block 25,933,634 and untouched since | Out of scope (limit) |
| Spam tokens | Returned. No filtering |
| What counts as "interacted" | `Transfer` in or out |
| Native ETH | Out of scope |
| ERC-721 (bonus) | Contracts plus the token IDs seen in logs, with optional `ownerOf` verification |
| Freshness | Default responses use a slot-aligned, reorg-aware latest tail, normally a few seconds behind the tip. `finality=finalized` remains available; tail health failure automatically degrades to that roughly 64-block/13-minute base with `tailError` |

## 0.1 Implementation status

The per-account backfill now depends on the chain-agnostic `AccountLogSource` contract, not an RPC method on discovery. `src/sources/` contains the unchanged two-direction RPC implementation, a page-streaming HyperSync implementation, and provider-neutral fallback/breaker composition. `src/networks.ts` holds the EVM network facts used by generic server construction. HyperSync is dynamically imported only when configured; every stream closes on completion, timeout, or shutdown.

**HyperSync is unplugged by default (`LOG_SOURCE=rpc`).** Live comparison against RPC: normal account `0x5a52…` 219 logs / 109 tokens and NFT holder `0x6ff0…` 288 logs / 278 items, both identical, in 3 HyperSync requests (0.35–0.8 s) vs 172 Quicknode calls (160–220 s). The Binance hot wallet `0x28c6…` hung inside a 100k-block stream (30 s and 600 s timeouts both hit), although an earlier standalone benchmark streamed the whole window in 13 s. Root cause not yet found; the source stays opt-in until it is.

HyperSync folds pages directly into the same `ChunkAggregate` using the same strict decoder and self-transfer/ERC-721 rules as RPC. Its query end is always the finalized target plus one (exclusive), and a source height below the requested inclusive end fails to RPC. A failed or partial stream cannot be committed; after two consecutive failures, the primary cools down for 60 s. `/health` exposes redacted per-source chunks, requests, logs, failures and fallbacks. Fake-client tests cover pagination, aggregate equivalence, height guards, 401/429/timeout/malformed fallback, cooldown/retry, redaction, and mid-stream resume.

## 1. Architecture as built

A TypeScript service on Node 26: Hono, viem on our own `RpcClient`, and PostgreSQL 18 via Drizzle. Finalized aggregates exist only for **tracked accounts**. A bounded tail journals every unfinalized `Transfer`, so an account first requested mid-tail is covered automatically.

```
                    Quicknode JSON-RPC (+ fallbacks)
          ┌──────────────────┬────────────────────┬───────────────────┐
          │ finalized epoch  │ latest slot        │ backfill/balances │
          ▼                  ▼                    ▼
   Finalized follower   Reorg-aware tail     Backfill + balance worker
   holdings/nft_items   all Transfers        account history / state
          │                  │                    │
          └──────────────► PostgreSQL 18 ◄────────┘
                              │
              repeatable-read finalized base + tail overlay
                              ▼
             GET /v1/.../erc20|erc721?finality=latest
```

1. **Shared finalized follower.** A single follower per network reads **every** `Transfer` log once, with no address filter. It applies only the logs whose `from`/`to` is a tracked account. At the head this is one `getLogs` per finalized epoch (32 blocks, about 17–22k logs, about 10–14 MB), no matter how many accounts are tracked. With the tail running, the follower polls `finalized` when the tail tip enters a new epoch (finality only moves at epoch boundaries), re-woken every 4 slots of that epoch until finalized actually moves, with a 390 s fallback timer. While the follower is more than an epoch behind finalized (startup, outage) the tail waits for it instead of fetching hundreds of headers its commits would prune.
2. **Backfill with handoff.** A new account is tracked and the follower cursor C is read in one transaction. The account is then backfilled once over [25,933,634, C] through HyperSync with per-chunk RPC fallback (or directly through RPC without a key). The follower owns every block above C. Until the backfill finishes, the API answers `202 syncing`.
3. **Reorg-aware tail.** While the follower process is leader, it polls at slot +2.5/+5/+9 s. Direct successors use EIP-234 block-hash logs. Catch-up uses one range query only after validating a by-number header chain and every returned `log.blockHash`. A fork walks back by number to the common ancestor (at most `TAIL_MAX_DEPTH` = 64 blocks, never below the finalized cursor), rewinds only `tail_blocks`/`tail_transfers` and re-ingests; without an ancestor it wipes the tail and serves finalized (with `tailError`) until the next tick rebuilds it. The finalized tables never roll back; finalization prunes tail rows ≤ the new cursor in the follower's own commit (wiping the tail if its block at the cursor has another hash).
4. **Shared balance worker.** Balances of active accounts are re-read every 60 s and right after a tail reorg, batched across accounts at the tail tip hash (or the finalized cursor while the tail is empty or disabled). Every balance carries its block number (and hash with the tail).
5. **Request path.** Once backfilled, discovery is a repeatable-read database snapshot and normally costs zero RPC calls; only missing balance snapshots or ownership verification read state.

**The key point:** RPC cost depends on the block rate and the number of active (account, token) pairs, not on how many requests users make. 5,000 balance requests a minute become about 50 Multicall calls a minute (§4).

### 1.1 Alternatives considered

| Alternative | Why not (yet) |
|---|---|
| **Global ingestion**: index every `Transfer` for all holders (REPORT rev 2's recommendation) | The follower already sees every log at the head, so the difference is only the backfill and storage. Global backfill is cheap in credits (about 260 calls of 1k blocks, about 5k credits). But it means downloading about 70–115 GB of JSON *(estimate)* with a streaming parser, and storing about 15–40M (holder, token) rows (1.5–6 GB) *(extrapolated)*. Storage then grows with the chain, not with users. That is closer in spirit to a full indexer, and more than a one-day PoC needed. **Still an option:** if the cold-start `202 syncing` is unacceptable, bootstrap a global table once from HyperSync or SQD and hand over to the same follower. Nothing else would change |
| **Per-account on demand** (the first build): rescan new blocks on every request | Each request cost about 2 `getLogs` plus a Multicall, about 15k calls/min at 5k users. Under finalized-only it was still about 1.6k `getLogs`/min plus about 5k Multicalls/min. That is what the follower and balance worker replaced |
| **Realtime tail** with reorg rollback (journal or reversible tail) | **Chosen and implemented.** The irreversible finalized aggregates stay unchanged. Every decoded unfinalized Transfer is journaled, rewound/re-ingested on a fork, and folded at read time. This avoids the invalid earlier idea of deleting aggregate pairs above a fork, which could not restore counts or NFT ownership |
| **Indexer frameworks** (Squid SDK, Envio HyperIndex, Ponder, Goldsky) | They own the process, the schema and the filters. None supports a tracked set that grows at runtime with a handoff. Squid and Ponder fetch blocks one by one in RPC mode, at about 15–30× our credits (§3). Hosted pipelines also contradict "new networks with only JSON-RPC" |

## 2. Measured facts

Mainnet, 2026-10-14. Provider latency varied about 6× during the day, so each figure notes its context. The full tables are in [PROGRESS.md](PROGRESS.md).

**Chain and logs**
- The window started at block 25,933,634 (mined 2026-09-08). It was about 257k blocks at the time of measurement and grows by about 7,200 per day.
- `Transfer` volume is about 300–1,000 logs per block (typically 450–700) and about 200–450 KB of JSON per block.
- ERC-20 and ERC-721 share topic0. They differ in topic count: 3 for ERC-20, 4 for ERC-721 (`tokenId` indexed). The ERC-721 share was 0.3–9%.
- Unfiltered `getLogs` over 1,000 blocks returned 701,764 logs, 446 MB, in 4.0 s, with no result cap (Quicknode demo endpoint). 10k unfiltered blocks would be about 4.5 GB, beyond a JS string. That is why follower ranges are capped at 500 blocks (`ETH_FOLLOWER_MAX_RANGE`, hard limit 1,000).

**Backfill: address-filtered, cold, full window**

| Chunk | Normal account (`0x5a52…`, 109 tokens) | Binance hot wallet (`0x28c6…`, about 2.5M logs) | Calls | Conditions |
|---|---|---|---|---|
| 10k | 2.4–2.6 s (morning), 5.5–11.7 s (afternoon) | 81–88 s, later 64–98 s with failures, 1.9 GB RSS | 52 | 49 MB per received-side chunk |
| 1k | 36.7 s | 305 s, 733 MB | 516 | slow provider |
| **3k, pipelined (default)** | **8.0 s** (floored by `RPC_MAX_RPS=20`) | **71.6 s**, about 1.1 GB, 0 errors | 172 / 320 | about 1.6 GB of JSON for Binance; download-bound |

**Steady state with the follower (live, Quicknode)**
- One epoch advance: **1 `getLogs`**, plus 2 head calls (`finalized`, `latest`) per 30 s poll.
- Balance refresh of 109 pairs: **1 `eth_call`**.
- 10 warm requests: **0 RPC calls**, 7–12 ms each.
- A cold backfill under the shared limiter took 172 calls and about 26 s, with two 202s first.
- Fake-RPC acceptance test: 5k accounts × 10 tokens cost 1 `getLogs` per epoch and 50 `eth_call` for 50k pairs. Serving 5k requests afterwards cost 0 calls.

**RPC limits**
- **Multicall3:** 2,000 `balanceOf` calls in one call are safe (about 6.8k gas each). HTTP 413 above about 1.3–1.7 MB of calldata. `eth_estimateGas` is capped at 16.7M. We use 1,000 per call.
- **Public RPCs** failed in practice: archive refused, "specify an address", "method not available", -32005 rate limits, HTTP 525. Result caps were 10k (mevblocker) and 20k (tenderly), both with a suggested sub-range, which `RpcClient` parses.
- **Quicknode** (from docs; dashboard not checked):
  - 20 credits per standard call at any range or response size.
  - Each call inside a JSON-RPC batch is billed.
  - Build plan: $49 for 80M credits at 50 RPS.
  - `getLogs` range up to 10k blocks on paid plans.

## 3. Third-party data sources and frameworks

No source replaces the service. None answers "tokens this address interacted with, plus balances at block N" for a network that has only JSON-RPC. The useful ones are **data sources behind our adapter**, never frameworks we build on.

| Source | Finding | Role |
|---|---|---|
| Quicknode JSON-RPC | All of §2 | **Primary** for follower, heads and balances; fallback (or explicit source) for account backfills |
| **Envio HyperSync** (live, free token) | One OR query covers sent and received. Normal account: 1 request, 0.6 s, logs identical to Quicknode's 172 calls. Binance: 2,548,175 logs over about 257k blocks in 13.0 s and 580 MB RSS with `@envio-dev/hypersync-client` 1.4.1 `stream()`; `collect()` peaked at 2.6 GB. It serves the chain head, so queries are capped at our finalized target. No rate-limit headers were observed; 429 remains a handled risk | **Implemented, opt-in (off by default)** for per-account backfill, with RPC whole-chunk fallback; hangs on Binance-size chunks |
| Envio HyperRPC | 403: our token is not enabled for it. `getLogs` only, no `eth_call` | Possible `RPC_FALLBACK_URLS` entry |
| Envio HyperIndex | A full framework with a fixed config, its own DB and its own loop | Rejected |
| SQD Portal | 0 head lag. Topics must be lowercase and 32-byte padded, or it **silently returns 0 logs**. The free tier gives frequent 529s at about 0.1 MB/s. Paid tiers are $99 and $499 | Backup `LogSource` on a paid tier |
| Squid SDK (`@subsquid/evm-processor` 1.30.1) | RPC mode makes `eth_getBlockByNumber` per block plus unfiltered `getLogs` in 10-block slices: about 36 calls per epoch vs about 2 for us. Fixed filters, no runtime account set or handoff, a TypeORM store, owns the main loop, GPL-3.0. Pipes SDK 1.0.0-beta.8 is Portal-only | Rejected |
| Ponder (`ponder` 0.18.1, MIT; Postgres 14–17 only per its requirements) | Rejected from docs and source (`sync-historical`), with no spike. The log, row and credit numbers are *(estimates)*:<br>1. **Static addresses.** Filters are fixed in `ponder.config.ts` (only `factory()` is dynamic), so runtime-tracked accounts would force global ingestion: about 690 logs/block × 257k ≈ 177M logs, 15–40M rows, a large RPC cache. No per-account backfill handoff.<br>2. **Block calls.** Historical sync calls `eth_getBlockByNumber(n, true)` for every block with a matching log, and realtime fetches every block. That is ≥ 257k block calls for the backfill, about 5M+ credits (6–10M including `getLogs`), and 32+ calls per epoch vs our 1–2.<br>3. **No finalized-only mode.** Realtime is reorg-aware (`reorgWindow`).<br>4. **Owns the app.** Its own `onchainTable` schema, process (`ponder start`) and codegen. Balances through `publicClients` would cost one call per request.<br>5. **Backfill-only use** is redundant with HyperSync. SQD's fork `@subsquid/ponder` adds a Portal historical source (beta) | Rejected |
| Quicknode Streams | Managed block push with reorg handling, about 4.3M credits/month on mainnet *(docs)* | Production alternative to our follower |
| GoldRush, Ankr | Historical holdings APIs, proprietary, major chains only | Shows the gap for new networks |
| Goldsky | Hosted pipelines into your own database | Not used |

## 4. Cost (Quicknode credits)

Assumptions: 5k active users with about 10 tokens each (50k pairs), 20 credits per call, 7,200 blocks per day.

| Item | Calls | Credits |
|---|---|---|
| Cold backfill per new account (3k chunks over about 257k blocks; measured) | 172 once per account; 5k accounts ≈ 860k | **about 17M once** |
| Cold backfill with HyperSync healthy (measured normal account) | 1 HyperSync query per 100k-block chunk; 0 Quicknode calls | **0 Quicknode credits** |
| Follower, 1 unfiltered `getLogs` per finalized epoch (measured) | about 225/day | about 0.14M/month |
| Tail: 1 `latest` header + 1 `getLogs({ blockHash })` per block (measured live 1.2–1.4 headers/block including the follower's epoch polls: blocks appear 2.0–3.8 s after the slot, so the +2.5 s poll is often early) | about 15.8–17.3k/day | about 9.5–10.4M/month |
| Head polling: `finalized` + `latest` once per epoch (the tail wakes the follower; was every 30 s ≈ 3.5M) | about 450/day | about 0.3M/month |
| Balance worker, change-driven *(estimate)*: each 60 s refresh reads only pairs with a Transfer after their snapshot (assumed 1% of 50k = 500 pairs → 1 Multicall) plus always-refresh pairs held (WETH/stETH; ≤ 1 call/min per 1000 such pairs); hourly sweep of non-zero pairs (50k/1000 = 50 calls/h); all 50 calls once per tail reorg | about 2.6k/day, plus 1.4k/day per 1000 always-refresh pairs held | about 1.6M/month instead of about 43M; independent of zero-balance spam tokens |
| First balance read for a newly active account | 1 per activation | small |
| Requests for backfilled accounts (measured) | 0 | 0 |
| **Total steady state** | | **about 53–54M/month; fits Build (80M)** |

- **The tail is cheap; balances dominate.** Polling `finalized` at epoch boundaries instead of every 30 s pays for most of the tail. A per-block refresh of only the pairs touched by each block's Transfers (not implemented) would add about 4M *(estimate)*.
- **Balance cadence.** The cost scales linearly with the refresh cadence and the activity window (`BALANCE_REFRESH_MS`, `ACTIVE_ACCOUNT_TTL_MS`). Refreshing every block (12 s) would cost about 215M/month and need the Accelerate plan.
- **The naive design** (one Multicall per request) would cost 5k × 60 × 24 × 30 × 20 ≈ **4.3B credits/month**.
- **The backfill grows with the window**, by about 7,200 blocks a day. HyperSync would make it about 0 credits.
- **Bandwidth:** about 10–14 MB per epoch, about 3 GB/day at the head *(estimate)*.
- **Storage:** one row per (tracked account, token) plus one balance snapshot per active pair. That is about 100k rows for 5k × 10 *(estimate)*.

## 5. Design

### Follower (`src/follower/follower.ts`)
- **Polling:** it polls the finalized head every `FOLLOWER_POLL_MS` (30 s without the tail; with the tail it is woken at each epoch boundary and 390 s is only a fallback, via `follower.kick()`). When finalized is above the global `follower_cursor`, it fetches (cursor, head] unfiltered in ≤ 500-block ranges. Logs whose from/to isn't tracked are dropped before decoding. Each range commits in one transaction with the cursor advance (number, hash, timestamp).
- **Exactly one per network:** it holds a session `pg_try_advisory_lock` on a dedicated connection, re-checked every tick. Commits also lock the cursor row and refuse a range that doesn't continue it.
- **Race with new accounts:** if an account was tracked while a range was being decoded, the commit applies nothing and the range is refetched with that account included.
- **Startup:** `follower.load()` reads the saved cursor before the server listens, so a restart can serve immediately.
- **Health:** responses are `stale` after a failed poll, a lag above one epoch, or 20 min without progress. Discovery returns 503 above 96 blocks of lag or after 30 min without progress.

### Backfill and handoff (`src/discovery/service.ts`)
- **`trackAccount`:** in one transaction, it takes a share lock on the cursor row, inserts `tracked_accounts`, and stores C as `backfill_target`. The store rejects backfill chunks above C. Counts add, first/last block widen, and the newest ERC-721 transfer wins by (block, logIndex), so the two writers commute.
- **Scan:** `AccountLogSource` supplies an aggregate for one inclusive range. HyperSync ORs topic1/topic2 in one streaming query; RPC runs the two directions in parallel. Up to 8 chunks are in flight. Completed chunks commit strictly in order, each with `progress_to`, so a failure resumes at the first incomplete chunk.
- **Aggregation while decoding:** logs go straight into per-token aggregates. Only self-transfers are deduplicated.
- **Range splitting:** split on "too large" or on a timeout of a multi-block range, using the provider's suggested range when given.
- **Source-specific range:** direct RPC halves after a split/heavy response and grows after three light chunks; that range is persisted. HyperSync uses fixed 100k-block chunks. If it falls back, RPC may split internally, while the full composite chunk stays atomic.
- **Requests during backfill:** a request waits up to 10 s, then gets `202 syncing` with progress. Backfills are bounded (4 concurrent, 64 queued); beyond that the API returns 503 with `Retry-After`.

### Balances (`src/balances/worker.ts`)
- **Which accounts:** those that requested balances within 5 min are active.
- **When:** every 60 s and on every follower advance, the worker reads all their pairs at **the follower cursor, pinned by hash** (EIP-1898). Reads are packed into Multicall3 `aggregate3` calls (`allowFailure`, 1,000 per call) and stored in `balance_snapshots` with block number and hash.
- **No inferred freshness:** a snapshot is never relabelled as valid at a newer block. Each token reports `balanceBlock`, and the response reports `balancesAtBlockHeight`. "No Transfer, so unchanged" is never assumed, because of WETH `deposit` and rebasing tokens.
- **Failure handling:** batches split only on size, gas or revert errors. Authentication errors, rate limits and outages propagate as 503.
- **Request path:** only a token with no snapshot yet is read on the request path, single-flight per account, through a bounded limiter (16 concurrent, 256 queued).

### RPC resilience (`src/rpc/`)
`RpcClient` is the only component that performs JSON-RPC HTTP; HyperSync's optional streaming client is confined to the backfill source. Details are in README "Unreliable RPC".
- **Error classes:** `rate_limited`, `transient`, `too_large` (carrying the suggested range), `unsupported`, `fatal`.
- **Retries:** full-jitter backoff. `Retry-After` is honoured but capped at 10 s.
- **Breakers and failover:** a breaker per endpoint (5 failures, 15 s open). "Method not available" is remembered per endpoint and method for 10 min. Requests fail over across `RPC_FALLBACK_URLS`.
- **Validation:** a `validate` option makes `null` or invalid blocks and logs fail over.
- **Accounting:** a token-bucket limiter (`RPC_MAX_RPS`, shared by follower, backfill and balances), per-attempt stats in `/health`, and API-key redaction.

### Tail (`src/tail/tail.ts`)
- **Leader only:** runs while `follower.status().leader`, on timers at slot + `TAIL_POLL_OFFSETS_MS` (2.5/5/9 s; Ethereum slots are 12 s from genesis 1606824023). Once the tip is the current slot's block the rest of the slot is skipped.
- **Per tick,** H = `latest`: H ≤ cursor, or lower than the tip with a hash we store → lagging node, ignored. H = tip + 1 with `parentHash` = tip → one `getLogs({ blockHash })`. H further ahead → headers tip+1..H by number (≤ `ETH_FOLLOWER_MAX_RANGE`), parent chain verified, one range `getLogs` whose every `log.blockHash` must match. Parent mismatch or same height/other hash → reorg walk (above). Empty tail → anchor at the follower cursor hash.
- **Store contract:** `appendTailBlocks(network, expectedParentHash, blocks)` is compare-and-append: in one transaction (share lock on the cursor row, `FOR UPDATE` on the tip) it rejects unless the tip hash, or the cursor hash for an empty tail, equals `expectedParentHash`, numbers are contiguous and every transfer carries its block's hash. `rewindTail`/`wipeTail` only delete tail rows.
- **Restart:** tail rows persist; the stored tip is re-validated against the canonical header (reorg path on mismatch). Wiping is always safe: a rebuild is about 90 headers plus one range `getLogs`.
- **Health:** `tip`, `latestNumber`, `lagBlocks`, `reorgs`, `lastReorgDepth`, `lastError`, `healthy` in `/health`. Unhealthy after a wipe until rebuilt, after 3 consecutive failed ticks, or with lag and no advance for 36 s; discovery then serves finalized with `tailError`.

### Interfaces (`src/core/types.ts`)
- **`AccountLogSource`:** chain-agnostic per-account aggregate source with a name, preferred committed range, request/log metadata, stats and optional shutdown. `withFallback` composes providers outside discovery.
- **`ChainAdapter`** (EVM: `createEvmAdapter`):
  - head and blocks: `finalizedHead`, `blockByNumber`, and for the tail `latestHeader`/`headerByNumber` (with `parentHash`)
  - tail logs: `transfersInBlock(blockHash)` (EIP-234) and `transfersInRange(range, headers)` (hash-verified)
  - logs: an `accountLogs` source for backfill; `scanAllTransfers(range, onTransfer, accept)` remains RPC-only for the follower
  - state reads: `erc20BalancePairs(pairs, blockHash)` and `erc721Owners(items, blockHash)`
  - addresses: `normalizeAddress`

  A new EVM chain is a `src/networks.ts` registry entry. A non-EVM chain implements the interface.
- **`Store`:** async. One implementation serves `pg` and PGlite.
- **`RpcClient`:** transport and policy.

### Tables (`src/store/schema.ts`, migrations in `drizzle/`)
- `follower_cursor(network, block_number, block_hash, block_timestamp, updated_at)`
- `tracked_accounts(network, account, backfill_target, tracked_at)`
- `account_scan_progress(network, account, progress_to)`
- `account_scan_range(network, account, logs_range)`
- `holdings(network, holder, token, standard, first_block, last_block, sent_count, received_count)`, primary key (network, holder, token)
- `nft_items(network, holder, token, token_id, held, last_block, last_log_index)`
- `balance_snapshots(network, account, token, balance, error, block_number, block_hash)`

- `tail_blocks(network, number, hash, parent_hash, timestamp)` and `tail_transfers(network, block_number, log_index, block_hash, token, standard, from, to, value, token_id, tx_hash)`, indexed by (network, from) and (network, to)

Individual transfers are stored only for the unfinalized tail (about 520 per block, measured 37.8k rows for 73 blocks), and pruned as the follower finalizes them.

### API (Hono)
- `GET /v1/ethereum/accounts/{addr}/erc20?balances=true` and `…/erc721?verifyOwnership=true` return:
  - `finality`: `latest` (default with a healthy tail) or `finalized` (`?finality=finalized`, or the fallback with `tailError`)
  - `blockHeight`/`blockHash`: the tail tip (latest) or the follower cursor, plus `finalizedBlockHeight`/`finalizedBlockHash` for latest. Cursor, holdings, the account's tail transfers and the tail tip come from one repeatable-read transaction, so a concurrent prune cannot double count or drop a transfer. The tail is folded with the same `addTransfer` and merged: counts add, bounds widen, the tail's NFT transfer wins, tail-only tokens are included
  - freshness: `stale`, `lagBlocks`, `followerLagBlocks`, `dataAgeS`
  - per token: first and last seen block, sent and received counts (`unfinalizedSentCount`/`unfinalizedReceivedCount` when the tail contributes), and optionally `balance`, `balanceBlock` and (with the tail) `balanceBlockHash`; NFT items carry `unfinalized: true` when their latest transfer is in the tail
- During a backfill: `202 syncing` with `Retry-After: 2`.
- `/health` reports the follower, the tail, the balance worker, and RPC stats per method.

## 6. Why this stack

| Choice | Reason |
|---|---|
| **TypeScript on Node 26, native type stripping** | `node src/server.ts` with no build step. The constraints are `.ts` import extensions and no enums or parameter properties. TypeScript 7 is used only for `tsc --noEmit`. It matches the Trezor Suite ecosystem and is fast to write for a PoC. Go would be the natural production choice, since Blockbook is Go |
| **Hono** (over Fastify) | Viktor's preference. It is small and built on Web-standard Request/Response, and `app.request()` tests routes without a port. Only the HTTP layer depended on the framework, so the swap was cheap |
| **viem**, on top of our own `RpcClient` | viem covers everything Ethereum-specific: types, `erc20Abi`/`erc721Abi`/`multicall3Abi`, topic encoding, strict `decodeEventLog`, and `getBlock`/`call` actions. Inside `RpcClient`, viem's `http()` transport (one per endpoint, `retryCount: 0`, no batching, `maxResponseBodySize: false`) only builds requests, applies timeouts and parses responses |
| **Our own retry policy** instead of viem's | viem's retry and `fallback` would resend `-32005` "more than 10000 results" (each attempt billed) instead of splitting. It doesn't retry Quicknode `-32010`, treats a `null` block as success, has no circuit breaker, doesn't cap `Retry-After`, and hides retried attempts from billing stats. "RPC is unreliable and billed per call" is the core of the brief, so this layer is ours |
| **PostgreSQL 18** (docker-compose `postgres:18-alpine`) | Transactional cursor plus upserts (`LEAST`/`GREATEST`), session advisory locks for the single follower, `FOR SHARE`/`FOR UPDATE` for the handoff, and repeatable-read responses. The first build used `node:sqlite` (zero dependencies). We moved before the follower because it needs the lock and more than one process |
| **Drizzle ORM** | A typed schema with generated SQL migrations committed in `drizzle/`, applied at startup. The same code runs on `pg` and PGlite, and it doesn't take over the process |
| **PGlite** for tests | Real Postgres compiled to WASM, in-process. `npm test` runs the real migrations with no Docker or network |
| **vitest** (over `node:test`) | Watch mode, `vi.fn`/fake timers/`stubGlobal` instead of hand-rolled stubs, and readable diffs |
| **`process.loadEnvFile()`** (over dotenv) | Built into Node, so we need neither a dependency nor our own parser. Real environment variables take precedence |
| **Multicall3 `aggregate3`** | Thousands of `balanceOf`/`ownerOf` reads per call, with per-call failure. Batching across accounts is what makes the balance cost manageable |

At 5k users polling once a minute, the API sees about 83 requests/s. That is trivial for one Node process doing indexed Postgres reads.

## 7. Limits and risks

- **Freshness and reorgs:** default responses trail `latest` by 0–1 blocks (measured live). Mainnet sees roughly 4–8 reorgs a day, about 99.7% of them one block deep ([ethresear.ch: reorg statistics](https://ethresear.ch/t/the-second-slot-itch-statistical-analysis-of-reorgs/16333), [post-Pectra reorg rate](https://ethresear.ch/t/post-pectra-effects-on-ethereum-reorg-rate-propagation-and-block-size/23018)); those only rewind tail rows. Anything deeper than 64 blocks or reaching the finalized cursor degrades to the ~13-minute finalized view until the next tick rebuilds. Unfinalized data can still disappear; clients that need certainty use `finality=finalized`.
- **Load-balanced RPC:** Quicknode answers from several nodes, so a header or by-number log can come from another fork. Logs are fetched by hash or hash-verified, and a header that changes mid catch-up is retried; a node on a losing fork can still cause a rewind that is undone on the next tick (counted as a reorg).
- **Tail is leader-only.** Another replica serves the leader's tail from Postgres, but its own tail health is not live. Balances refresh every 60 s, not per block, so they usually trail `blockHeight` by a few blocks (labelled).
- **Discovery is `Transfer`-only.** Tokens that change balances without `Transfer` (WETH `deposit`, rebasing tokens) and non-standard legacy NFTs (3-topic CryptoKitties-style) are missed or misclassified. ERC-1155 is not supported.
- **Holdings from before block 25,933,634 are invisible**, by definition of the source of truth.
- **Spam and address-poisoning tokens are returned.** Many show `balanceStatus: "error"`.
- **Cold start:** about 8–26 s for a normal account, depending on the shared rate limit, and about 75 s and about 1 GB RSS for an exchange wallet. The first requests get 202. The backfill window grows by about 7,200 blocks a day.
- **Single replica:** the active-account set, single-flight maps and limiters live in memory, and every process runs a balance worker. Only the follower is safe across processes.
- **Tracked accounts are never untracked.** The per-epoch cost doesn't grow with them, but the set and the commit-time check do.
- **5k users is not 5k addresses.** Suite users have several accounts, and discovery probes empty addresses up to a gap limit. The real tracked set could be several times larger, which scales the backfill and balance cost.
- **Single provider:** with no fallback endpoint configured, a Quicknode outage stops the follower. Responses go `stale` and then 503.
- **Not verified on the Quicknode dashboard:** credits per call regardless of range, and per-inner-call batch billing. Both come from docs.

## 8. Next experiments

1. **Deploy check:** verify the Linux N-API build loads and run `scripts/compare-sources.ts` against a normal and heavy account. Network access was intentionally unavailable during implementation.
2. **Raise `RPC_MAX_RPS` toward the plan's 50.** This should bring a fallback normal cold scan from about 8.6 s to about 3.5 s, while keeping headroom for the follower and balances.
3. **Check credits on the Quicknode dashboard:** one 3k `getLogs`, a 3-call batch, and a 1,000-call Multicall.
4. **A live HTTP load test:** 5k accounts polling once a minute, measuring RPC calls/min and p99 latency.
5. **Decide the open limits:** shared state for multiple replicas (active set and balance-worker leader in Postgres), untracking idle accounts, spam flags, native ETH, a per-block refresh of the balance pairs touched by each tail block, and tail health shared across replicas.

## Sources

- [Quicknode API credits](https://www.quicknode.com/api-credits), [pricing](https://www.quicknode.com/pricing), [eth_getLogs](https://www.quicknode.com/docs/ethereum/eth_getLogs), [10k block range limit](https://support.quicknode.com/hc/en-us/articles/10258449939473-Understanding-the-10-000-Block-Range-Limit-for-querying-Logs-and-Events)
- [SQD Portal EVM API](https://docs.sqd.dev/en/portal/evm/api), [SQD SDK overview](https://docs.sqd.dev/en/sdk/overview)
- [Envio HyperSync](https://docs.envio.dev/docs/HyperSync/overview), [HyperSync query](https://docs.envio.dev/docs/HyperSync/hypersync-query), [HyperRPC](https://docs.envio.dev/docs/HyperRPC/overview-hyperrpc), [pricing](https://envio.dev/pricing/hypersync)
- [Ponder requirements](https://ponder.sh/docs/requirements)
- [Goldsky Turbo](https://docs.goldsky.com/turbo-pipelines/quickstart)
