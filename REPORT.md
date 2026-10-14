# Token discovery PoC: architecture and rationale

Status: describes the system **as built** on branch `poc/token-discovery` (2026-10-14, 66 tests). How we got here, including the dead ends: [PROGRESS.md](PROGRESS.md). Running it, API fields and configuration: [README.md](README.md). Values marked *(estimate)* are derived, not measured.

## 0. Scope decisions

Answers from the scope round with Jakub and Tali, plus Viktor's later decisions.

| Question | Decision |
|---|---|
| What "not a full indexer" means | Clarified by Jakub and Tali: no Blockbook-style discovery over blocks, transactions and native transfers. An index of `Transfer` logs is in scope. They found no existing service that does token discovery over plain RPC, hence the PoC |
| Primary data source | Quicknode `eth_getLogs` (premium: 10k-block ranges). SQD and Envio are backfill or backup sources |
| Tokens held before block 25,933,634 and untouched since | Out of scope (limit) |
| Spam tokens | Returned. No filtering |
| What counts as "interacted" | `Transfer` in or out |
| Native ETH | Out of scope |
| ERC-721 (bonus) | Contracts plus the token IDs seen in logs, with optional `ownerOf` verification |
| Freshness | The scope answer was "a few seconds of lag is fine". **We then chose finalized only, with no rollback.** That puts responses about 64 blocks (about 13 min) behind the tip. It is the main trade-off to defend (§7) |

## 1. Architecture as built

A TypeScript service on Node 26: Hono, viem on our own `RpcClient`, and PostgreSQL 18 via Drizzle. Only finalized data is stored, and only for **tracked accounts**: accounts someone has requested.

```
                         ┌────────────── Quicknode JSON-RPC (+ fallbacks) ──────────────┐
                         │ 1 unfiltered getLogs per finalized epoch (≤500-block ranges) │
   finalized head poll ──┤ address-filtered getLogs for one account's backfill          │
   (every 30 s)          │ Multicall3 aggregate3 balanceOf at the follower block (hash) │
                         └──────────────▲────────────────▲───────────────▲──────────────┘
                                        │                │               │
                            ┌───────────┴───┐  ┌─────────┴──────┐  ┌─────┴───────────┐
                            │ Follower      │  │ Backfill       │  │ Balance worker  │
                            │ (1 per net,   │  │ [start, C] per │  │ active accounts,│
                            │ advisory lock)│  │ new account    │  │ 60 s + advance  │
                            └───────┬───────┘  └───────┬────────┘  └────────┬────────┘
                                    └──────────► PostgreSQL 18 ◄────────────┘
                                                      ▲
                       GET /v1/ethereum/accounts/{addr}/erc20|erc721  (DB reads only once backfilled)
```

1. **Shared finalized follower.** A single follower per network reads **every** `Transfer` log once, with no address filter. It applies only the logs whose `from`/`to` is a tracked account. At the head this is one `getLogs` per finalized epoch (32 blocks, about 17–22k logs, about 10–14 MB), no matter how many accounts are tracked.
2. **Backfill with handoff.** A new account is tracked and the follower cursor C is read in one transaction. The account is then backfilled once over [25,933,634, C] with address-filtered `getLogs`. The follower owns every block above C. Until the backfill finishes, the API answers `202 syncing`.
3. **Shared balance worker.** Balances of active accounts are re-read on a cadence, batched across all accounts into Multicall3 calls at one block. Every balance carries the block it was read at.
4. **Request path.** Once an account is backfilled, a request is pure database reads, with **zero RPC calls**.

**The key point:** RPC cost depends on the block rate and the number of active (account, token) pairs, not on how many requests users make. 5,000 balance requests a minute become about 50 Multicall calls a minute (§4).

### 1.1 Alternatives considered

| Alternative | Why not (yet) |
|---|---|
| **Global ingestion**: index every `Transfer` for all holders (REPORT rev 2's recommendation) | The follower already sees every log at the head, so the difference is only the backfill and storage. Global backfill is cheap in credits (about 260 calls of 1k blocks, about 5k credits). But it means downloading about 70–115 GB of JSON *(estimate)* with a streaming parser, and storing about 15–40M (holder, token) rows (1.5–6 GB) *(extrapolated)*. Storage then grows with the chain, not with users. That is closer in spirit to a full indexer, and more than a one-day PoC needed. **Still an option:** if the cold-start `202 syncing` is unacceptable, bootstrap a global table once from HyperSync or SQD and hand over to the same follower. Nothing else would change |
| **Per-account on demand** (the first build): rescan new blocks on every request | Each request cost about 2 `getLogs` plus a Multicall, about 15k calls/min at 5k users. Under finalized-only it was still about 1.6k `getLogs`/min plus about 5k Multicalls/min. That is what the follower and balance worker replaced |
| **Realtime tail** with reorg rollback (journal or reversible tail) | Viktor's decision was "only finalized blocks". Finalized blocks cannot reorg, so there is no rollback code, and the cursor hash makes `blockHeight`/`blockHash` consistent by construction. The earlier "delete pairs above the fork" rollback was invalid anyway, because it cannot restore counts or NFT ownership |
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
| Quicknode JSON-RPC | All of §2 | **Primary** for logs, heads and balances |
| **Envio HyperSync** (live, free token) | One OR query covers sent and received. Normal account: 1 request, 0.6 s, logs identical to Quicknode. Binance: 13.0 s and 580 MB with `@envio-dev/hypersync-client` 1.4.1 `stream()` (napi, loads on Node 26 darwin-arm64); `collect()` peaked at 2.6 GB and sequential JSON took 77 s. Follower epoch: 1 request, 150 ms vs 112 ms on Quicknode, 16,777 logs equal. It **serves the chain head (0 lag vs `latest`)**, so `to_block` must be capped at our finalized block. No rate limit seen at 8 parallel requests | **Next: backfill `LogSource`**, about 5× faster on heavy accounts and about 0 Quicknode credits |
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
| Follower, 1 unfiltered `getLogs` per finalized epoch (measured) | about 225/day | about 0.14M/month |
| Head polling, `finalized` + `latest` every 30 s | about 5.8k/day | about 3.5M/month |
| Balance worker: 50 Multicalls per refresh, every 60 s plus once per follower advance (about +10%) | about 79k/day | about 47M/month |
| First balance read for a newly active account | 1 per activation | small |
| Requests for backfilled accounts (measured) | 0 | 0 |
| **Total steady state** | | **about 51M/month; fits Build (80M)** |

- **Balances dominate.** The cost scales linearly with the refresh cadence and the activity window (`BALANCE_REFRESH_MS`, `ACTIVE_ACCOUNT_TTL_MS`). Refreshing every block (12 s) would cost about 215M/month and need the Accelerate plan.
- **The naive design** (one Multicall per request) would cost 5k × 60 × 24 × 30 × 20 ≈ **4.3B credits/month**.
- **The backfill grows with the window**, by about 7,200 blocks a day. HyperSync would make it about 0 credits.
- **Bandwidth:** about 10–14 MB per epoch, about 3 GB/day at the head *(estimate)*.
- **Storage:** one row per (tracked account, token) plus one balance snapshot per active pair. That is about 100k rows for 5k × 10 *(estimate)*.

## 5. Design

### Follower (`src/follower/follower.ts`)
- **Polling:** it polls the finalized head every 30 s. When finalized is above the global `follower_cursor`, it fetches (cursor, head] unfiltered in ≤ 500-block ranges. Logs whose from/to isn't tracked are dropped before decoding. Each range commits in one transaction with the cursor advance (number, hash, timestamp).
- **Exactly one per network:** it holds a session `pg_try_advisory_lock` on a dedicated connection, re-checked every tick. Commits also lock the cursor row and refuse a range that doesn't continue it.
- **Race with new accounts:** if an account was tracked while a range was being decoded, the commit applies nothing and the range is refetched with that account included.
- **Startup:** `follower.load()` reads the saved cursor before the server listens, so a restart can serve immediately.
- **Health:** responses are `stale` after a failed poll, a lag above one epoch, or 20 min without progress. Discovery returns 503 above 96 blocks of lag or after 30 min without progress.

### Backfill and handoff (`src/discovery/service.ts`)
- **`trackAccount`:** in one transaction, it takes a share lock on the cursor row, inserts `tracked_accounts`, and stores C as `backfill_target`. The store rejects backfill chunks above C. Counts add, first/last block widen, and the newest ERC-721 transfer wins by (block, logIndex), so the two writers commute.
- **Scan:** `Transfer` with the account in topic1 and in topic2, run in parallel per chunk. 3k-block chunks, up to 8 in flight. Completed chunks commit strictly in order, each with its `progress_to`, so a failure resumes at the first incomplete chunk.
- **Aggregation while decoding:** logs go straight into per-token aggregates. Only self-transfers are deduplicated.
- **Range splitting:** split on "too large" or on a timeout of a multi-block range, using the provider's suggested range when given.
- **Adaptive per-account range:** it halves after a split, a response over 50 MB, or a slow response over 5 MB, and doubles after three light chunks. The range is persisted in `account_scan_range`.
- **Requests during backfill:** a request waits up to 10 s, then gets `202 syncing` with progress. Backfills are bounded (4 concurrent, 64 queued); beyond that the API returns 503 with `Retry-After`.

### Balances (`src/balances/worker.ts`)
- **Which accounts:** those that requested balances within 5 min are active.
- **When:** every 60 s and on every follower advance, the worker reads all their pairs at **the follower cursor, pinned by hash** (EIP-1898). Reads are packed into Multicall3 `aggregate3` calls (`allowFailure`, 1,000 per call) and stored in `balance_snapshots` with block number and hash.
- **No inferred freshness:** a snapshot is never relabelled as valid at a newer block. Each token reports `balanceBlock`, and the response reports `balancesAtBlockHeight`. "No Transfer, so unchanged" is never assumed, because of WETH `deposit` and rebasing tokens.
- **Failure handling:** batches split only on size, gas or revert errors. Authentication errors, rate limits and outages propagate as 503.
- **Request path:** only a token with no snapshot yet is read on the request path, single-flight per account, through a bounded limiter (16 concurrent, 256 queued).

### RPC resilience (`src/rpc/`)
`RpcClient` is the only component that performs HTTP. Details are in README "Unreliable RPC".
- **Error classes:** `rate_limited`, `transient`, `too_large` (carrying the suggested range), `unsupported`, `fatal`.
- **Retries:** full-jitter backoff. `Retry-After` is honoured but capped at 10 s.
- **Breakers and failover:** a breaker per endpoint (5 failures, 15 s open). "Method not available" is remembered per endpoint and method for 10 min. Requests fail over across `RPC_FALLBACK_URLS`.
- **Validation:** a `validate` option makes `null` or invalid blocks and logs fail over.
- **Accounting:** a token-bucket limiter (`RPC_MAX_RPS`, shared by follower, backfill and balances), per-attempt stats in `/health`, and API-key redaction.

### Interfaces (`src/core/types.ts`)
- **`ChainAdapter`** (EVM: `createEvmAdapter`):
  - head and blocks: `finalizedHead`, `blockByNumber`
  - logs: `scanChunk(account, range)` for the backfill, `scanAllTransfers(range, onTransfer, accept)` for the follower
  - state reads: `erc20BalancePairs(pairs, blockHash)` and `erc721Owners(items, blockHash)`
  - addresses: `normalizeAddress`

  A new EVM chain is a config entry. A non-EVM chain implements the interface.
- **`Store`:** async. One implementation serves `pg` and PGlite.
- **`RpcClient`:** transport and policy.
- **Not built yet:** splitting log reads out into a `LogSource` (RPC, HyperSync, SQD) behind `scanChunk`/`scanAllTransfers`, as the first step of the HyperSync work.

### Tables (`src/store/schema.ts`, migrations in `drizzle/`)
- `follower_cursor(network, block_number, block_hash, block_timestamp, updated_at)`
- `tracked_accounts(network, account, backfill_target, tracked_at)`
- `account_scan_progress(network, account, progress_to)`
- `account_scan_range(network, account, logs_range)`
- `holdings(network, holder, token, standard, first_block, last_block, sent_count, received_count)`, primary key (network, holder, token)
- `nft_items(network, holder, token, token_id, held, last_block, last_log_index)`
- `balance_snapshots(network, account, token, balance, error, block_number, block_hash)`

No individual transfers are stored.

### API (Hono)
- `GET /v1/ethereum/accounts/{addr}/erc20?balances=true` and `…/erc721?verifyOwnership=true` return:
  - `blockHeight`/`blockHash`: the follower cursor, read in the same repeatable-read transaction as the holdings
  - freshness: `stale`, `lagBlocks`, `followerLagBlocks`, `dataAgeS`
  - per token: first and last seen block, sent and received counts, and optionally `balance` and `balanceBlock`
- During a backfill: `202 syncing` with `Retry-After: 2`.
- `/health` reports the follower, the balance worker, and RPC stats per method.

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

- **Freshness:** responses are about 13 min behind the tip (finalized), plus up to one 30 s poll. A realtime tail would bring back reorg handling. The brief says "almost realtime", so expect this question.
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

1. **A `HyperSyncLogSource` for the backfill:**
   - `stream()` with the OR query and `JoinNothing`, `to_block` capped at RPC finalized
   - a canary against silent empty results
   - Quicknode chunking as the fallback
   - check that the Linux napi build loads before deploying

   Expected: normal accounts in under 1 s, Binance in about 13 s, about 0 Quicknode credits.
2. **Raise `RPC_MAX_RPS` toward the plan's 50.** This should bring a normal cold scan from about 8.6 s to about 3.5 s, while keeping headroom for the follower and balances.
3. **Check credits on the Quicknode dashboard:** one 3k `getLogs`, a 3-call batch, and a 1,000-call Multicall.
4. **A live HTTP load test:** 5k accounts polling once a minute, measuring RPC calls/min and p99 latency.
5. **Decide the open limits:** shared state for multiple replicas (active set and balance-worker leader in Postgres), untracking idle accounts, spam flags, native ETH, and whether a realtime tail is worth the reorg handling.

## Sources

- [Quicknode API credits](https://www.quicknode.com/api-credits), [pricing](https://www.quicknode.com/pricing), [eth_getLogs](https://www.quicknode.com/docs/ethereum/eth_getLogs), [10k block range limit](https://support.quicknode.com/hc/en-us/articles/10258449939473-Understanding-the-10-000-Block-Range-Limit-for-querying-Logs-and-Events)
- [SQD Portal EVM API](https://docs.sqd.dev/en/portal/evm/api), [SQD SDK overview](https://docs.sqd.dev/en/sdk/overview)
- [Envio HyperSync](https://docs.envio.dev/docs/HyperSync/overview), [HyperSync query](https://docs.envio.dev/docs/HyperSync/hypersync-query), [HyperRPC](https://docs.envio.dev/docs/HyperRPC/overview-hyperrpc), [pricing](https://envio.dev/pricing/hypersync)
- [Ponder requirements](https://ponder.sh/docs/requirements)
- [Goldsky Turbo](https://docs.goldsky.com/turbo-pipelines/quickstart)
